""" Provides Google Drive access and functionality """

from datetime import datetime
from enum import Enum
import io
import logging
import os
from typing import Optional

from dotenv import load_dotenv, set_key
from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaIoBaseUpload

from base_server.helpers.messaging import AutomationMessage
from app.config.loaders import load_json

DRIVE_ID = '0AFFLPR67olISUk9PVA'
FIELDS = 'name, parents, mimeType, id'
FIELDS_LIST = 'files(name, id, parents, mimeType)'
RETRIES = 10
SCOPES = [
    'https://www.googleapis.com/auth/drive',
    'https://www.googleapis.com/auth/drive.activity',
    'https://www.googleapis.com/auth/drive.metadata',
    'https://www.googleapis.com/auth/drive.scripts',
    'https://www.googleapis.com/auth/spreadsheets',
    'https://www.googleapis.com/auth/documents'
]

MIMETYPE_COMPATIBILITY = {
    'application/pdf': set([
        'image/jpeg',
        'image/jpg'
    ])
}

logger = logging.getLogger(__name__)
load_dotenv(override=True)


class MimeType(Enum):
    """ Enum for IDE friendly mime type specification when using google drive. """
    PDF = 'application/pdf'
    JPEG = 'image/jpeg'
    JPG = 'image/jpg'
    PNG = 'image/png'
    HTML = 'text/html'
    CSV = 'text/csv'
    JSON = 'application/json'
    PLAIN_TEXT = 'text/plain'
    WORD_DOC = 'application/msword'
    WORD_DOCX = 'application/vnd.openxmlformats-officedocument.wordprocessingml.document'
    GOOGLE_DOCS = 'application/vnd.google-apps.document'
    EXCEL_XLS = 'application/vnd.ms-excel'
    EXCEL_XLSX = 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
    EXCEL_XLSM = 'application/vnd.ms-excel.sheet.macroenabled.12'
    GOOGLE_SHEETS = 'application/vnd.google-apps.spreadsheet'
    GOOGLE_FOLDER = 'application/vnd.google-apps.folder'

    _MIME_TYPE_TO_EXTENSION = load_json('mime_type_extensions.json')
    _EXTENSION_TO_MIME_TYPE = {v: k for k,
                               v in _MIME_TYPE_TO_EXTENSION.items()}

    @staticmethod
    def from_string(mime_type_str: str):
        """Returns a MimeType object for a corresponding mime_type_str.

        Args:
            mime_type_str (str): MimeType as expressed in the Google Drive API (e.g. 'application/json').

        Returns:
            Optional[MimeType]: Enum representation of MimeType.
        """

        for mime_type in MimeType:
            if mime_type.value == mime_type_str:
                return mime_type
        return None

    @staticmethod
    def get_extension(mime_type_str: str):
        """Get the extension str for a mime type str.

        Args:
            mime_type_str (str): MimeType as expressed in the Google Drive API (e.g. 'application/json').

        Returns:
            Optional[str]: File extention of MimeType without '.' if known (e.g. 'xlsx'), None otherwise.
        """
        return MimeType._MIME_TYPE_TO_EXTENSION.value.get(mime_type_str, None)

    @staticmethod
    def from_extension(extension: str):
        """Get the MimeType enum value from a file extension str. Returns `None` for unknown file extension and logs error.

        Args:
            extension (str): File extention of MimeType without '.' (e.g. 'xlsx')

        Returns:
            _type_: Optional[MimeType]: Enum representation of MimeType.
        """
        mime_type_str = MimeType._EXTENSION_TO_MIME_TYPE.value.get(
            extension, None)
        if mime_type_str:
            return MimeType.from_string(mime_type_str)
        logger.error(
            'Failed to make MimeType object due to unknown file extension: %s', extension)
        return None

    @property
    def extension(self):
        """ The file extension str for a MimeType object.

        Returns:
            str: File extention of MimeType without '.' (e.g. 'xlsx').
        """
        return MimeType._MIME_TYPE_TO_EXTENSION.value.get(self.value, '')

    @property
    def download_mime_type(self) -> 'MimeType':
        """
        The default mime type for downloading.

        Returns:
            - The closest equivalent MIME type for Google files (e.g., Word or Excel).
            - The same MIME type for non-Google files.
        """
        google_file_translation = {
            MimeType.GOOGLE_DOCS: MimeType.WORD_DOCX,
            MimeType.GOOGLE_SHEETS: MimeType.EXCEL_XLSX,
        }
        return google_file_translation.get(self, self)

    @property
    def is_google_file(self):
        """ A bool representing whether the file is a Google file or any other kind of file. """
        return self in [MimeType.GOOGLE_DOCS, MimeType.GOOGLE_SHEETS]


class Item:
    """ Class representing either a file or folder in Google Drive. """

    def __init__(self, name: str, item_id: str, mime_type: MimeType, parent: Optional[str]):
        """Initialize a generic Google Drive item.

        Args:
            name (str): The name of the item.
            item_id (str): The ID of the item.
            mime_type (MimeType): The MIME type of the item.
            parent (Optional[str]): The ID of the parent folder (this is None for the top level folder).
        """

        self.name = name
        self.item_id = item_id
        self.mime_type = mime_type
        self.parent = parent

    @classmethod
    def from_json(cls, json_data: dict):
        """Create an Item (File or Folder) from its JSON representation.

        Args:
            json_data (dict): A dictionary representing the item's metadata from Google Drive.

        Returns:
            File|Folder|None: An instance of File or Folder, depending on the MimeType. None if MimeType not known.
        """

        mime_type = MimeType.from_string(json_data.get('mimeType', ''))
        if mime_type == MimeType.GOOGLE_FOLDER:
            return Folder(
                name=json_data['name'],
                item_id=json_data['id'],
                parent=json_data.get('parents', [None])[0]
            )
        if not mime_type:
            logger.error('Could not create file object, MimeType not known: %s', json_data.get(
                'mimeType', ''))
            return None
        return File(
            name=json_data['name'],
            item_id=json_data['id'],
            mime_type=mime_type,
            parent=json_data.get('parents', [None])[0]
        )

    @classmethod
    def get_by_id(cls, item_id: str) -> Optional['Item']:
        """Gets an Item object ny ID. If the item is of the wrong type, return None.

        Args:
            item_id (str): Google Drive item ID.

        Returns:
            File|Folder|None: The Folder/File object with the provided ID, None if anything goes wrong.
        """
        service = start_service()

        results = get_request(service, fileId=item_id,
                              supportsAllDrives=True, fields=FIELDS)
        if not results:
            logger.error('Get item by ID failed due to API error.')
            return None

        item = cls.from_json(results)
        if isinstance(item, cls):
            return item

        logger.warning('get_by_id failed as "%s" is not type %s', item, cls)
        return None

    def __str__(self):
        return f'Item(name={self.name}, id={self.item_id})'

    def rename(self, new_name: str) -> bool:
        """ Renames the item to `new_name`. Returns `bool` representing success of the function. """
        service = start_service()

        update_body = {
            'name': new_name
        }

        results = update_request(
            service, fileId=self.item_id, body=update_body, supportsAllDrives=True, fields='name')
        if not results:
            logger.error('Rename item failed due to API error.')
            return False

        self.name = new_name
        return True

    def move(self, new_parent_id: str) -> bool:
        """ Moves the item to the folder with id `new_parent_id`. Returns `bool` representing success of the function. """
        service = start_service()

        # Move the file to the new folder
        results = update_request(
            service,
            fileId=self.item_id,
            addParents=new_parent_id,
            removeParents=self.parent if self.parent else '',
            supportsAllDrives=True,
            fields='id, parents'
        )
        if not results:
            logger.error('Move item failed due to API error.')
            return False

        self.parent = new_parent_id
        return True

    def delete(self) -> bool:
        """ Deletes the item. Returns `bool` representing success of the function. """
        service = start_service()

        results = delete_request(
            service,
            fileId=self.item_id,
            supportsAllDrives=True
        )
        if results is None:
            logger.error('Delete item failed due to API error.')
            return False

        return True


class Folder(Item):
    """ Class representing a folder in Google Drive. """

    def __init__(self, name: str, item_id: str, parent: str):
        """Initialize a Google Drive folder.

        Args:
            name (str): The name of the folder.
            item_id (str): The ID of the folder.
            parent (Optional[str]): Google Drive ID of parent folder (None for top level folder).
        """

        super().__init__(name, item_id, MimeType.GOOGLE_FOLDER, parent)

    @classmethod
    def get_by_id(cls, item_id) -> Optional['Folder']:
        folder = super().get_by_id(item_id)
        if not isinstance(folder, Folder):
            return None
        return folder

    @classmethod
    def new_folder(cls, parent_folder_id: str, name: str):
        # pylint: disable-msg=no-member
        """Creates a new folder in the parent folder with id `parent_folder_id` and name `name`.

        Args:
            parent_folder_id (str): Google Drive ID of folder to create folder within
            name (str): Name of new folder

        Raises:
            RuntimeError: Should never happen, protection against somehow getting the wrong subclass of Item in output

        Returns:
            Folder: Created Google Drive folder object
        """
        service = start_service()

        file_metadata = {
            'name': name,
            'driveId': DRIVE_ID,
            'parents': [parent_folder_id],
            'mimeType': MimeType.GOOGLE_FOLDER.value
        }

        created_folder = create_request(
            service, body=file_metadata, supportsAllDrives=True, fields=FIELDS)
        if not isinstance(created_folder, dict):
            logger.error('New folder failed due to API error.')
            return None

        new_folder = Folder.from_json(created_folder)
        if not isinstance(new_folder, Folder):
            raise RuntimeError('New folder object is not of Folder class')
        return new_folder

    def __str__(self):
        return f'Folder(name={self.name}, id={self.item_id})'

    def find_subfolder(self, name: str, silent=False):
        """Finds the folder in this folder with the provided name. If there are multiple, it arbitrarily picks the first one in the API response.

        Args:
            name (str): Name of folder to look for
            silent (bool, optional): Whether to log if the folder is not found.
                Useful if looking and expecting not to find as part of normal operations. Defaults to False.

        Returns:
            Optional[Folder]: Folder if found, None otherwise.
        """
        folders = self.list_folders()
        if folders is None:
            logger.error(
                'API error finding subfolder "%s" in "%s".', name, self)
            return None

        found_folders: list[Folder] = []
        for folder in folders:
            if folder.name != name:
                continue
            found_folders.append(folder)

        if len(found_folders) == 0:
            if not silent:
                logger.warning(
                    'find_subfolder failed, folder with name "%s" not found in folder, "%s"', name, self)
            return None

        if len(found_folders) > 1 and not silent:
            logger.warning('find_subfolder found more than 1 folder with name "%s" in '
                           'folder "%s", arbitrarily choosing first matching folder', name, self)

        return found_folders[0]

    def find_file(self, name: str, silent=False):
        """Finds the file in this folder with the provided name. If there are multiple, it arbitrarily picks the first one in the API response.

        Args:
            name (str): _description_
            silent (bool, optional): Whether to log if the file is not found.
                Useful if looking and expecting not to find as part of normal operations. Defaults to False.

        Returns:
            Optional[File]: File if found, None otherwise.
        """

        files = self.list_files()
        if files is None:
            logger.error(
                'API error finding file "%s" in "%s".', name, self)
            return None

        found_files: list[File] = []
        for folder in files:
            if folder.name != name:
                continue
            found_files.append(folder)

        if len(found_files) == 0:
            if not silent:
                logger.warning(
                    'find_subfolder failed, folder with name "%s" not found in folder with id, "%s"', name, self.item_id)
            return None

        if len(found_files) > 1 and not silent:
            logger.warning('find_subfolder found more than 1 folder with name "%s" in '
                           'folder with folder_id "%s", arbitrarily choosing first matching folder', name, self.item_id)

        return found_files[0]

    def list_items(self, silent=False):
        """Returns list of items contained within this Google folder. Logs warning if no items are found and not silent.

        Args:
            silent (bool, optional): Whether to log warning if folder is empty. Useful if looking and expecting an
                empty folder as part of normal operations. Defaults to False.

        Returns:
            Optional[list[Item]]: List of all items within this folder, None if there is an error.
        """
        service = start_service()
        results = list_request(
            service,
            includeItemsFromAllDrives=True,
            driveId=DRIVE_ID,
            supportsAllDrives=True,
            fields=FIELDS_LIST,
            q=f"trashed=false and '{self.item_id}' in parents",
            corpora='drive'
        )
        if not isinstance(results, dict):
            logger.error('List items failed due to API error.')
            return None

        # Get json representations of found items
        item_dicts: list[dict[str, str]] = results.get('files', [])
        if len(item_dicts) == 0 and not silent:
            logger.warning('No files found in: %s', self)

        # Convert json representations into Folder and File objects
        items = [Item.from_json(item_dict) for item_dict in item_dicts]

        return items

    def list_files(self, silent=False):
        """Returns list of files contained within this Google folder.

        Returns:
            Optional[list[File]]: List of all files within this folder, None if there is an error.
        """
        items = self.list_items(silent)
        if items is None:
            return None
        files = [item for item in items if isinstance(item, File)]
        return files

    def list_folders(self):
        """Returns list of folders contained within this Google folder.

        Returns:
            Optional[list[Folder]]: List of all folder within this folder, None if there is an error.
        """
        items = self.list_items()
        if items is None:
            return None
        folders = [item for item in items
                   if isinstance(item, Folder)]
        return folders

    def new_child_folder(self, name):
        """ Creates a new subfolder with name, `name` returns created `Folder`. """
        service = start_service()

        file_metadata = {
            'name': name,
            'driveId': DRIVE_ID,
            'parents': [self.item_id],
            'mimeType': MimeType.GOOGLE_FOLDER.value
        }

        created_folder = create_request(
            service, body=file_metadata, supportsAllDrives=True, fields=FIELDS)
        if not isinstance(created_folder, dict):
            logger.error('New child folder failed due to API error.')
            return None

        new_folder = Folder.from_json(created_folder)
        if not isinstance(new_folder, Folder):
            logger.error('New child folder is not of Folder class.')
            return None
        return new_folder

    def upload_child_file(self, path: str, name: Optional[str] = None, mime_type: Optional[MimeType] = None):
        """ Uploads a file to the folder with id `folder_id` from the file at `path`. Returns the uploaded file. """
        service = start_service()

        if mime_type is None:
            mime_type = MimeType.from_extension(path.split('.')[-1])
        if not isinstance(mime_type, MimeType):
            logger.error(
                'Could not upload child file: Mimetype could not be determined.')
            return None

        file_metadata = {
            'name': name if name else os.path.basename(path),
            'driveId': DRIVE_ID,
            'parents': [self.item_id],
            'mimeType': mime_type.value
        }

        media = create_request(service, body=file_metadata,
                               media_body=path, supportsAllDrives=True, fields=FIELDS)
        if not media:
            logger.error('Could not upload child file due to API error')
            return None

        file = File.from_json(media)
        if not isinstance(file, File):
            logger.error('Uploaded file is not of File class.')
        return file

    def upload_raw_file(self, file_name: str, file_bytes: bytes, mime_type: MimeType, bytes_mime_type: Optional[MimeType] = None):
        """ Uploads a file to the folder. """
        service = start_service()

        file_metadata = {
            'name': file_name,
            'driveId': DRIVE_ID,
            'parents': [self.item_id],
            'mimeType': mime_type.value
        }

        if not bytes_mime_type:
            bytes_mime_type = mime_type
        media = MediaIoBaseUpload(io.BytesIO(
            file_bytes), mimetype=bytes_mime_type.value)

        created_file = create_request(
            service, body=file_metadata, media_body=media, supportsAllDrives=True, fields=FIELDS)
        if not created_file:
            logger.error('Upload raw file failed due to API error.')
            return None

        return File.from_json(created_file)

    def new_spreadsheet(self, name: str):
        """ Creates new blank spreadsheet with name `name` in this folder. """
        return self.upload_raw_file(name, bytes(), MimeType.GOOGLE_SHEETS, MimeType.CSV)

    def navigate_path(self, path: list[str], automation_messages: Optional[list[AutomationMessage]] = None, create_folders=True):
        """ Returns the folder object of the last folder in `path` by repeatedly finding the subfolder of each
            folder in `path`. If `automation_messages` is provided, appends error messages if any occur. """
        current_folder = self
        for folder_name in path:
            next_folder = current_folder.find_subfolder(folder_name)
            if not next_folder:

                # If the folder is not found and create_folders is False, report the error and return None
                if not create_folders:
                    if automation_messages:
                        automation_messages.append(AutomationMessage.error(
                            f'"{folder_name}" not found in "{current_folder.name}"'))
                    return None

                # Otherwise, create the folder, inform the user of the created folder, report any errors and navigate to the created folder
                next_folder = current_folder.new_child_folder(folder_name)
                if not next_folder:
                    if automation_messages:
                        automation_messages.append(AutomationMessage.error(
                            f'Failed to create folder "{folder_name}" in "{current_folder.name}"'))
                    logger.error(
                        'navigate_path failed, failed to create folder "%s" in "%s"', folder_name, current_folder)
                    return None
                if not automation_messages is None:
                    automation_messages.append(AutomationMessage.info(
                        f'Created folder "{folder_name}" in "{current_folder.name}". If you believe this '
                        f'folder already existed, please check and consolidate the folder structure.'
                    ))
            current_folder = next_folder

        return current_folder

    def download_all(self, path: str, provided_mime_type: Optional[MimeType] = None):
        """ Downloads all files found in the folder with id `folder_id` to the folder specified in `path`. If `provided_mime_type` is not
            left blank, then all files will be attempted to downloaded as said mime type. Returns number of files downloaded. """

        file_list = self.list_files()
        if file_list is None:
            logger.error(
                'download_all failed, API error getting file list from folder: %s', self)
            return 0
        files_downloaded = 0

        if len(file_list) == 0:
            logger.warning(
                'download_all failed, no files found in folder: %s', self)

        for file in file_list:
            download_name = file.name
            if provided_mime_type is None:
                target_mime_type = file.mime_type.download_mime_type
            else:
                target_mime_type = provided_mime_type

            # These characters are allowed in GDrive file names, but not Windows file paths, they must be removed
            for char in ['?', '|', '/', '\\', '<', '>', ':', '\'', '*']:
                download_name = download_name.replace(char, '')

            # If the file to be downloaded is a normal file that is not of the specified mime_type, don't download but report
            if not file.mime_type or (target_mime_type is not file.mime_type and not file.mime_type.is_google_file and
                                      # type: ignore
                                      not file.mime_type.value in MIMETYPE_COMPATIBILITY.get(target_mime_type.value, set([]))): # type: ignore
                logger.warning(
                    'download_all found a file with the wrong mime type.'
                    '\n\tName: "%s"'
                    '\n\tID: "%s"'
                    '\n\tFound Mimetype: "%s"'
                    '\n\tDesired Mimetype: "%s"',
                    download_name, file.item_id, file.mime_type.value, target_mime_type.value
                )
                continue

            # If the file to be downloaded doesn't end with the correct extension, add the extension in the downloaded file
            if download_name[-len(target_mime_type.extension)-1:] != '.' + target_mime_type.extension:
                download_name += '.' + target_mime_type.extension

            if file.save_content(os.path.join(path, download_name), target_mime_type):
                files_downloaded += 1

        return files_downloaded


class File(Item):
    """ Class representing a file in Google Drive. """

    def __init__(self, name: str, item_id: str, mime_type: MimeType, parent: str):
        """
        Initialize a Google Drive file.

        :param name: The name of the file.
        :param id: The ID of the file.
        :param mime_type: The MIME type of the file.
        :param size: The size of the file in bytes.
        """
        super().__init__(name, item_id, mime_type, parent)

    @classmethod
    def get_by_id(cls, item_id) -> Optional['File']:
        file = super().get_by_id(item_id)
        if not isinstance(file, File):
            logger.error(
                'get_by_id failed, item with ID "%s" is not a File', item_id)
            return None
        return file

    def save_content(self, path, mime_type: Optional[MimeType] = None):
        """ Downloads this file to `path` with `mime_type:MimeType`. returns `bool` representing success of the function. """
        service = start_service()

        if mime_type is None:
            mime_type = self.mime_type.download_mime_type

        if self.mime_type in [MimeType.GOOGLE_DOCS, MimeType.GOOGLE_SHEETS]:
            results = export_request(
                service, fileId=self.item_id, mimeType=mime_type.value)
        else:
            results = get_media_request(
                service, fileId=self.item_id, supportsAllDrives=True)

        if not isinstance(results, bytes):
            logger.error(
                'save_content failed, API request did not return bytes got "%s" instead', type(results))
            return False

        with open(path, 'wb') as file:
            file.write(results)

        return True

    def copy(self, destination_folder_id: str, new_name: Optional[str] = None):
        """ Copies this file to the folder with id `destination_folder_id`, with name, `new_name`. Uses own name if `new_name` is not provided. """
        service = start_service()

        if new_name is None:
            new_name = self.name

        new_file_json = {
            'kind': 'drive#file',
            'driveId': DRIVE_ID,
            'parents': [destination_folder_id],
            'name': new_name
        }

        results = copy_request(service, fileId=self.item_id,
                               supportsAllDrives=True, fields=FIELDS, body=new_file_json)
        if not results:
            logger.error('Google Drive copy file failed due to API error.')
            return None

        return File.from_json(results)

    def __str__(self):
        return f"File(name={self.name}, id={self.item_id})"


def load_client_config():
    """ Shorthand for Google Drive client config. """
    client_config = {
        'web': {
            'client_id': os.getenv('GOOGLE_DRIVE_CLIENT_ID'),
            'client_secret': os.getenv('GOOGLE_DRIVE_CLIENT_SECRET'),
            'auth_uri': 'https://accounts.google.com/o/oauth2/auth',
            'token_uri': 'https://oauth2.googleapis.com/token',
            'redirect_uris': [os.getenv('GOOGLE_REDIRECT_URI')]
        }
    }
    return client_config


def get_google_suite_credentials():
    """ Get Google Suite credentials for entire Google suite API. """
    creds = None

    # Load credentials from environment variables
    if os.getenv('GOOGLE_REFRESH_TOKEN'):
        creds = Credentials(
            token=os.getenv('GOOGLE_ACCESS_TOKEN'),
            refresh_token=os.getenv('GOOGLE_REFRESH_TOKEN'),
            token_uri='https://oauth2.googleapis.com/token',
            client_id=os.getenv('GOOGLE_CLIENT_ID'),
            client_secret=os.getenv('GOOGLE_CLIENT_SECRET'),
            expiry=datetime.fromisoformat(os.getenv('GOOGLE_DRIVE_EXPIRY', '2000-01-01T00:00:00.000000'))
        )

    # Refresh the credentials if needed
    if creds and creds.expired and creds.refresh_token:
        creds.refresh(Request())
        if not isinstance(creds.expiry, datetime):
            logger.critical(
                'Google Drive service failed to initialize due to invalid credentials expiry: %s', creds.expiry)
            raise ValueError('Invalid credentials expiry')
        set_key('.env', 'GOOGLE_ACCESS_TOKEN', creds.token)
        set_key('.env', 'GOOGLE_REFRESH_TOKEN', creds.refresh_token)
        set_key('.env', 'GOOGLE_EXPIRY', creds.expiry.isoformat())
        load_dotenv(override=True)  # Reload the environment variables

    # If there are no valid credentials, authenticate the user
    if not creds or not creds.valid:
        config = load_client_config()
        flow = InstalledAppFlow.from_client_config(config, SCOPES)
        creds = flow.run_local_server(prompt='consent')

        if not isinstance(creds.expiry, datetime):
            logger.critical(
                'Google API service failed to initialize due to invalid credentials expiry: %s', creds.expiry)
            raise ValueError('Invalid credentials expiry')

        # Save the refresh token and access token in the .env file
        set_key('.env', 'GOOGLE_ACCESS_TOKEN', str(creds.token))
        set_key('.env', 'GOOGLE_REFRESH_TOKEN', str(creds.refresh_token))
        set_key('.env', 'GOOGLE_EXPIRY', creds.expiry.isoformat())
        load_dotenv(override=True)  # Reload the environment variables

    return creds

def start_service():
    """Returns service object that can execute API calls."""
    creds = get_google_suite_credentials()

    try:
        service = build('drive', 'v3', credentials=creds,
                        cache_discovery=False)
        return service
    except HttpError as error:
        logger.critical(
            'Google Drive service failed to initialize due to HTTP Error: %s', error)
        raise RuntimeError from error


def get_request(service, **kwargs):
    """ Makes a get request using `service`. """
    for i in range(RETRIES):
        try:
            data = service.files().get(**kwargs).execute()
            return data
        except (HttpError, TimeoutError) as e:
            if i < RETRIES - 1 and (isinstance(e, TimeoutError) or e.status_code in [500, 503]):
                continue
            logger.error('Google drive get request API error: %s', e)
            break

    return None


def create_request(service, **kwargs):
    """ Makes a create request using `service`. """
    for i in range(RETRIES):
        try:
            data = service.files().create(**kwargs).execute()
            return data
        except (HttpError, TimeoutError) as e:
            if i < RETRIES - 1 and (isinstance(e, TimeoutError) or e.status_code in [500, 503]):
                continue
            logger.error('Google drive create request API error: %s', e)
            break

    return None


def update_request(service, **kwargs):
    """ Makes an update request using `service`. """
    for i in range(RETRIES):
        try:
            data = service.files().update(**kwargs).execute()
            return data
        except (HttpError, TimeoutError) as e:
            if i < RETRIES - 1 and (isinstance(e, TimeoutError) or e.status_code in [500, 503]):
                continue
            logger.error('Google drive create request API error: %s', e)
            break

    return None


def delete_request(service, **kwargs):
    """ Makes an delete request using `service`. """
    for i in range(RETRIES):
        try:
            data = service.files().delete(**kwargs).execute()
            return data
        except (HttpError, TimeoutError) as e:
            if i < RETRIES - 1 and (isinstance(e, TimeoutError) or e.status_code in [500, 503]):
                continue
            logger.error('Google drive create request API error: %s', e)
            break

    return None


def list_request(service, **kwargs):
    """ Makes a list request using `service`. """
    for i in range(RETRIES):
        try:
            data = service.files().list(**kwargs).execute()
            return data
        except (HttpError, TimeoutError) as e:
            if i < RETRIES - 1 and (isinstance(e, TimeoutError) or e.status_code in [500, 503]):
                continue
            logger.error('Google drive list request API error: %s', e)
            break

    return None


def copy_request(service, **kwargs):
    """ Makes a copy request using `service`. """
    for i in range(RETRIES):
        try:
            data = service.files().copy(**kwargs).execute()
            return data
        except (HttpError, TimeoutError) as e:
            if i < RETRIES - 1 and (isinstance(e, TimeoutError) or e.status_code in [500, 503]):
                continue
            logger.error('Google drive copy request API error: %s', e)
            break

    return None


def export_request(service, **kwargs):
    """ Makes a export request using `service`. """
    for i in range(RETRIES):
        try:
            data = service.files().export(**kwargs).execute()
            return data
        except (HttpError, TimeoutError) as e:
            if i < RETRIES - 1 and (isinstance(e, TimeoutError) or e.status_code in [500, 503]):
                continue
            logger.error('Google drive export request API error: %s', e)
            break

    return None


def get_media_request(service, **kwargs):
    """ Makes a get media request using `service`. """
    for i in range(RETRIES):
        try:
            data = service.files().get_media(**kwargs).execute()
            return data
        except (HttpError, TimeoutError) as e:
            if i < RETRIES - 1 and (isinstance(e, TimeoutError) or e.status_code in [500, 503]):
                continue
            logger.error('Google drive get media request API error: %s', e)
            break

    return None
