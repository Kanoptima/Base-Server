""" Module to provide messaging functionality, mainly for displaying information to users. """

import json
from enum import Enum
from datetime import datetime
from typing import Optional
from base_server.helpers.dates import Dates


class CustomDataEncoder(json.JSONEncoder):
    """ Class to enable json serialisation for the AutomationMessage class. """

    def default(self, o):
        if isinstance(o, AutomationMessage):
            return o.to_dict()
        if isinstance(o, datetime):
            return {'datetime_obj': o.isoformat()}
        if isinstance(o, Dates):
            return {'dates_start_date': o.start_date.isoformat(), 'dates_end_date': o.end_date.isoformat()}
        return super().default(o)


class CustomDataDecoder(json.JSONDecoder):
    """Class to decode JSON strings into custom objects."""

    def __init__(self, *args, **kwargs):
        super().__init__(object_hook=self.object_hook, *args, **kwargs)

    def object_hook(self, obj):  # type: ignore
        # pylint: disable-msg=method-hidden
        """ Decodes JSON strings into custom objects. """

        # Decode AutomationMessage if applicable
        if 'severity' in obj and 'message' in obj:  # Adjust as per your structure
            return AutomationMessage.from_dict(obj)

        # Decode datetime strings
        if 'datetime_obj' in obj:
            return datetime.fromisoformat(obj['datetime_obj'])

        if 'dates_start_date' in obj and 'dates_end_date' in obj:
            return Dates(datetime.fromisoformat(obj['dates_start_date']), datetime.fromisoformat(obj['dates_end_date']))

        return obj


class Severity(Enum):
    """ Simple enum representing the severity of a message. """
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"

    def __eq__(self, other):
        return self.__class__ is other.__class__ and other.value == self.value

    @classmethod
    def from_string(cls, value: str):
        """ Get Severity object from lower case severity `str`. """
        try:
            return cls(value)
        except ValueError as e:
            raise ValueError(f"Invalid severity: {value}") from e


class AutomationMessage:
    """ Class to encapsulate a single message as part of an automation, mainly to be displayed at the end of an automation.
        Contains a severity, message and date  """

    def __init__(self, severity: Severity, message: str, date: Optional[datetime] = None):
        if not isinstance(severity, Severity):
            raise ValueError(
                "severity must be an instance of the Severity enum")

        self.severity = severity
        self.message = message
        self.date = date or datetime.now()

    def __repr__(self):
        return f'<AutomationMessage {self.severity}: "{self.message}" at {self.date}>'

    def to_dict(self):
        """ Enables JSON serialisation, encodes severity as lowercase string and date as isoformat. """
        return {
            'severity': self.severity.value,
            'message': self.message,
            'date': self.date.isoformat()
        }

    @classmethod
    def from_dict(cls, data: dict):
        """ Enables JSON deserialisation, assumes data has severity, message and date fields, with date being an isoformat string. """
        return cls(
            severity=Severity.from_string(data['severity']),
            message=data['message'],
            date=datetime.fromisoformat(data['date'])
        )

    def __str__(self):
        """String representation for use in logs or external systems like ClickUp."""
        return f"[{self.severity.value.upper()}] {self.message} (Occurred: {self.date.isoformat()})"

    @classmethod
    def info(cls, message: str, date: Optional[datetime] = None):
        """ Initialises and returns an info `AutomationMessage` """
        return AutomationMessage(Severity.INFO, message, date)

    @classmethod
    def warning(cls, message: str, date: Optional[datetime] = None):
        """ Initialises and returns a warning `AutomationMessage` """
        return AutomationMessage(Severity.WARNING, message, date)

    @classmethod
    def error(cls, message: str, date: Optional[datetime] = None):
        """ Initialises and returns an error `AutomationMessage` """
        return AutomationMessage(Severity.ERROR, message, date)


def report_error_free(messages: list[AutomationMessage]):
    """ Determines if an automation was successful by checking for any error messages in a `list[message]`, returns `bool`. """
    for message in messages:
        if message.severity == Severity.ERROR:
            return False

    return True


def extract_latest_drive_id(comments: list[dict]):
    """Returns the most recent Google Drive ID from the provided comments section. Identifies the most recent Google Drive
    link and extracts the ID from it for easy use. Returns `None` if no valid Google Drive link is found.

    Args:
        comments (list[dict]): list of comments from a Clickup task, each comment is a dict with keys 'date' and 'comment_text'.

    Returns:
        Optional[str]: Google Drive folder ID if found, otherwise `None`.
    """

    latest_comment = {'date': '0'}
    for comment in comments:
        if 'date' not in comment.keys() or 'comment_text' not in comment.keys():
            continue
        if float(comment['date']) < float(latest_comment['date']):
            continue
        if 'attributes' in comment['comment']:
            if 'link' in comment['comment']['attributes']:
                if 'https://drive.google.com/drive' in comment['comment']['attributes']['link']:
                    comment['comment_text'] = comment['comment']['attributes']['link']
        if 'https://drive.google.com/drive' not in comment['comment_text']:
            continue
        latest_comment = comment

    if 'comment_text' not in latest_comment or not isinstance(latest_comment['comment_text'], str):
        return None

    # Identify the folder ID from the google drive URL
    folder_id = latest_comment['comment_text'][latest_comment['comment_text'].find(
        'folders/')+len('folders/'):]
    last_characters = ['?', '/', '\n', ' ']
    for last_character in last_characters:
        if last_character in folder_id:
            folder_id = folder_id[:folder_id.find(last_character)]

    return folder_id


def extract_latest_spreadsheet_id(comments: list[dict]):
    """Returns the most recent Google Sheets ID from the provided comments section. Identifies the most recent Google Sheets
    link and extracts the ID from it for easy use. Returns `None` if no valid Google Sheets link is found.

    Args:
        comments (list[dict]): list of comments from a Clickup task, each comment is a dict with keys 'date' and 'comment_text'.

    Returns:
        Optional[str]: Google Sheets spreadsheet ID if found, otherwise `None`.
    """

    latest_comment = {'date': '0'}
    for comment in comments:
        if 'date' not in comment.keys() or 'comment_text' not in comment.keys():
            continue
        if float(comment['date']) < float(latest_comment['date']):
            continue
        if 'attributes' in comment['comment']:
            if 'link' in comment['comment']['attributes']:
                if 'https://docs.google.com/spreadsheets/d/' in comment['comment']['attributes']['link']:
                    comment['comment_text'] = comment['comment']['attributes']['link']
        if 'https://docs.google.com/spreadsheets/d/' not in comment['comment_text']:
            continue
        latest_comment = comment

    if 'comment_text' not in latest_comment or not isinstance(latest_comment['comment_text'], str):
        return None

    # Identify the folder ID from the google drive URL
    spreadsheet_id = latest_comment['comment_text'][latest_comment['comment_text'].find(
        'spreadsheets/d/')+len('spreadsheets/d/'):]
    last_characters = ['?', '/', '\n', ' ']
    for last_character in last_characters:
        if last_character in spreadsheet_id:
            spreadsheet_id = spreadsheet_id[:spreadsheet_id.find(
                last_character)]

    return spreadsheet_id
