""" Helper functions for sending emails using the Gmail API. """

from email.mime.application import MIMEApplication
from email.mime.image import MIMEImage
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import os
import logging
import base64

from google.auth.transport.requests import Request
from google.auth.exceptions import RefreshError
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

from base_server.models.gmail_account import GmailAccount

logger = logging.getLogger(__name__)

# Gmail API scopes needed for sending email
SCOPES = ['https://www.googleapis.com/auth/gmail.send',
          'https://www.googleapis.com/auth/gmail.modify']


def load_client_config():
    """Loads Gmail OAuth2 client configuration from environment variables."""
    client_config = {
        'web': {
            'client_id': os.getenv('GMAIL_CLIENT_ID'),
            'client_secret': os.getenv('GMAIL_CLIENT_SECRET'),
            'auth_uri': 'https://accounts.google.com/o/oauth2/auth',
            'token_uri': 'https://oauth2.googleapis.com/token',
            'redirect_uris': [os.getenv('GOOGLE_REDIRECT_URI')],
        }
    }
    return client_config


def authorize_gmail_account(email_address: str):
    """
    Starts the OAuth2 flow to authorize a Gmail account and store credentials in the database.

    This should be run manually (e.g. via an admin route, CLI command, or setup script)
    when credentials do not yet exist or have expired/revoked refresh tokens.

    Args:
        email_address (str): Gmail address to authorize.

    Returns:
        Credentials | None: Authorized Gmail credentials, or None if authorization fails.
    """
    logger.info('Starting Gmail OAuth flow for %s', email_address)

    client_config = load_client_config()

    try:
        flow = InstalledAppFlow.from_client_config(client_config, SCOPES)
        # This will open a browser window or prompt for manual authorization.
        credentials = flow.run_local_server(prompt='consent')
        GmailAccount.store_credentials(email_address, credentials)
        logger.info(
            'Successfully authorized and stored credentials for %s', email_address)
        return credentials
    except ValueError as e:
        logger.error('Error authorizing Gmail account for %s: %s',
                     email_address, e)
        return None


def get_gmail_service(from_email: str):
    """
    Returns an authenticated Gmail API service for the given email.
    Automatically attempts to refresh or recreate credentials if needed.
    """
    credentials = GmailAccount.get_credentials(from_email)

    if not credentials:
        logger.warning(
            'No credentials found for %s. Attempting new authorization.', from_email)
        credentials = authorize_gmail_account(from_email)
        if not credentials:
            logger.error(
                'Failed to authorize new credentials for %s.', from_email)
            return None

    # Attempt to refresh if expired
    if not credentials.valid:
        try:
            credentials.refresh(Request())
            GmailAccount.store_credentials(from_email, credentials)
        except RefreshError as e:
            logger.warning(
                'Credentials for %s invalid or revoked: %s', from_email, e)
            credentials = authorize_gmail_account(from_email)
            if not credentials:
                logger.error(
                    'Failed to reauthorize Gmail credentials for %s.', from_email)
                return None

    try:
        return build('gmail', 'v1', credentials=credentials, cache_discovery=False)
    except HttpError as e:
        logger.error('Error building Gmail service for %s: %s', from_email, e)
        return None


def _attach_images(message: MIMEMultipart, inline_images: dict):
    """Attaches inline images to the email."""
    for cid, image_path in inline_images.items():
        with open(os.path.join('app', 'static', image_path), 'rb') as file:
            image_bytes = file.read()
        image_part = MIMEImage(image_bytes)
        image_part.add_header('Content-ID', f'<{cid}>')
        image_part.add_header('Content-Disposition', 'inline', filename=cid)
        message.attach(image_part)


def _attach_files(message: MIMEMultipart, attachments: list):
    """Attaches files to the email."""
    for filename, file_bytes in attachments:
        part = MIMEApplication(file_bytes, Name=filename)
        part['Content-Disposition'] = f'attachment; filename="{filename}"'
        message.attach(part)


def send_email(
    to_email: str | list[str] | tuple[str],
    subject: str,
    body: str,
    from_email='integrations@sbfo.com.au',
    **options,
):
    # pylint: disable-msg=no-member
    """
    Sends an email using the Gmail API.

    Args:
        to_email (str | list[str] | tuple[str]): Recipient email(s).
        subject (str): Email subject.
        body (str): Email body (plain text or HTML).
        from_email (str, optional): Sender email. Defaults to 'integrations@sbfo.com.au'.
        **options:
            - is_html (bool): If True, treats the body as HTML.
            - attachments (list[tuple[str, bytes]]): List of (filename, file bytes).
            - inline_images (dict[str, str]): Inline images {cid: path}.

    Returns:
        dict | None: Gmail API response or None if an error occurs.
    """
    service = get_gmail_service(from_email)
    if not service:
        logger.error('Could not authenticate email.')
        return None

    message = MIMEMultipart()
    message['Subject'] = subject
    message['To'] = ', '.join(to_email) if isinstance(
        to_email, (list, tuple)) else to_email
    message.attach(MIMEText(body, 'html' if options.get(
        'is_html', False) else 'plain'))

    _attach_images(message, options.get('inline_images', {}))
    _attach_files(message, options.get('attachments', []))

    raw_message = base64.urlsafe_b64encode(message.as_bytes()).decode()

    try:
        return service.users().messages().send(userId='me', body={'raw': raw_message}).execute()
    except HttpError as e:
        logger.error('Error sending email: %s', e)
        return None
