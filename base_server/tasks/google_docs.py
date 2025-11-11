""" Module to provide access to Google Docs. """

import logging
from builtins import TimeoutError
import os

from dotenv import load_dotenv
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

from base_server.tasks.google_drive import get_google_suite_credentials

load_dotenv(override=True)

logger = logging.getLogger(__name__)

RETRIES = 5


def load_client_config():
    """ Shorthand for Google Sheets client config. """
    client_config = {
        'web': {
            'client_id': os.getenv('GOOGLE_SHEETS_CLIENT_ID'),
            'client_secret': os.getenv('GOOGLE_SHEETS_CLIENT_SECRET'),
            'auth_uri': 'https://accounts.google.com/o/oauth2/auth',
            'token_uri': 'https://oauth2.googleapis.com/token',
            'redirect_uris': [os.getenv('GOOGLE_REDIRECT_URI')]
        }
    }
    return client_config


def start_service():
    """Start Google Docs API service."""
    creds = get_google_suite_credentials()

    return build('docs', 'v1', credentials=creds)


def get(service, **kwargs):
    """ Makes a get request using `service`. """
    for i in range(RETRIES):
        try:
            return service.get(**kwargs).execute()
        except (HttpError, TimeoutError) as e:
            if i < RETRIES - 1 and (
                isinstance(e, TimeoutError) or getattr(
                    e, "status_code", None) in [500, 503]
            ):
                continue
            logger.error('Google Docs get request API error: %s', e)
            break
    return None


def batch_update(service, **kwargs):
    """ Makes a batchUpdate request using `service`. """
    for i in range(RETRIES):
        try:
            return service.batchUpdate(**kwargs).execute()
        except (HttpError, TimeoutError) as e:
            if i < RETRIES - 1 and (
                isinstance(e, TimeoutError) or getattr(
                    e, "status_code", None) in [500, 503]
            ):
                continue
            logger.error('Google Docs batchUpdate API error: %s', e)
            break
    return None


class Document:
    """A class for interacting with a Google Docs file."""

    def __init__(self, preload):
        self.document_id = preload['documentId']
        self.json = preload
        self.requests = []

    def __repr__(self) -> str:
        return f'<Google Document {self.document_id}>'

    @classmethod
    def get_by_id(cls, document_id: str):
        """Return Document object with data for Google Doc with id `document_id`."""
        # pylint: disable-msg=no-member
        service = start_service().documents()
        data = get(service, documentId=document_id)
        if not data:
            logger.error(
                'Could not get document "%s" due to API error', document_id
            )
            return None
        return Document(data)

    def commit_changes(self):
        """Commit queued changes to the document."""
        # pylint: disable-msg=no-member
        if not self.requests:
            return True

        service = start_service().documents()
        preload = batch_update(
            service, documentId=self.document_id, body={
                'requests': self.requests}
        )
        if not preload:
            logger.error(
                'Could not commit changes to document "%s" due to API error.', self.document_id
            )
            return False

        # Reset requests after commit
        self.requests = []
        return True

    def replace_all_text(self, find: str, replace: str):
        """Replace all found text with replacement text.

        Args:
            find (str): Text to find.
            replace (str): Text to replace found text with.
        """
        request = {
            'replaceAllText': {
                'replaceText': replace,
                'containsText': {
                    'text': find,
                    'matchCase': True,
                    'searchByRegex': False
                }
            }
        }
        self.requests.append(request)
