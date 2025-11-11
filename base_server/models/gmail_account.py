""" Provides Gmail account model. """

import json
import logging
import os
from typing import Optional

from cryptography.fernet import Fernet, InvalidToken
from dotenv import load_dotenv
from google.oauth2.credentials import Credentials
from google.auth.external_account_authorized_user import Credentials as OtherCredentials
from sqlalchemy.exc import SQLAlchemyError

from base_server.extensions import db

load_dotenv()

fernet = Fernet(os.getenv('DB_ENCRYPTION_KEY', ''))
logger = logging.getLogger(__name__)

class GmailAccount(db.Model):
    """Model for storing Gmail account credentials."""

    __tablename__ = 'gmail_accounts'

    id: int = db.Column(db.Integer, primary_key=True)
    email_address: str = db.Column(db.String(255), unique=True, nullable=False)
    encrypted_credentials: bytes = db.Column(db.Text, nullable=False)

    def __repr__(self):
        return f'<GmailAccount "{self.email_address}">'

    @classmethod
    def store_credentials(cls, email_address, credentials: Credentials | OtherCredentials):
        """
        Encrypt and store Gmail credentials.

        Args:
            email_address (str): The email address associated with the credentials.
            credentials (Credentials): A Google OAuth2 Credentials object.

        Returns:
            bool: True if successful, False otherwise.
        """
        try:
            encrypted_data = fernet.encrypt(credentials.to_json().encode())

            account: Optional['GmailAccount'] = cls.query.filter_by(email_address=email_address).first()
            if account:
                account.encrypted_credentials = encrypted_data
            else:
                account = cls(email_address=email_address, encrypted_credentials=encrypted_data) # type: ignore
                db.session.add(account)

            db.session.commit()
            logger.info("Successfully stored credentials for %s", email_address)
            return True
        except SQLAlchemyError as e:
            logger.error("Database error storing credentials for %s: %s", email_address, e)
            db.session.rollback()
        return False

    @classmethod
    def get_credentials(cls, email_address):
        """
        Retrieve and decrypt Gmail credentials.

        Args:
            email_address (str): The email address associated with the credentials.

        Returns:
            Optional[Credentials]: A Google OAuth2 Credentials object if successful, None otherwise.
        """
        try:
            account = cls.query.filter_by(email_address=email_address).first()
            if not account:
                logger.warning("No credentials found for %s", email_address)
                return None

            decrypted_data = fernet.decrypt(account.encrypted_credentials).decode()
            return Credentials.from_authorized_user_info(json.loads(decrypted_data))
        except InvalidToken as e:
            logger.error("Error decrypting credentials for %s: %s", email_address, e)
        except ValueError as e:
            logger.error("Invalid credentials stored for %s: %s", email_address, e)
        return None
