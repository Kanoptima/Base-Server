""" Provides Client model """

from datetime import datetime
import logging
from typing import Optional

from cryptography.fernet import Fernet
from flask import current_app
from sqlalchemy.exc import SQLAlchemyError

from base_server.extensions import db


logger = logging.getLogger(__name__)


class XeroClient(db.Model):
    """ Model of Xero Client """
    __tablename__ = 'xero_clients'

    id: int = db.Column(db.Integer, primary_key=True)
    url: Optional[str] = db.Column(db.String(15), nullable=True)
    name: Optional[str] = db.Column(db.String(150), nullable=True)
    tenant_id: Optional[str] = db.Column(
        db.String(255), unique=True, nullable=True)
    access_token: Optional[bytes] = db.Column(db.Text, nullable=True)
    refresh_token: Optional[bytes] = db.Column(db.Text, nullable=True)
    access_token_expiry: Optional[datetime] = db.Column(
        db.DateTime, nullable=True)

    def __repr__(self):
        return f'<Xero Client ({self.id})>'

    @classmethod
    def get_by_id(cls, client_id: int):
        """Safely gets a Xero Client by ID from the database.

        Args:
            client_id (str): ID of client to get.

        Returns:
            Optional[Client]: Client object if found, None otherwise.
        """
        try:
            client: Optional['XeroClient'] = cls.query.get(client_id)
        except SQLAlchemyError as e:
            logger.error(
                'Get Client "%s" by client ID from database failed: %s', client_id, e)
            db.session.rollback()
            client = None
        return client

    @classmethod
    def create_client(cls, new_id: Optional[int]):
        """Creates and saves a new XeroClient in the database.

        Args:
            new_id (Optional[int]): Iden

        Returns:
            Optional[XeroClient]: The newly created client if successful, None otherwise.
        """
        client = cls(id=new_id) # type: ignore
        try:
            db.session.add(client)
            db.session.commit()
            return client
        except SQLAlchemyError as e:
            logger.error('Creating new XeroClient failed: %s', e)
            db.session.rollback()
            return None

    def delete_client(self):
        """Deletes this XeroClient instance from the database.

        Returns:
            bool: True if deletion succeeded, False otherwise.
        """
        try:
            db.session.delete(self)
            db.session.commit()
            return True
        except SQLAlchemyError as e:
            logger.error('Deleting XeroClient %s failed: %s', self.id, e)
            db.session.rollback()
            return False

    @classmethod
    def list_clients(cls):
        """Lists all XeroClients currently stored in the database.

        Returns:
            list[XeroClient]: List of all client objects.
        """
        try:
            return cls.query.all()
        except SQLAlchemyError as e:
            logger.error('Listing XeroClients failed: %s', e)
            db.session.rollback()
            return []

    def set_tokens(self, access_token: str, refresh_token: str, expiry: datetime, tenant_id: Optional[str] = None):
        """Sets Xero access and refresh tokens for this client, as well as the expiry for the access token.
        Optionally, tenant ID can be included in the saving, for initial authorisation.

        Args:
            access_token (str): Xero API access token
            refresh_token (str): Xero API refresh token
            expiry (datetime): Xero API access token expiry datetime
            tenant_id (Optional[str], optional): Tenant ID if known. Defaults to None.
        """
        fernet = Fernet(current_app.config['DB_ENCRYPTION_KEY'])
        self.access_token = fernet.encrypt(bytes(access_token, 'utf-8'))
        self.refresh_token = fernet.encrypt(bytes(refresh_token, 'utf-8'))
        self.access_token_expiry = expiry
        if tenant_id:
            self.tenant_id = tenant_id

        try:
            db.session.commit()
        except SQLAlchemyError as e:
            logger.error(
                'Updating access token for XeroClient %s failed: %s', self.id, e)
            db.session.rollback()

    def get_access_token(self):
        """Gets Xero API access token, this method must be used to decrypt the access token as stored in the database.

        Returns:
            str: Xero API access token
        """
        if not self.access_token:
            return None
        fernet = Fernet(current_app.config['DB_ENCRYPTION_KEY'])
        return fernet.decrypt(self.access_token).decode('utf-8')

    def get_xero_refresh_token(self):
        """Gets Xero API refresh token, this method must be used to decrypt the refresh token as stored in the database.

        Returns:
            str: Xero API refresh token
        """
        if not self.refresh_token:
            return None
        fernet = Fernet(current_app.config['DB_ENCRYPTION_KEY'])
        return fernet.decrypt(self.refresh_token).decode('utf-8')
