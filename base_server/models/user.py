""" Provides User model """

from datetime import datetime, timedelta
import logging
from typing import Optional

from flask_login import UserMixin
from sqlalchemy.exc import SQLAlchemyError

from base_server.extensions import db
from base_server.models.user_page_visit import UserPageVisit
from base_server.models.web_page import WebPage

logger = logging.getLogger(__name__)
MAX_PAGE_VISITS = 100

class User(UserMixin, db.Model):
    """ Model of a User. """
    __tablename__ = 'users'

    id: int = db.Column(db.Integer, primary_key=True)
    email: str = db.Column(db.String(150), unique=True, nullable=False)
    name: str = db.Column(db.String(150), nullable=False)
    page_visits = db.relationship(
        "UserPageVisit",
        back_populates="user",
        cascade="all, delete-orphan",
        order_by="UserPageVisit.timestamp",
        lazy="dynamic"
    )
    is_admin: bool = db.Column(db.Boolean, nullable=False, default=False)
    annature_id: str = db.Column(db.String(64), nullable=True)

    def __repr__(self):
        return f'<User "{self.name}">'

    def log_page_visit(self, page: Optional[WebPage]):
        """Record that this user visited a page, enforcing visit history limit."""
        if not page:
            return
        visit = UserPageVisit(user=self, page=page) # type: ignore
        db.session.add(visit)
        db.session.flush()  # ensures this visit gets an ID before pruning

        # enforce max visit history
        total_visits = self.page_visits.count()
        if total_visits > MAX_PAGE_VISITS:
            # delete oldest visits
            excess = total_visits - MAX_PAGE_VISITS

            # delete oldest 'excess' visits
            oldest_visits = (
                self.page_visits.order_by(UserPageVisit.timestamp.asc())
                .limit(excess)
                .all()
            )
            for v in oldest_visits:
                db.session.delete(v)

        db.session.commit()

    def most_used_pages(self, limit: int = 5, days: Optional[int] = None) -> list[WebPage]:
        """Return the most used pages by this user."""
        query = db.session.query(
            WebPage, db.func.count(UserPageVisit.id).label("visit_count")
        ).join(UserPageVisit).filter(UserPageVisit.user_id == self.id)

        if days:
            cutoff = datetime.now() - timedelta(days=days)
            query = query.filter(UserPageVisit.timestamp >= cutoff)

        query = query.group_by(WebPage.id).order_by(db.desc("visit_count")).limit(limit)
        return [row[0] for row in query.all()]

    @classmethod
    def get_by_email(cls, email: str):
        """ Gets user object with given `email`. """
        try:
            user: Optional['User'] = cls.query.filter_by(email=email).first()
        except SQLAlchemyError as e:
            logger.error(
                'Get User "%s" by email from database failed: %s', email, e)
            db.session.rollback()
            user = None
        return user

    @classmethod
    def set_admin_status(cls, email: str, is_admin: bool):
        """ Elevates or delevates a user to or from admin using their email address. Returns True on success. """
        try:
            user: Optional[User] = cls.query.filter_by(email=email).first()
            if not user:
                logger.warning('User with email "%s" not found.', email)
                return False
            user.is_admin = is_admin
            db.session.commit()
            return True
        except SQLAlchemyError as e:
            logger.error('Error setting admin status for "%s": "%s"', email, e)
            db.session.rollback()
            return False

    @classmethod
    def update_annature_ids(cls, annature_users: list[dict[str, str]]):
        """ Updates users in the database with their corresponding Annature IDs from the API response. """
        try:
            # Create a mapping of email to Annature ID for quick lookup
            email_to_annature_id = {
                user['email'].lower(): user['id'] for user in annature_users}

            # Fetch all users from the local database
            local_users: list[User] = cls.query.all()
            updated_count = 0

            for user in local_users:
                # Match Annature user by email (case-insensitive)
                annature_id = email_to_annature_id.get(user.email.lower())

                if annature_id and user.annature_id != annature_id:
                    logger.info('Updating Annature ID for user "%s" from "%s" to "%s".',
                                user.email, user.annature_id, annature_id)
                    user.annature_id = annature_id
                    updated_count += 1

            if updated_count > 0:
                db.session.commit()
                logger.info(
                    "Successfully updated %d users with Annature IDs.", updated_count)
            else:
                logger.info("No Annature IDs needed updating.")

        except SQLAlchemyError as e:
            logger.error("Database error while updating Annature IDs: %s", e)
            db.session.rollback()
