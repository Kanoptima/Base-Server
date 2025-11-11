""" Provides UserPageVisit model, connecting users to the pages that they use most. """

from base_server.extensions import db

class UserPageVisit(db.Model):
    """Tracks each time a user visits a page."""
    __tablename__ = "user_page_visits"

    id = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(db.Integer, db.ForeignKey("users.id"), nullable=False, index=True)
    page_id = db.Column(db.Integer, db.ForeignKey("web_pages.id"), nullable=False, index=True)
    timestamp = db.Column(db.DateTime, server_default=db.func.now(), index=True)

    user = db.relationship("User", back_populates="page_visits")
    page = db.relationship("WebPage", back_populates="visits")
