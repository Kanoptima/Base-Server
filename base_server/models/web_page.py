""" Provides WebPage model, storing the pages that are the beginning point for automations. """

from typing import Optional

from base_server.extensions import db

class WebPage(db.Model):
    """Model for a single web page in the database
    """
    __tablename__ = 'web_pages'

    id = db.Column(db.Integer, primary_key=True)
    url = db.Column(db.String(255), unique=True, nullable=False)
    name = db.Column(db.String(150), nullable=False)
    description = db.Column(db.String(255), nullable=True)
    group = db.Column(db.String(100), nullable=False, index=True)

    visits = db.relationship('UserPageVisit', back_populates='page', cascade='all, delete-orphan')

    def __repr__(self):
        return f'<WebPage {self.name} ({self.url})>'

    @classmethod
    def get_by_group(cls, group_name: str) -> list['WebPage']:
        """Return all pages under the given group."""
        return cls.query.filter_by(group=group_name).all()

    @classmethod
    def get_by_url(cls, url: str) -> Optional['WebPage']:
        """Return the page with the given url."""
        web_page = cls.query.filter_by(url=url).first()
        if not isinstance(web_page, cls):
            return None
        return web_page


    @classmethod
    def delete_by_url(cls, url: str) -> None:
        """Delete the page with the given url."""
        web_page = cls.query.filter_by(url=url).first()
        if isinstance(web_page, cls):
            db.session.delete(web_page)
            db.session.commit()


    @classmethod
    def upsert_pages(cls, pages: list[dict]) -> None:
        """Add or update multiple web pages at once, keyed by URL.

        Args:
            pages (list[dict]): List of page info dicts with keys:
                - url (str): URL of the page (required, unique)
                - name (str): Display name of the page
                - description (str): Optional description
                - group (str): Group/category of the page
        """
        existing_pages = {p.url: p for p in cls.query.all()}

        for page_data in pages:
            url = page_data.get('url')
            if not url:
                continue  # skip invalid entries

            try:
                if url in existing_pages:
                    # update existing entry
                    page = existing_pages[url]
                    page.name = page_data['name']
                    page.description = page_data['description']
                    page.group = page_data['group']
                else:
                    # insert new page
                    new_page = cls(
                        url=url, # type: ignore
                        name = page_data['name'], # type: ignore
                        description = page_data['description'], # type: ignore
                        group = page_data['group'] # type: ignore
                    )
                    db.session.add(new_page)
            except KeyError:
                continue

        db.session.commit()
