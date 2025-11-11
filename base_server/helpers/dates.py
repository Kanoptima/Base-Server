""" Module providing date related helper functions and `Dates` class for automation date ranges. """

from calendar import monthrange
from datetime import datetime, timedelta

from tzlocal import get_localzone


class Dates:
    """ Class for describing a date range for use in automations with date ranges. """

    def __init__(self, start_date: datetime, end_date: datetime):
        self.start_date = start_date
        self.end_date = end_date

    def __repr__(self) -> str:
        return f'[{self.start_str()}, {self.end_str()}]'

    @classmethod
    def from_strings(cls, start: str, end: str):
        """ Return a `Dates` object using a start and end string formatted as `'%Y-%m-%d'` """
        return cls(datetime.strptime(start, '%Y-%m-%d'), datetime.strptime(end, '%Y-%m-%d'))

    def is_financial_year(self):
        """ Determines if this date range is a full financial year. """
        return (
            self.start_date.day == 1 and
            self.start_date.month == 7 and
            self.end_date.day == 30 and
            self.end_date.month == 6 and
            self.end_date.year - self.start_date.year == 1
        )

    def is_quarter(self):
        """ Determines if this date range is a full quarter. """
        return (
            self.start_date.day == 1 and
            self.start_date.month in [1, 4, 7, 10] and
            self.end_date.day == monthrange(self.end_date.year, self.end_date.month)[1] and
            self.end_date.month - self.start_date.month == 2 and
            self.end_date.year == self.start_date.year
        )

    def start_str(self):
        """ Return YYYY-MM-DD formatting of `self.start_date` """
        return self.start_date.strftime('%Y-%m-%d')

    def end_str(self):
        """ Return YYYY-MM-DD formatting of `self.end_date` """
        return self.end_date.strftime('%Y-%m-%d')

    def xero_where_str(self):
        """ Return a string for use in a Xero API where query """
        return (f'Date>=DateTime({self.start_date.year}, {self.start_date.month:02d}, {self.start_date.day:02d}) AND '
                f'Date<DateTime({self.end_date.year}, {self.end_date.month:02d}, {self.end_date.day:02d})')


def date_to_recency_string(last_updated: datetime):
    """ Converts a datetime object to a string that is expressed as either today,
        yesterday or a day of the week at the correct time if it was recent. """
    now = datetime.now()
    # Long time ago
    if last_updated == datetime.min:
        return "Never"

    # Today
    if last_updated.date() == now.date():
        return last_updated.strftime("Today at %I:%M %p")

    # Yesterday
    if last_updated.date() >= (now - timedelta(days=1)).date():
        return last_updated.strftime("Yesterday at %I:%M %p")

    # This week
    if last_updated >= now - timedelta(days=7):
        return last_updated.strftime("%A at %I:%M %p")

    # Older than that
    return last_updated.strftime("%d %B, %Y at %I:%M %p")


def current_iso_timestamp():
    """ Returns isoformat timestamp of right now without microseconds. """
    # Might want to replace the get_localzone code with one that gets it from the base_server
    return datetime.now(get_localzone()).isoformat(timespec='milliseconds')


def iso_to_readable(iso_date: str):
    """ Returns readable date str based on `iso_date` ('%d %b %Y'), returns None for invalid values of `iso_date`. """
    try:
        return datetime.fromisoformat(iso_date).strftime('%d %b %Y')
    except ValueError:
        return None


def get_financial_year(date: datetime) -> int:
    """ Returns the financial year for a given date. """
    return date.year if date.month < 7 else date.year + 1


def get_sunday(date: datetime):
    """ Returns the next sunday from the provided date, unless it is sunday, in which case it returns itself. """
    days_until_sunday = 6 - date.weekday()  # Monday=0, Sunday=6
    return date + timedelta(days=days_until_sunday)
