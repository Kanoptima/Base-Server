""" Module for helper functions related to user authorisation. """

from functools import wraps
from typing import Optional

from flask import redirect, url_for, flash
from flask_login import current_user  # type: ignore

from base_server.extensions import db
from base_server.models.user import User

current_user: User


def admin_required(f):
    """ Decorator to restrict access to admin users. """
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if not current_user.is_authenticated or not current_user.is_admin:
            flash('You do not have access to this page.', 'danger')
            return redirect(url_for('main.index'))
        return f(*args, **kwargs)
    return decorated_function


def staff_login_required(f):
    """ Decorator to restrict access to SBFO/Kanoptima users. """
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if not current_user.is_authenticated:
            return redirect('/user/login')
        if 'kanoptima.com.au' not in current_user.email and 'sbfo.com.au' not in current_user.email:
            flash('You do not have access to this page.', 'danger')
            return redirect(url_for('main.bad_email'))
        return f(*args, **kwargs)
    return decorated_function


def set_admin(email: str, admin=True):
    """ Set a user with `email` to be an admin if `admin==True`, or not if `admin==False`. """
    user: Optional[User] = User.query.filter_by(email=email).first()
    if user:
        user.is_admin = admin
        db.session.commit()
