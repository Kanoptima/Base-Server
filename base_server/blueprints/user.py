""" Provides endpoints for everything to do with user state, including logging in and out. """

from flask import Blueprint, current_app, redirect, url_for, session, request
from flask_login import login_user, logout_user, login_required # type: ignore
from google_auth_oauthlib.flow import Flow
import requests
from base_server.models.user import User
from base_server.extensions import db

bp = Blueprint('user', __name__, url_prefix='/user')

def load_client_config():
    """ Since this is used more than once, here is a shorthand for Google Oauth client config. """
    client_config = {
        'web': {
            'client_id': current_app.config['GOOGLE_OAUTH_CLIENT_ID'],
            'client_secret': current_app.config['GOOGLE_OAUTH_CLIENT_SECRET'],
            'auth_uri': 'https://accounts.google.com/o/oauth2/auth',
            'token_uri': 'https://oauth2.googleapis.com/token',
            'redirect_uris': [current_app.config['GOOGLE_OAUTH_REDIRECT_URI']]
        }
    }
    return client_config

@bp.route('/login')
def login():
    """ Begins login flow, connects to Google's Oauth API. """

    client_config = load_client_config()
    flow = Flow.from_client_config(
        client_config,
        scopes=['https://www.googleapis.com/auth/userinfo.email', 'https://www.googleapis.com/auth/userinfo.profile', 'openid'],
        redirect_uri=current_app.config['GOOGLE_OAUTH_REDIRECT_URI']
    )
    flow.redirect_uri = current_app.config['GOOGLE_OAUTH_REDIRECT_URI']
    authorization_url, state = flow.authorization_url()
    session['state'] = state
    return redirect(authorization_url)

@bp.route('/callback')
def callback():
    """ Callback endpoint, for after authentication with Google, to receive authorisation. """

    client_config = load_client_config()
    flow = Flow.from_client_config(
        client_config,
        scopes=['https://www.googleapis.com/auth/userinfo.email', 'https://www.googleapis.com/auth/userinfo.profile', 'openid'],
        state=session['state']
    )
    flow.redirect_uri = current_app.config['GOOGLE_OAUTH_REDIRECT_URI']
    flow.fetch_token(authorization_response=request.url)

    credentials = flow.credentials
    user_info = requests.get(
        'https://www.googleapis.com/oauth2/v1/userinfo',
        params={'access_token': credentials.token},
        timeout=20
    ).json()

    user = User.query.filter_by(email=user_info['email']).first()
    if not user:
        user = User(email=user_info['email'], name=user_info['name']) # type: ignore
        db.session.add(user)
        db.session.commit()

    login_user(user)
    return redirect(url_for('main.index'))  # Update based on your main route

@bp.route('/logout')
@login_required
def logout():
    """ To log out a user, not that they ever would. """
    logout_user()
    return redirect(url_for('user.login'))
