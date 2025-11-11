"""Base server with functionality for small accounting deployments, involving API functionalities, Google Drive,
Gmail, automatic scheduling, worker deployment, Google user protection, and common html functionality. """

from flask import Blueprint, Flask, render_template
from .extensions import login_manager, celery_init_app, configure_logging

from .blueprints.main import bp as main_bp
from .blueprints.user import bp as user_bp

base_bp = Blueprint(
    "base",
    __name__,
    template_folder="templates",
    static_folder="static"
)

def not_found_error(error):
    """ Handles 404 errors. """
    # pylint: disable-msg=unused-argument
    return render_template('error/404.html'), 404

def internal_error(error):
    """ Handles 500 errors. """
    # pylint: disable-msg=unused-argument
    return render_template('error/500.html'), 500

def create_app(config_class, schedule: dict[str, dict]):
    """ Creates Flask app using Config class. """
    app = Flask(__name__)
    app.config.from_object(config_class)

    configure_logging()

    celery_init_app(app, schedule)
    login_manager.init_app(app)
    login_manager.login_view = 'user.login' # type: ignore

    app.register_error_handler(404, not_found_error)
    app.register_error_handler(500, internal_error)

    # Register favicon
    app.add_url_rule('/favicon.ico', 'favicon', lambda: app.send_static_file('favicon.ico'))

    # Register main blueprint
    app.register_blueprint(main_bp)
    app.register_blueprint(user_bp)


    return app
