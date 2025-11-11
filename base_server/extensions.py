""" Provides extensions for flask application including celery, sqlalchemy and logging """

import json
from logging.config import dictConfig
import pkgutil

from celery import Celery, Task
from celery.schedules import crontab
from dotenv import load_dotenv
from flask import Flask
from flask_login import LoginManager
from flask_migrate import Migrate
from flask_sqlalchemy import SQLAlchemy
from kombu.serialization import register

from .helpers.messaging import CustomDataEncoder, CustomDataDecoder

load_dotenv()

db = SQLAlchemy()
migrate = Migrate(db=db)
login_manager = LoginManager()

@login_manager.user_loader
def load_user(user_id):
    """ Get the User object for the user with id `user_id`. """

    # Import here to avoid circular import
    from .models.user import User
    return User.query.get(int(user_id))


def configure_logging():
    """ Configures logging for the application. """

    logging_config = {
        "version": 1,
        "disable_existing_loggers": False,
        "formatters": {
            "default": {
                "format": "[%(asctime)s] [%(levelname)s] %(module)s: %(message)s",
                "datefmt": "%Y-%m-%d %H:%M:%S",
            },
            "verbose": {
                "format": "[%(asctime)s] [%(levelname)s] [%(name)s:%(lineno)d] %(message)s",
                "datefmt": "%Y-%m-%d %H:%M:%S",
            },
        },
        "handlers": {
            "console": {
                "class": "logging.StreamHandler",
                "formatter": "default",
                "level": "INFO",
            }
        },
        "root": {
            "handlers": ["console"],
            "level": "INFO",
        },
    }
    dictConfig(logging_config)

class MyCelery(Celery):
    """ Simple child of Celery class used for custom task naming scheme. """

    def gen_task_name(self, name, module:str):
        i = module.find("tasks.")
        if i != -1:
            module = module[i+6:]
        return super().gen_task_name(name, module)

def celery_init_app(app: Flask, schedule: dict[str, dict]) -> Celery:
    """ Creates `Celery` app and binds it to provided `Flask` app """

    class FlaskTask(Task):
        # pylint: disable-msg=abstract-method
        """ Task class for the Flask context """
        def __call__(self, *args: object, **kwargs: object) -> object:
            with app.app_context():
                return self.run(*args, **kwargs)

    register(
        'custom-json',
        lambda obj: json.dumps(obj, cls=CustomDataEncoder),
        lambda data: json.loads(data, cls=CustomDataDecoder),
        content_type='application/json',
        content_encoding='utf-8'
    )

    celery_app = MyCelery(app.name, task_cls=FlaskTask)
    celery_app.config_from_object(app.config["CELERY"])

    package = __import__('app.tasks', fromlist=[""])
    task_modules = [f'app.tasks.{name}' for _, name, _ in pkgutil.iter_modules(package.__path__)]
    celery_app.autodiscover_tasks(task_modules, force=True)
    celery_app.conf.update(
        task_serializer='custom-json',
        accept_content=['custom-json'],
        result_serializer='custom-json',
        timezone='Australia/Sydney',
        enable_utc=True,
    )
    for entry in schedule.values():
        entry['schedule'] = crontab.from_string(entry['schedule'])
    celery_app.conf.beat_schedule = schedule
    celery_app.set_default()
    app.extensions['celery'] = celery_app
    return celery_app
