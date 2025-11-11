""" Provides configuration for application in the form of a Config
    class that is loaded with the data from the `.env` file """

import os
from dataclasses import dataclass
from dotenv import load_dotenv

load_dotenv(override=True)

@dataclass(frozen=True)
class Config:
    """ Config class that contains the environment variables for the application. """
    SECRET_KEY = os.getenv('SECRET_KEY')
    SQLALCHEMY_DATABASE_URI = (
        f'mysql+pymysql://{os.getenv("DB_USERNAME")}:{os.getenv("DB_PASSWORD")}'
        f'@{os.getenv("DB_HOST")}/{os.getenv("DB_NAME")}'
    )
    SQLALCHEMY_TRACK_MODIFICATIONS = False

    SESSION_COOKIE_SECURE = os.getenv('SESSION_COOKIE_SECURE')
    SESSION_COOKIE_HTTPONLY = os.getenv('SESSION_COOKIE_HTTPONLY')
    SESSION_COOKIE_SAMESITE = os.getenv('SESSION_COOKIE_SAMESITE')

    CELERY = {
        'broker_url': os.getenv('REDIS_URI'),
        'result_backend': os.getenv('REDIS_URI'),
        'task_ignore_result': True,
        'broker_connection_retry_on_startup': True
    }

for key, value in os.environ.items():
    setattr(Config, key, value)
