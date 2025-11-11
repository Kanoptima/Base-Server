""" File related helper functions. """

import csv
from json import dump, load
import logging
import os

import requests

logger = logging.getLogger(__name__)


def save_json(path, obj):
    """ Saves json serialisable `obj` to `path` json file in tests folder. """
    with open(os.path.join('tests', path), 'w', encoding='utf-8') as file:
        dump(obj, file, indent=4)


def read_json(path):
    """ Reads a json file and returns its contents as interpereted by `json` library. """
    with open(os.path.join('tests', path), 'r', encoding='utf-8') as file:
        return load(file)


def log_json(path, obj):
    """ Saves json serialisable `obj` to `path` json file in log folder. """
    with open(os.path.join('logs', path), 'w', encoding='utf-8') as file:
        dump(obj, file, indent=4)


def save_csv(path: str, data: list[list[str]]):
    """
    Saves a list of lists as a CSV file to the specified path.

    Parameters:
        data (list of list of str): The data to save as CSV.
        file_path (str): The file path to save the CSV file.

    Returns:
        None
    """
    with open(path, mode='w', newline='', encoding='utf-8') as file:
        csv_writer = csv.writer(file)
        csv_writer.writerows(data)


def url_download(url, path):
    """ Downloads file from `url` and saves it to `path`. """
    try:
        r = requests.get(url, allow_redirects=True, timeout=20)
        r.raise_for_status()  # Raise an HTTPError for bad responses
        with open(path, 'wb') as file:
            file.write(r.content)
    except requests.exceptions.RequestException as e:
        logger.error('Error downloading file: %s', e)
    except IOError as e:
        logger.error('Error saving file: %s', e)
