""" Module for loading config data into memory, such as json config files. """

import os
import json

def load_json(file_name):
    """Load a JSON file from the config/json directory."""
    base_dir = os.path.dirname(os.path.abspath(__file__))
    file_path = os.path.join(base_dir, 'json', file_name)
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"Config file {file_name} not found in {file_path}")
    with open(file_path, 'r', encoding='utf-8') as file:
        return json.load(file)
