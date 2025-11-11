""" Module for providing string formatting helper functions. """

import json
from base_server.helpers.messaging import AutomationMessage


def to_camel_case(title: str):
    """ Takes a string of words seperated by spaces and returns a string of that string in camel case.
        For instance `"Hello world"` -> `"helloWorld"` """
    title_words = title.split(" ")
    output = ""
    for i, word in enumerate(title_words):
        if len(word) == 0:
            continue
        if word == "and":
            continue
        if i == 0:
            output += word.lower()
            continue

        output += word.capitalize()

    for bad_char in ["\\", "/", ".", "|", ",", "-", "&"]:
        output = output.replace(bad_char, "")

    return output


def to_snake_case(title: str):
    """ Takes a string of words seperated by spaces and returns a string of that string in snake case.
        For instance `"Hello world"` -> `"hello_world"` """
    title_words = title.split(" ")
    output = ""
    for i, word in enumerate(title_words):
        if len(word) == 0:
            continue
        if word == "and":
            continue
        if i == 0:
            output += word.lower()
            continue
        output += f"_{word.lower()}"

    for bad_char in ["\\", "/", ".", "|", ",", "-", "&"]:
        output = output.replace(bad_char, "")

    return output


def snake_to_camel(snake_title: str):
    """ Takes a title in snake case and returns a string of that title in camel case.
        For instance `"hello_world"` -> `"helloWorld"` """
    components = snake_title.split('_')
    camel_case_str = components[0] + \
        ''.join(word.title() for word in components[1:])
    return camel_case_str


def dicts_to_csv(data: list[dict]):
    """ Takes a list of dicts and returns a list of lists that represents the csv of that dictionary. """
    if not data:
        return []

    headers = list(data[0].keys())
    output = [headers]

    for item in data:
        row = [str(item.get(header, "")) for header in headers]
        output.append(row)

    return output


def automation_results_str(results: list[AutomationMessage]):
    """ Returns a simple string representation of the messages in `results` primariliy for posting as a comment to clickup. """
    output = '\n'.join(result.message for result in results)
    return output


def standardise_phone_number(number):
    """Given a value that is a mobile number of some description, return a +61***... string of the mobile number

    Args:
        number (Any): The mobile number to standardise the format of

    Returns:
        str: The standardised format mobile number
    """
    if not number:
        return None
    standardised = (
        str(number)
        .replace(' ', '')
        .replace('(', '')
        .replace(')', '')
    )
    standardised = f'+61{standardised[-9:]}'
    return standardised


def make_serializable_flat(d):
    """Replaces any non serialisable values in a flat dictionary with 'Invalid'. Must be flat, doesn't check sub dicts or lists.

    Args:
        d (dict): Dictionary to be made serialisable

    Returns:
        dict: Flat dictionary that can be serialised
    """
    return {
        k: v if json_serializable(v) else "Invalid"
        for k, v in d.items()
    }


def json_serializable(value):
    """Checks if a value is json serialisable

    Args:
        value (Any): A value in a dictionary that needs to be serialised

    Returns:
        bool: True if can be serialised, False otherwise.
    """
    try:
        json.dumps(value)
        return True
    except (TypeError, OverflowError):
        return False
