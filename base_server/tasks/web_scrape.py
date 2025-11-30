""" Module providing access to web scraper API  """

import base64
import logging
import os
import time

from dotenv import load_dotenv

from base_server.helpers.api_client import ApiClient
from base_server.helpers.dates import Dates

api_client = ApiClient(os.getenv('WEB_SCRAPE_ADDRESS'))
logger = logging.getLogger(__name__)

load_dotenv()


def execute_uncoded_statement_lines(client_id: str, dates: Dates):
    """ Starts the process of navigating Xero to get uncoded statement lines. Returns `task_id` to retrieve results with. """
    params = {
        'client_id': client_id,
        'start_date': dates.start_date.isoformat(),
        'end_date': dates.end_date.isoformat()
    }
    results = api_client.post('uncoded_statement_lines', params=params)
    if not results:
        logger.error(
            'API error while executing uncoded statement lines scrape.')
        return None
    return results.get('task_id')


def get_uncoded_statement_lines(task_id: str, timeout=480):
    """ Gets results of uncoded statement lines task with id `task_id`. Logs and returns `None` on error. """
    params = {
        'task_id': task_id
    }
    start_time = time.time()
    while time.time() - start_time < timeout:
        results = api_client.get('results', params=params)
        if isinstance(results, list):
            return results
        if not isinstance(results, dict):
            logger.error(
                'Get uncoded statement lines failed due to API error.')
            return None
        if results.get('status') == 'pending':
            time.sleep(1)
            continue
        break

    logger.error(
        'Get uncoded statement lines timed out after %d seconds', timeout)
    return None


def execute_management_report(xero_url: str, dates: Dates):
    """ Starts the process of navigating Xero to get management report. Returns `task_id` to retrieve results with. """
    params = {
        'client_id': xero_url,
        'start_date': dates.start_date.isoformat(),
        'end_date': dates.end_date.isoformat()
    }
    results = api_client.post('management_report', params=params)
    if not results:
        logger.error('API error while executing management report scrape.')
        return None
    return results.get('task_id')


def get_management_report(task_id: str, timeout=480):
    """ Gets results of management report task with id `task_id`. Returns raw bytes of excel file. Logs and returns `None` on error. """
    params = {
        'task_id': task_id
    }
    start_time = time.time()
    while time.time() - start_time < timeout:
        results = api_client.get('results', params=params)
        if not isinstance(results, dict):
            logger.error('Get management report failed due to API error.')
            return None
        if results.get('status') == 'pending':
            time.sleep(1)
            continue
        if 'content' in results:
            return base64.b64decode(results['content'])
        break

    logger.error('Get management report timed out after %d seconds', timeout)
    return None


def execute_general_ledger_detail(xero_url: str, dates: Dates):
    """ Starts the process of navigating Xero to get general ledger detail. Returns `task_id` to retrieve results with. """
    params = {
        'client_id': xero_url,
        'start_date': dates.start_date.isoformat(),
        'end_date': dates.end_date.isoformat()
    }
    results = api_client.post('general_ledger_detail', params=params)
    if not results:
        logger.error('API error while executing general ledger detail scrape.')
        return None
    return results.get('task_id')


def get_general_ledger_detail(task_id: str, timeout=480):
    """ Gets results of general ledger detail task with id `task_id`. Returns raw bytes of excel file. Logs and returns `None` on error. """
    params = {
        'task_id': task_id
    }
    start_time = time.time()
    while time.time() - start_time < timeout:
        results = api_client.get('results', params=params)
        if not isinstance(results, dict):
            logger.error('Get general ledger detail failed due to API error.')
            return None
        if results.get('status') == 'pending':
            time.sleep(1)
            continue
        if 'content' in results:
            return base64.b64decode(results['content'])
        break

    logger.error(
        'Get general ledger detail timed out after %d seconds', timeout)
    return None


def execute_general_ledger_summary(xero_url: str, dates: Dates):
    """ Starts the process of navigating Xero to get general ledger summary. Returns `task_id` to retrieve results with. """
    params = {
        'client_id': xero_url,
        'start_date': dates.start_date.isoformat(),
        'end_date': dates.end_date.isoformat()
    }
    results = api_client.post('general_ledger_summary', params=params)
    if not results:
        logger.error(
            'API error while executing general ledger summary scrape.')
        return None
    return results.get('task_id')


def get_general_ledger_summary(task_id: str, timeout=480):
    """ Gets results of general ledger summary task with id `task_id`. Returns raw bytes of excel file. Logs and returns `None` on error. """
    params = {
        'task_id': task_id
    }
    start_time = time.time()
    while time.time() - start_time < timeout:
        results = api_client.get('results', params=params)
        if not isinstance(results, dict):
            logger.error('Get general ledger summary failed due to API error.')
            return None
        if results.get('status') == 'pending':
            time.sleep(1)
            continue
        if 'content' in results:
            return base64.b64decode(results['content'])
        break

    logger.error(
        'Get general ledger summary timed out after %d seconds', timeout)
    return None


def execute_revenue_data(xero_url: str, dates: Dates):
    """ Starts the process of navigating Xero to get revenue data. Returns `task_id` to retrieve results with. """
    params = {
        'client_id': xero_url,
        'start_date': dates.start_date.isoformat(),
        'end_date': dates.end_date.isoformat()
    }
    results = api_client.post('revenue_data', params=params)
    if not results:
        logger.error('API error while revenue data summary scrape.')
        return None
    return results.get('task_id')


def get_revenue_data(task_id: str, timeout=480):
    """ Gets results of revenue data task with id `task_id`. Returns raw bytes of excel file. Logs and returns `None` on error. """
    params = {
        'task_id': task_id
    }
    start_time = time.time()
    while time.time() - start_time < timeout:
        results = api_client.get('results', params=params)
        if not isinstance(results, dict):
            logger.error('Get revenue data failed due to API error.')
            return None
        if results.get('status') == 'pending':
            time.sleep(1)
            continue
        if 'content' in results:
            return base64.b64decode(results['content'])
        break

    logger.error('Get revenue data timed out after %d seconds', timeout)
    return None


def execute_gst_rec(xero_url: str, dates: Dates):
    """ Starts the process of navigating Xero to get GST rec. Returns `task_id` to retrieve results with. """
    params = {
        'client_id': xero_url,
        'start_date': dates.start_date.isoformat(),
        'end_date': dates.end_date.isoformat()
    }
    results = api_client.post('gst_rec', params=params)
    if not results:
        logger.error('API error while executing GST Reconciliation scrape.')
        return None
    return results.get('task_id')


def get_gst_rec(task_id: str, timeout=480):
    """Gets results of GST Rec task with id `task_id`. Returns raw bytes of excel file
    (XLS FILE NOT XLSX FILE BECAUSE XERO IS DUMB). Logs and returns `None` on error.
    """
    params = {
        'task_id': task_id
    }
    start_time = time.time()
    while time.time() - start_time < timeout:
        results = api_client.get('results', params=params)
        if not isinstance(results, dict):
            logger.error('Get GST rec failed due to API error.')
            return None
        if results.get('status') == 'pending':
            time.sleep(1)
            continue
        if 'content' in results:
            return base64.b64decode(results['content'])
        break

    logger.error(
        'Get GST rec timed out after %d seconds', timeout)
    return None

def execute_payable_invoice(xero_url: str, dates: Dates):
    """ Starts the process of navigating Xero to get payable invoice. Returns `task_id` to retrieve results with. """
    params = {
        'client_id': xero_url,
        'start_date': dates.start_date.isoformat(),
        'end_date': dates.end_date.isoformat()
    }
    results = api_client.post('payable_invoice', params=params)
    if not results:
        logger.error('API error while executing payable invoice scrape.')
        return None
    return results.get('task_id')


def get_payable_invoice(task_id: str, timeout=1200):
    """Gets results of payable invoice task with id `task_id`. Returns raw bytes of excel file. Logs and returns `None` on error.
    """
    params = {
        'task_id': task_id
    }
    start_time = time.time()
    while time.time() - start_time < timeout:
        results = api_client.get('results', params=params)
        if not isinstance(results, dict):
            logger.error('Get payable invoice failed due to API error.')
            return None
        if results.get('status') == 'pending':
            time.sleep(1)
            continue
        if 'content' in results:
            return base64.b64decode(results['content'])
        break

    logger.error(
        'Get payable invoice timed out after %d seconds', timeout)
    return None
