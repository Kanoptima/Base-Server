""" Module to handle tasks that invlove Clickup, mainly operating the API and manipulating API received data. """

from base64 import b64encode
from datetime import datetime
import json
import logging
import os
from typing import Optional

from dotenv import load_dotenv

from base_server.helpers.api_client import ApiClient
from base_server.helpers.dates import Dates

load_dotenv(override=True)
logger = logging.getLogger(__name__)
api_client = ApiClient('https://api.yourpayroll.com.au/api/v2/business',
                       {'Authorization': 'Basic ' + b64encode(bytes(f'{os.getenv('KEYPAY_API_KEY')}', 'utf-8')).decode('utf-8')})


def get_business_list() -> Optional[list[dict[str, str]]]:
    """Returns the raw json output of the keypay API's list businesses endpoint.

    Returns:
        Optional[list[dict[str, str]]]: Raw json output of the list businesses endpoint if successful, None otherwise.
    """
    business_list = api_client.get('')
    if not isinstance(business_list, list):
        return None
    return business_list


def get_business_id(name: str):
    """Returns the business ID for a given KeyPay business name.
    Args:
        name (str): Client name to get the ID for.
    
    Returns:
        Optional[str]: Business ID if found, None otherwise.
    """
    business_list = get_business_list()
    business_id = None
    if not business_list:
        logger.error(
            'Get business ID failed: Could not get business list from Keypay.')
        return None
    for business in business_list:
        if business['name'].lower() != name.lower():
            continue
        business_id = business['id']
    if business_id is None or not isinstance(business_id, int):
        logger.error(
            'Get business ID failed: Could not find "%s" in business list.', name)
        return None
    return str(business_id)

def get_roster_shifts(business_id: str, dates: Dates) -> Optional[list[dict]]:
    """Returns the raw json output of the keypay API's get roster shift.

    Args:
        business_id (str): Business ID of business to get roster shifts for.
        dates (Dates): Date range to get roster shifts for.

    Returns:
        Optional[list[dict]]: Raw json output of the roster shift endpoint if successful, None otherwise.
    """
    endpoint = f'{business_id}/rostershift'
    params = {'filter.fromDate': dates.start_str(
    ), 'filter.toDate': dates.end_str()}
    roster_shifts = api_client.get(endpoint, params=params)
    if not isinstance(roster_shifts, list):
        logger.error(
            'Keypay roster shifts API returned non-list response: %s', roster_shifts)
        return None
    return roster_shifts


def simplify_roster_shifts(raw_roster_shifts: list[dict], extracted: str) -> list[dict]:
    """Returns a single depth version of roster_sheets ready for splunk to ingest.

    Args:
        raw_roster_shifts (list[dict]): Raw roster shifts data from Keypay API.
        extracted (str): Date print of the time of data extraction, required for Splunk ingestion.

    Returns:
        list[dict]: Simplified single depth roster shifts data.
    """
    output: list[dict] = []
    for entry in raw_roster_shifts:
        new_entry = {'timestamp': entry['startTime'],
                     'extracted': extracted}
        new_entry.update({key: value for key, value in entry.items() if
                          key not in ['breaks', 'warnings', 'qualifications']})
        new_entry['numberOfBreaks'] = 0
        new_entry['breaksTime'] = 0
        if 'breaks' not in entry:
            output.append(new_entry)
            continue
        for break_obj in entry['breaks']:
            if (break_obj.get('isPaidBreak', True) or
                'startTime' not in break_obj or
                    'endTime' not in break_obj):
                continue
            new_entry['numberOfBreaks'] += 1
            new_entry['breaksTime'] += (datetime.strptime(break_obj['endTime'],
                                                          '%Y-%m-%dT%H:%M:%S').timestamp() -
                                        datetime.strptime(break_obj['startTime'],
                                                          '%Y-%m-%dT%H:%M:%S').timestamp())
        output.append(new_entry)

    return output


def list_employees(business_id: str) -> Optional[list[dict]]:
    """Returns the raw json output of the keypay API's list employees endpoint
    for business with id `business_id`. Returns `None` and logs on error.

    Args:
        business_id (str): Business ID of business to get employees for.

    Returns:
        Optional[list[dict]]: Raw json output of the employee/details endpoint if successful, None otherwise.
    """
    endpoint = f'{business_id}/employee/details'
    employees = api_client.get(endpoint)
    if not isinstance(employees, list):
        logger.error(
            'Keypay employees API returned non-list response: %s', employees)
        return None
    return employees


def get_payroll(business_id: str, dates: Dates) -> Optional[list[dict]]:
    """Returns the raw json output of the keypay API's get payroll endpoint for `business_id` over the `dates` provided.

    Args:
        business_id (str): Business ID of business to get payroll for.
        dates (Dates): Date range to get payroll for.

    Returns:
        Optional[list[dict]]: Raw json output of the payroll endpoint if successful, None otherwise.
    """
    endpoint = f'{business_id}/report/grosstonet'
    params = {'request.fromDate': dates.start_str(
    ), 'request.toDate': dates.end_str()}
    payroll = api_client.get(endpoint, params=params)
    if not isinstance(payroll, list):
        logger.error(
            'Keypay payroll API returned non-list response: %s', payroll)
        return None
    return payroll


def get_leave_liability(business_id: str, date: datetime) -> Optional[list[dict]]:
    """Returns the raw json output of the keypay API's get leave liability endpoint for `business_id` over the `dates` provided.

    Args:
        business_id (str): Business ID of business to get leave liability for.
        date (datetime): Date to get leave liability as at.

    Returns:
        list[dict]: Raw json output of the leave liability endpoint if successful, None otherwise.
    """
    endpoint = f'{business_id}/report/leaveliability'
    params = {'request.asAtDate': date.strftime('%Y-%m-%d')}
    leave_liability = api_client.get(endpoint, params=params)
    if not isinstance(leave_liability, list):
        logger.error(
            'Keypay leave liability API returned non-list response: %s', leave_liability)
        return None
    return leave_liability


def get_payg_withholding(business_id: str, dates: Dates) -> Optional[list[dict]]:
    """Returns the raw json output of the keypay API's get payg endpoint for `business_id` over the `dates` provided.

    Args:
        business_id (str): Business ID of business to get payg for.
        dates (Dates): Date range to get payg withholding for.

    Returns:
        Optional[list[dict]]: Raw json output of the payg endpoint if successful, None otherwise.
    """
    endpoint = f'{business_id}/report/payg'
    params = {'request.fromDate': dates.start_str(
    ), 'request.toDate': dates.end_str()}
    payg_withholding = api_client.get(endpoint, params=params)
    if not isinstance(payg_withholding, list):
        logger.error(
            'Keypay payg API returned non-list response: %s', payg_withholding)
        return None
    return payg_withholding


def list_locations(business_id: str):
    """Gets the Keypay API list of locations for a provided business.

    Args:
        business_id (str): Business ID of business to get locations for.

    Returns:
        Optional[list]: List of locations as dicts if successful, None otherwise.
    """
    endpoint = f'{business_id}/location'
    locations = api_client.get(endpoint)
    if not isinstance(locations, list):
        logger.error(
            'Keypay locations API returned non-list response: %s', locations)
        return None
    return locations


def create_location(business_id: str, location_object: dict):
    """Creates a location in Keypay for a provided business.

    Args:
        business_id (str): Business ID of business to create a location for.
        location_object(dict): 

    Returns:
        Optional[dict]: Created location object if successful, None otherwise.
    """
    endpoint = f'{business_id}/location'
    response = api_client.post(endpoint, data=json.dumps(location_object))
    if not isinstance(response, dict):
        logger.error('Keypay create location API error.')
        return None
    return response


def list_pay_runs(business_id: str):
    """Gets the Keypay API list of pay runs for a provided business.

    Args:
        business_id (str): Business ID of business to get pay runs for.

    Returns:
        Optional[list]: List of pay runs as dicts if successful, None otherwise.
    """
    endpoint = f'{business_id}/payrun'
    params = {
        '$orderby': 'PayPeriodEnding desc'
    }
    pay_runs = api_client.get(endpoint, params=params)
    if not isinstance(pay_runs, list):
        logger.error(
            'Keypay pay runs API returned non-list response: %s', pay_runs)
        return None
    return pay_runs


def list_pay_run_employee_ids(business_id: str, pay_run_id: str):
    """Gets the employee IDs for every employee involved in the specified business and payrun.

    Args:
        business_id (str): Business ID of business to get employee IDs.
        pay_run_id (str): Pay Run ID to get Employee IDs for.

    Returns:
        Optional[list]: List of employee IDs as strings if successful, None otherwise.
    """
    endpoint = f'{business_id}/payrun/{pay_run_id}/earningslines'
    report = api_client.get(endpoint)
    if not isinstance(report, dict):
        logger.error(
            'Keypay pay runs API returned non-dict response: %s', report)
        return None
    earnings_lines = report.get('earningsLines')
    if not isinstance(earnings_lines, dict):
        logger.error('Keypay returned invalid earnings lines report: %s', earnings_lines)
        return None
    return list(earnings_lines.keys())


def get_super_contribution_report(business_id: str, dates: Dates):
    """Returns the raw bytes output of the keypay API's get super contribution report as excel file.

    Args:
        business_id (str): Business ID of business to get super contribution report for.
        dates (Dates): Date range to get super contribution report for.

    Returns:
        Optional[bytes]: Raw excel output of the super contribution report endpoint if successful, None otherwise.
    """
    endpoint = f'{business_id}/report/supercontributions/xlsx'
    params = {
        'SuperContributionsReportExportType': 'AccrualsExcel',
        'FilterType': 'DateRange',
        'GroupBy': 'Employee',
        'FromDate': dates.start_str(),
        'ToDate': dates.end_str()
    }
    variance_report = api_client.get(endpoint, headers={'Accept': 'application/vnd.ms-excel'}, params=params, raw=True)
    if not isinstance(variance_report, bytes):
        logger.error(
            'Keypay super contributions report API returned non-bytes response: %s', variance_report)
        return None
    return variance_report

def get_variance_report(business_id: str, payrun_1: dict, payrun_2: dict):
    """Gets the variance report between two pay runs.

    Args:
        business_id (str): Business ID of business to get variance report for.
        payrun_1 (dict): First pay run to compare.
        payrun_2 (dict): Second pay run to compare.

    Returns:
        Optional[bytes]: Variance report as raw excel file bytes if successful, None otherwise.
    """
    endpoint = f'{business_id}/report/payrunvariance/xlsx'
    params = {
        'PayRunId1': payrun_1['id'],
        'PayRunId2': payrun_2['id'],
        'PayPeriodFrom1': payrun_1['payPeriodStarting'],
        'PayPeriodTo1': payrun_1['payPeriodEnding'],
        'PayPeriodFrom2': payrun_2['payPeriodStarting'],
        'PayPeriodTo2': payrun_2['payPeriodEnding'],
        'ComparisonType': 'PayRuns',
        'HighlightVariancePercentage': 5,
        'OnlyShowVariance': False
    }
    variance_report = api_client.get(endpoint, headers={'Accept': 'application/vnd.ms-excel'}, params=params, raw=True)
    if not isinstance(variance_report, bytes):
        logger.error(
            'Keypay variance report API returned non-bytes response: %s', variance_report)
        return None
    return variance_report

def get_payslip_file(business_id: str, employee_id: str, pay_run_id: str) -> Optional[bytes]:
    """Gets the payslip file for an employee in a pay run.

    Args:
        business_id (str): Business ID of business to get payslip for.
        employee_id (str): Employee ID of employee to get payslip for.
        pay_run_id (str): Pay Run ID of pay run to get payslip for.

    Returns:
        Optional[bytes]: Payslip file as bytes if successful, None otherwise.
    """
    endpoint = f'{business_id}/payrun/{pay_run_id}/file/payslip/{employee_id}'
    response = api_client.get(
        endpoint, headers={'Accept': 'application/pdf'}, raw=True)
    if not isinstance(response, bytes):
        logger.error('Keypay get payslip API error.')
        return None
    return response


def get_audit_report_file(business_id: str, pay_run_id: str):
    """Gets the audit report file for a given pay run.

    Args:
        business_id (str): Business ID of business to get audit report for.
        pay_run_id (str): Pay Run ID of pay run to get audit report for.

    Returns:
        Optional[bytes]: Audit report file as bytes if successful, None otherwise.
    """

    endpoint = f'{business_id}/report/payrunaudit/{pay_run_id}/xlsx'
    params = {
        'ShowAllSummaryDetails': True,
        'ShowAllEmployeeDetails': False
    }
    response = api_client.get(endpoint, headers={
                              'Accept': 'application/vnd.ms-excel'}, params=params, raw=True)
    if not isinstance(response, bytes):
        logger.error('Keypay get audit report API error.')
        return None
    return response
