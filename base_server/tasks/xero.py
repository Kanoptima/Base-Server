""" Module to provide access to Xero API functions. """

from base64 import b64encode
from datetime import datetime, timedelta
import json
import logging
import os
from typing import Optional

from celery import shared_task
from dotenv import load_dotenv

from base_server.helpers.api_client import ApiClient
from base_server.helpers.dates import Dates, iso_to_readable
from base_server.helpers.formatting import to_camel_case
from base_server.helpers.messaging import AutomationMessage
from base_server.models.xero_client import XeroClient


load_dotenv(override=True)

XERO_CLIENT_ID = os.getenv('XERO_CLIENT_ID', '')
CLIENT_SECRET = os.getenv('XERO_CLIENT_SECRET', '')

AUTH_HEADERS = {
    'Authorization': 'Basic ' + b64encode(bytes(XERO_CLIENT_ID + ':' + CLIENT_SECRET, 'utf-8')).decode('utf-8'),
    'Content-Type': 'application/x-www-form-urlencoded'
}

logger = logging.getLogger(__name__)
auth_client = ApiClient('https://identity.xero.com', AUTH_HEADERS)
main_client = ApiClient('https://api.xero.com')


def date_to_readable(xero_datetime: str):
    """ Takes a raw datetime str provided by a xero API response and returns it in the format '%d/%m/%Y %H:%M:%S' """
    if xero_datetime is None:
        return None
    # Extract the epoch time in milliseconds from the string
    epoch_time_ms = int(xero_datetime[6:-2])

    # Convert milliseconds to seconds
    epoch_time_s = epoch_time_ms / 1000

    # Convert to human-readable format (MM/DD/YYYY HH:MM:SS)
    readable_date = datetime.fromtimestamp(
        epoch_time_s).strftime('%d/%m/%Y %H:%M:%S')
    return readable_date


def date_to_seconds(xero_datetime: str, timezone_present: bool = False):
    """ Takes a raw datetime str provided by a xero API response and returns it in seconds since epoch. """
    if xero_datetime is None:
        return None
    # Extract the epoch time in milliseconds from the string
    if timezone_present:
        epoch_time_ms = int(xero_datetime[6:-7])
    else:
        epoch_time_ms = int(xero_datetime[6:-2])

    # Convert milliseconds to seconds
    epoch_time_s = epoch_time_ms // 1000
    return epoch_time_s


def refresh_client_tokens(client: XeroClient):
    """ Uses the refresh token for `client` to refresh access and refresh tokens and update the database with the new tokens. """
    refresh_token = client.get_xero_refresh_token()
    if not refresh_token:
        logger.error(
            'Could not refresh xero access token for "%s" as there was no refresh token in the database.', client.id)
        return False

    data = {
        'grant_type': 'refresh_token',
        'refresh_token': refresh_token
    }
    tokens = auth_client.post('connect/token', data=data)
    if not tokens:
        logger.error(
            'Refresh Xero access token failed for "%s" due to API error.', client)
        return False

    if 'access_token' not in tokens or 'refresh_token' not in tokens or 'refresh_token' not in tokens:
        logger.error(
            'Refresh Xero access token failed for "%s" due to missing tokens in response.', client)
        return False

    expiry_time = datetime.now() + timedelta(seconds=tokens['expires_in'])
    client.set_tokens(tokens['access_token'],
                      tokens['refresh_token'], expiry_time)

    return True


def simplify_profit_loss(report: dict, name: str, dates: Dates):
    """ Takes raw profit and loss report json response and simplifies it for Splunk ingestion. """
    simplified_report = {'extracted': date_to_readable(report.get('DateTimeUTC', '')),
                         'start': dates.start_str(),
                         'end': dates.end_str(),
                         'report_name': name}
    report_obj = report['Reports'][0]

    substitutions = {
        'Less Cost of Sales': 'CoS',
        'Less Operating Expenses': 'OpEx',
        'Plus Other Income': 'incomeOther',
        'Gross Profit': 'gross_profit',
        'Net Profit': 'net_profit'
    }

    sections: list[dict] = report_obj.get('Rows', [])
    for section in sections:
        if section.get('RowType') == 'Header':
            continue

        # If there is an explicit substition for the section title, use that, otherwise use the camel case conversion
        section_title: str = section.get('Title', '')
        section_title = substitutions.get(
            section_title, to_camel_case(section_title))
        if section_title != '':
            section_title += '_'

        rows = section.get('Rows')
        if rows is None:
            continue

        for row in rows:
            row_title = row.get('Cells')[0].get('Value')
            row_value = row.get('Cells')[1].get('Value')
            row_title = substitutions.get(row_title, row_title)
            if not isinstance(row_title, str):
                raise RuntimeError('Row title is not a string.')
            row_title = to_camel_case(row_title)
            simplified_report[section_title+row_title] = row_value

    return simplified_report


def get_profit_loss(xero_client_id: int, name: str, dates: Dates):
    """ Gets the profit and loss report from Xero API and returns the simplified version of the json response.
        All potential errors are logged and `None` is returned in this event. """
    client = XeroClient.get_by_id(xero_client_id)
    if not client:
        logger.error(
            'Could not get profit loss for "%s" as the client was not found', xero_client_id)
        return None

    tenant_id, access_token = get_tokens(client)
    if tenant_id is None:
        logger.error(
            'Could not get profit loss for "%s" as the client did not have a tenant ID.', xero_client_id)
        return None
    if access_token is None:
        logger.error(
            'Could not get profit loss for "%s" as the client did not have an access token.', xero_client_id)
        return None

    endpoint = 'api.xro/2.0/Reports/ProfitAndLoss'
    headers = {
        'Authorization': f'Bearer {access_token}',
        'Xero-Tenant-Id': tenant_id,
        'Accept': 'application/json'
    }
    params = {
        'fromDate': dates.start_str(),
        'toDate': dates.start_str()
    }

    raw_profit_loss = main_client.get(endpoint, headers=headers, params=params)
    if not raw_profit_loss:
        logger.error(
            'Get profit and loss API request failed for "%s".', xero_client_id)
        return None

    return simplify_profit_loss(raw_profit_loss, name, dates)


def simplify_journals(journals_raw: dict[str, list[dict]], extracted: str):
    """ Simplifies journals to be single depth, ready for splunk ingestion. """
    journal_lines = []

    for journal in journals_raw['Journals']:
        if not journal:
            continue
        raw_journal_lines = journal.get('JournalLines', [])
        for line in raw_journal_lines:
            new_line = {
                'timestamp': date_to_seconds(journal.get('JournalDate', ''), True),
                'extracted': extracted,
                'journal_id': journal.get('JournalID'),
                'journal_number': journal.get('JournalNumber'),
                'created_date_utc': date_to_seconds(journal.get('CreatedDateUTC', ''), True),
                'journal_reference': journal.get('Reference'),
                'source_id': journal.get('SourceID'),
                'journal_type': journal.get('SourceType'),
                'journal_line_id': line.get('JournalLineID'),
                'account_id': line.get('AccountID'),
                'account_code': line.get('AccountCode'),
                'account_type': line.get('AccountType'),
                'account_name': line.get('AccountName'),
                'description': line.get('Description'),
                'net_amount': line.get('NetAmount'),
                'gross_amount': line.get('GrossAmount'),
                'tax_amount': line.get('TaxAmount'),
                'tax_name': line.get('TaxName'),
                'tracking': line.get('TrackingCategories'),
            }
            journal_lines.append(new_line)

    return journal_lines


def get_journals(xero_client_id: int, since_date: datetime):
    """ Gets Journals for `org_id` since `since_date` using Xero API. Loops requesting over new pages until all pages
        are collected. Returns `journal_lines`, which are the simplified journal lines from the json response. """
    client = XeroClient.get_by_id(xero_client_id)
    if not client:
        logger.error(
            'Could not get journals for "%s" as the client was not found', xero_client_id)
        return None

    tenant_id, access_token = get_tokens(client)
    if tenant_id is None:
        logger.error(
            'Could not get journals for "%s" as the client did not have a tenant ID.', xero_client_id)
        return None
    if access_token is None:
        logger.error(
            'Could not get journals for "%s" as the client did not have an access token.', xero_client_id)
        return None

    if_modified_since = since_date.isoformat()
    endpoint = 'api.xro/2.0/Journals'
    headers = {
        'Authorization': f'Bearer {access_token}',
        'Xero-Tenant-Id': tenant_id,
        'Accept': 'application/json',
        'If-Modified-Since': if_modified_since
    }

    journal_lines = []
    params = None

    while True:
        extracted = datetime.now().strftime('%d/%m/%Y %H:%M:%S')
        journals_raw = main_client.get(
            endpoint, headers=headers, params=params)
        if not journals_raw:
            logger.error(
                'Get journals API request failed for "%s".', xero_client_id)
            return None
        new_lines = simplify_journals(journals_raw, extracted)
        if len(new_lines) == 0:
            break

        journal_lines += new_lines
        params = {'offset': new_lines[-1]['journal_number']}

    return journal_lines


def simplify_payments(payments_raw: dict[str, dict], extracted: str):
    """ Simplifies journals to be single depth, ready for splunk ingestion. """
    payments = []

    for payment in payments_raw['Payments']:
        new_payment = {
            'extracted': extracted,
            'timestamp': date_to_seconds(payment.get('Date', ''), True),
            'payment_id': payment.get('PaymentID'),
            'bank_amount': payment.get('BankAmount'),
            'amount': payment.get('Amount'),
            'currency_rate': payment.get('CurrencyRate'),
            'payment_type': payment.get('PaymentType'),
            'status': payment.get('Status'),
            'updated_date_utc': date_to_seconds(payment.get('UpdatedDateUTC', ''), True),
            'has_account': payment.get('HasAccount'),
            'is_reconciled': payment.get('IsReconciled'),
            'account_id': payment.get('Account').get('AccountID'),
            'account_code': payment.get('Account').get('Code'),
            'invoice_type': payment.get('Invoice').get('Type'),
            'invoice_id': payment.get('Invoice')['InvoiceID'],
            'invoice_is_discounted': payment.get('Invoice')['IsDiscounted'],
            'invoice_has_errors': payment.get('Invoice')['HasErrors'],
            'contact_id': payment.get('Invoice')['Contact']['ContactID'],
            'contact_name': payment.get('Invoice')['Contact']['Name'],
            'contact_has_validation_errors': payment.get('Invoice')['Contact']['HasValidationErrors'],
            'invoice_currency_code': payment.get('Invoice')['CurrencyCode'],
            'has_validation_errors': payment.get('HasValidationErrors')
        }
        payments.append(new_payment)

    return payments


def get_payments(xero_client_id: int, dates: Dates):
    """ Returns payments for client with id `xero_client_id` with id in `payment_ids`. """
    client = XeroClient.get_by_id(xero_client_id)
    if not client:
        logger.error(
            'Could not get profit loss for "%s" as the client was not found', xero_client_id)
        return None

    tenant_id, access_token = get_tokens(client)
    if tenant_id is None:
        logger.error(
            'Could not get profit loss for "%s" as the client did not have a tenant ID.', xero_client_id)
        return None
    if access_token is None:
        logger.error(
            'Could not get profit loss for "%s" as the client did not have an access token.', xero_client_id)
        return None

    endpoint = 'api.xro/2.0/Payments'
    headers = {
        'Authorization': f'Bearer {access_token}',
        'Xero-Tenant-Id': tenant_id,
        'Accept': 'application/json'
    }
    params = {
        'where': dates.xero_where_str()
    }

    extracted = datetime.now().strftime('%d/%m/%Y %H:%M:%S')
    payments_raw = main_client.get(endpoint, headers, params=params)
    if not payments_raw:
        logger.error(
            'Get payments failed for "%s" due to API error.', xero_client_id)
        return None

    return simplify_payments(payments_raw, extracted)


def simplify_invoice_payments(invoices_raw: dict[str, list[dict]], extracted: str):
    """ Simplifies invoices to be single depth, ready for splunk ingestion. There are two types of objects
        in the resulting list, invoices and payments, that are linked by a common invoice ID. """
    invoice_payments: list[dict] = []

    for invoice in invoices_raw['Invoices']:
        new_invoice = {
            'extracted': extracted,
            'timestamp': date_to_seconds(invoice.get('Date', ''), True),
            'invoice_id': invoice.get('InvoiceID'),
            'due_date': date_to_seconds(invoice.get('DueDate', ''), True),
            'invoice_number': invoice.get('InvoiceNumber'),
            'updated_date_utc': date_to_seconds(invoice.get('UpdatedDateUTC', ''), True),
            'invoice_reference': invoice.get('Reference'),
            'invoice_type': invoice.get('Type'),
            'currency_rate': invoice.get('CurrencyRate'),
            'contact_name': invoice.get('Contact', {}).get('Name'),
            'status': invoice.get('Status'),
            'sub_total': invoice.get('SubTotal'),
            'total_tax': invoice.get('TotalTax'),
            'total': invoice.get('Total')
        }
        invoice_payments.append(new_invoice)

        raw_payments: list[dict] = invoice.get('Payments', [])
        for payment in raw_payments:
            new_payment = {
                'extracted': extracted,
                'timestamp': date_to_seconds(invoice.get('Date', ''), True),
                'invoice_id': invoice.get('InvoiceID'),
                'payment_date': date_to_seconds(payment.get('Date', ''), True),
                'payment_amount': payment.get('Amount'),
                'currency_rate': payment.get('CurrencyRate')
            }
            invoice_payments.append(new_payment)

    return invoice_payments


def get_invoice_payments_since_date(xero_client_id: int, date: datetime):
    """ Returns simplified invoice/payments for client with id `xero_client_id` that are modified since `date`. """
    client = XeroClient.get_by_id(xero_client_id)
    if not client:
        logger.error(
            'Could not get profit loss for "%s" as the client was not found', xero_client_id)
        return None

    tenant_id, access_token = get_tokens(client)
    if tenant_id is None:
        logger.error(
            'Could not get profit loss for "%s" as the client did not have a tenant ID.', xero_client_id)
        return None
    if access_token is None:
        logger.error(
            'Could not get profit loss for "%s" as the client did not have an access token.', xero_client_id)
        return None

    endpoint = 'api.xro/2.0/Invoices'
    headers = {
        'Authorization': f'Bearer {access_token}',
        'Xero-Tenant-Id': tenant_id,
        'Accept': 'application/json',
        'If-Modified-Since': date.isoformat()
    }

    extracted = datetime.now().strftime('%d/%m/%Y %H:%M:%S')
    invoices_raw = main_client.get(endpoint, headers)
    if not invoices_raw:
        logger.error(
            'Get invoices since date failed for "%s" due to API error.', xero_client_id)
        return None

    return simplify_invoice_payments(invoices_raw, extracted)


def get_invoice_key(xero_client_id: int, invoice_ids: list[str]):
    """ Returns dict keyed by invoice ids, with each value being a str of the invoice reference and the url in brackets afterwards. """
    # Get invoice link and reference using invoice api

    invoice_key = {}
    for invoice_id in invoice_ids:

        # Get invoice report, the invoice itself is stored in Invoices field, which is a single item list
        invoice_report = get_invoice(xero_client_id, invoice_id)
        if invoice_report is None:
            continue
        invoice: dict = invoice_report.get('Invoices')[0]

        # Reference field will often have an empty str, in this case we just use the invoice ID so there is some text to use as a link
        reference = invoice.get('Reference')
        if not reference:
            reference = invoice.get('InvoiceNumber')

        # Store the relevant information of the invoice only, which is the reference or close enough and the attachment information
        invoice_key[invoice_id] = {
            'reference': reference,
            'attachments': invoice.get('Attachments', []),
            'id': invoice_id
        }

    return invoice_key


def get_contacts(xero_client_id: int, where_fliter: str):
    """ Gets contacts for a client with id `xero_client_id`, originally to enable the use of aged payables endpoint. """
    client = XeroClient.get_by_id(xero_client_id)
    if not client:
        logger.error(
            'Could not get Xero contacts for "%s" as the client was not found', xero_client_id)
        return None

    tenant_id, access_token = get_tokens(client)
    if tenant_id is None:
        logger.error(
            'Could not get Xero contacts for "%s" as the client did not have a tenant ID.', xero_client_id)
        return None
    if access_token is None:
        logger.error(
            'Could not get Xero contacts for "%s" as the client did not have an access token.', xero_client_id)
        return None

    endpoint = 'api.xro/2.0/Contacts'
    headers = {
        'Authorization': f'Bearer {access_token}',
        'Xero-Tenant-Id': tenant_id,
        'Accept': 'application/json'
    }
    params = {
        'where': where_fliter,
        'page': 1,
        'pageSize': 250
    }

    response: dict = main_client.get(endpoint, headers, params=params)
    if response:
        clients: list[dict] = response.get('Contacts', [])
        return clients

    return None


def get_contact_groups(xero_client_id: int, contact_group_id: Optional[str] = None):
    """ Gets contact groups for a client with id `xero_client_id`. If contact_group_id is provided,
        the contacts in that group will be included in the results. """
    client = XeroClient.get_by_id(xero_client_id)
    if not client:
        logger.error(
            'Could not get Xero contact groups for "%s" as the client was not found', xero_client_id)
        return None

    tenant_id, access_token = get_tokens(client)
    if tenant_id is None:
        logger.error(
            'Could not get Xero contact groups for "%s" as the client did not have a tenant ID.', xero_client_id)
        return None
    if access_token is None:
        logger.error(
            'Could not get Xero contact groups for "%s" as the client did not have an access token.', xero_client_id)
        return None

    if contact_group_id:
        endpoint = f'api.xro/2.0/ContactGroups/{contact_group_id}'
    else:
        endpoint = 'api.xro/2.0/ContactGroups'

    headers = {
        'Authorization': f'Bearer {access_token}',
        'Xero-Tenant-Id': tenant_id,
        'Accept': 'application/json'
    }

    response: dict = main_client.get(endpoint, headers)
    if response:
        return response

    logger.error(
        'Could not get Xero contact groups for "%s" due to API error.')
    return None


def formulate_aged_payables_row(cells: list[dict], report_date: datetime, column_key: dict[str, int],
                                invoice_ids: list[str], month_groups: int, age_by_due_date: bool):
    """ Create new row for simplified aged payables report. """

    # Get invoice ID from the first cells attributes
    invoice_id = ''
    attributes: list[dict] = cells[0].get('Attributes', [])
    if not attributes:
        return None
    for attribute in attributes:
        if attribute.get('Id') != 'invoiceID':
            continue
        invoice_id = attribute.get('Value', '')

    # I think this determines whether or not we want to use it
    try:
        due_value = float(cells[column_key['due']]['Value'])
        if due_value == 0:
            return None
    except (ValueError, KeyError):
        return None

    new_row = [iso_to_readable(cells[column_key['date']].get('Value', '')), iso_to_readable(
        cells[column_key['due_date']].get('Value', '')), invoice_id]

    # If there is no ageing date, don't make row
    try:
        if age_by_due_date:
            ageing_date = datetime.fromisoformat(
                cells[column_key['due_date']].get('Value', ''))
        else:
            ageing_date = datetime.fromisoformat(
                cells[column_key['date']].get('Value', ''))
    except ValueError:
        return None

    # Add invoice ID to the list only once we know that we're actually going to use it
    invoice_ids.append(invoice_id)

    # Determine which column the value should be placed in, have empty columns before and after to match where it should be
    if ageing_date > report_date:
        new_row += [due_value] + ([0] * (month_groups + 1)) + [due_value]
    else:
        year_diff = report_date.year - ageing_date.year
        month_diff = report_date.month - ageing_date.month
        total_month_diff = year_diff * 12 + month_diff
        total_month_diff = max(min(total_month_diff, month_groups), 0)
        new_row += ([0] * (total_month_diff + 1)) + [due_value] + \
            ([0] * (month_groups - total_month_diff)) + [due_value]
    return new_row


def formulated_aged_payables_section(rows: list[dict], report_date: datetime, column_key: dict[str, int],
                                     invoice_ids: list[str], month_groups: int, age_by_due_date: bool):
    """ Returns simplified section of aged payables report. """
    section = []
    for row in rows:
        row_type = row.get('RowType')

        # If this is a regular or summary row, add the cells as the next row
        if row_type in ['Row', 'SummaryRow']:
            new_row = formulate_aged_payables_row(
                row['Cells'], report_date, column_key, invoice_ids, month_groups, age_by_due_date)
            if not new_row:
                continue
            section.append(new_row)

        # If this is a section, add each of the subrows to the report
        if row_type == 'Section':
            subrows: list[dict] = row.get('Rows', [])
            for subrow in subrows:
                new_row = formulate_aged_payables_row(
                    subrow['Cells'], report_date, column_key, invoice_ids, month_groups, age_by_due_date)
                if not new_row:
                    continue
                section.append(new_row)

    return section


def generate_aged_payables_column_key(title_cells: list[dict]):
    """ Creates column key for formulate_aged_payables_row that informs what column contains what data. """
    column_key = {}
    for i, cell in enumerate(title_cells):
        value = cell['Value']
        if value == 'Date':
            column_key['date'] = i
            continue
        if value == 'Reference':
            column_key['reference'] = i
            continue
        if value == 'Due Date':
            column_key['due_date'] = i
            continue
        if value == 'Due':
            column_key['due'] = i
            continue
        if value == 'Due AUD':
            column_key['due'] = i
            continue
        if value == 'Due NZD':
            column_key['due'] = i
            continue
        if value == 'Due USD':
            column_key['due'] = i
            continue
        if value == 'Due EUR':
            column_key['due'] = i
            continue

    return column_key


def simplify_aged_payables(xero_client_id: int, contact_reports: list[dict], report_date: datetime, month_groups: int, age_by_due_date: bool):
    """ Simplifies the `rows` of one aged payables reports and adds them to the `overall_report`. """

    # Stores collections of report rows indexed by the name of the contact
    organised_report: dict[str, list[list]] = {}

    invoice_ids = []
    for contact_report in contact_reports:
        # Add entry for contact name
        contact_name = contact_report.get('contact_name', '')

        # Determine what column represents what
        column_key = generate_aged_payables_column_key(
            contact_report['Reports'][0]['Rows'][0]['Cells'])

        # Add each row to this entry
        rows: list[dict] = contact_report['Reports'][0]['Rows']
        section = formulated_aged_payables_section(
            rows, report_date, column_key, invoice_ids, month_groups, age_by_due_date)
        if len(section) > 0:
            organised_report[contact_name] = section

    # Replace invoice IDs with invoice reference and link
    invoice_key = get_invoice_key(xero_client_id, invoice_ids)
    for subsection in organised_report.values():
        for row in subsection:
            row[2] = invoice_key.get(row[2])

    sorted_report = {key: organised_report[key]
                     for key in sorted(organised_report.keys())}
    return sorted_report


def get_aged_payables(xero_client_id: int, date: datetime, month_groups: int, age_by_due_date: bool):
    """ Gets the aged payables for client with id `xero_client_id` for the contact
        with id `contact_id` since `since_date`. Returns `None` and logs on error. """
    # This appears to not be viable, since at least for BAS, there are way too many contacts, it will take forever and cost too many requests
    client = XeroClient.get_by_id(xero_client_id)
    if not client:
        logger.error(
            'Could not get aged payables for "%s" as the client was not found', xero_client_id)
        return None

    tenant_id, access_token = get_tokens(client)
    if tenant_id is None:
        logger.error(
            'Could not get aged payables for "%s" as the client did not have a tenant ID.', xero_client_id)
        return None
    if access_token is None:
        logger.error(
            'Could not get aged payables for "%s" as the client did not have an access token.', xero_client_id)
        return None

    # Find list of all contact groups
    contact_groups_report = get_contact_groups(xero_client_id)
    if not contact_groups_report:
        logger.warning(
            'Could not get contact groups for "%s" during aged payables get.', xero_client_id)
        contact_groups = []
    else:
        contact_groups: list[dict] = contact_groups_report.get(
            'ContactGroups', [])

    # Find CPI contact group id from report if it is there
    contact_group_id = None
    for contact_group in contact_groups:
        if contact_group.get('Name') == 'CPI':
            contact_group_id = contact_group.get('ContactGroupID')
            break

    # If there is a CPI contact group create a set of all contact IDs in the group, which can later be used as a filter
    contact_filter = None
    if contact_group_id:
        contact_groups_report = get_contact_groups(
            xero_client_id, contact_group_id)
        if not contact_groups_report:
            logger.error(
                'Could not get contact group "%s" for "%s" during aged payables get.', contact_group_id, xero_client_id)
            return None
        contact_filter = set()
        cpi_contacts: list[dict] = contact_groups_report.get('ContactGroups', [])[
            0].get('Contacts', [])
        for contact in cpi_contacts:
            contact_filter.add(contact.get('ContactID'))

    # Get contacts that have accounts payable, they are the required contacts for aged payables report
    where_filter = ('ContactStatus=="ACTIVE" AND isSupplier==true AND Balances!=null AND Balances.AccountsPayable!=null AND '
                    'Balances.AccountsPayable.Outstanding!=null AND Balances.AccountsPayable.Outstanding>0')
    contacts = get_contacts(xero_client_id, where_filter)
    if not contacts:
        logger.error(
            'Could not get aged payables for "%s" as the get contacts call returned None.', xero_client_id)
        return None

    endpoint = 'api.xro/2.0/Reports/AgedPayablesByContact'
    headers = {
        'Authorization': f'Bearer {access_token}',
        'Xero-Tenant-Id': tenant_id,
        'Accept': 'application/json'
    }

    contact_reports = []
    for contact in contacts:
        # Don't use contacts that aren't in the CPI contact group if it was found
        if contact_filter and contact.get('ContactID') not in contact_filter:
            continue
        params = {
            'contactID': contact.get('ContactID'),
            'toDate': date.strftime('%Y-%m-%d')
        }
        contact_report = main_client.get(endpoint, headers, params=params)
        contact_report['contact_name'] = contact.get('Name')
        contact_reports.append(contact_report)

    return simplify_aged_payables(xero_client_id, contact_reports, date, month_groups, age_by_due_date)


def simplify_trial_balance(raw_trial_balance: dict[str, list[dict]]):
    """ Simplifies trial balance and returns `dict[str, list[list]]` where each key is a section
        title and the values are the section arrays, logs and returns `None` on error. """
    rows = raw_trial_balance.get('Reports', [{}])[0].get('Rows')
    if not isinstance(rows, list):
        logger.error(
            'Could not simplify trial balance: "Rows" field is not a list')
        return None

    output = {}
    for row in rows:
        if not isinstance(row, dict):
            logger.error(
                'Could not simplify trial balance: "Rows" field contains non-dict item')
            return None
        row_type = row.get('RowType')
        if not row_type or row_type == 'header' or row.get('Title', '') == '':
            continue

        section = []
        subrow: dict[str, list[dict]]
        for subrow in row.get('Rows', []):
            section.append([cell.get('Value')
                           for cell in subrow.get('Cells', [])])
        output[row.get('Title')] = section

    return output


def get_trial_balance(xero_client_id: int, date: datetime):
    """ Gets Trial Balance for client with id `xero_client_id` as at `date`. """

    client = XeroClient.get_by_id(xero_client_id)
    if not client:
        logger.error(
            'Could not get trial balance for "%s" as the client was not found', xero_client_id)
        return None

    tenant_id, access_token = get_tokens(client)
    if tenant_id is None:
        logger.error(
            'Could not get trial balance for "%s" as the client did not have a tenant ID.', xero_client_id)
        return None
    if access_token is None:
        logger.error(
            'Could not get trial balance for "%s" as the client did not have an access token.', xero_client_id)
        return None

    endpoint = 'api.xro/2.0/Reports/TrialBalance'
    headers = {
        'Authorization': f'Bearer {access_token}',
        'Xero-Tenant-Id': tenant_id,
        'Accept': 'application/json'
    }
    params = {
        'date': date.strftime('%Y-%m-%d')
    }

    raw_trial_balance = main_client.get(endpoint, headers, params=params)
    if not raw_trial_balance:
        logger.error(
            'Could not get trial balance for "%s" due to an API error.', xero_client_id)
        return None
    return simplify_trial_balance(raw_trial_balance)


def simplify_balance_sheet(balance_sheet: dict[str, list[dict[str, list]]], date: datetime, extracted: datetime):
    """ Simplifies the Xero API json response for balance sheet into
        a list of lists that can be saved as a csv for easy splunk ingestion. """

    output: list[list] = [['date', 'extracted', 'account', 'value']]
    sections: list[dict[str, list[dict]]] = balance_sheet['Reports'][0]['Rows']
    for section in sections:
        if section.get('RowType') != 'Section':
            continue
        for row in section.get('Rows', []):
            if row.get('RowType') != 'Row':
                continue
            try:
                account = row['Cells'][0]['Value']
                if account == 'Net Assets':
                    continue
                value = row['Cells'][1]['Value']
            except (IndexError, KeyError):
                continue
            try:
                value_float = float(value)
                if value_float == 0:
                    continue
                output.append([date.strftime('%d %b %Y'), extracted.strftime(
                    "%d/%m/%Y %H:%M:%S"), account, value_float])
            except ValueError:
                output.append([date.strftime('%d %b %Y'), extracted.strftime(
                    "%d/%m/%Y %H:%M:%S"), account, value])

    return output


def get_balance_sheet(xero_client_id: int, date: datetime):
    """ Gets Balance Sheet for client with id `xero_client_id` as at `date`. """

    client = XeroClient.get_by_id(xero_client_id)
    if not client:
        logger.error(
            'Could not get balance sheet for "%s" as the client was not found', xero_client_id)
        return None

    tenant_id, access_token = get_tokens(client)
    if tenant_id is None:
        logger.error(
            'Could not get balance sheet for "%s" as the client did not have a tenant ID.', xero_client_id)
        return None
    if access_token is None:
        logger.error(
            'Could not get balance sheet for "%s" as the client did not have an access token.', xero_client_id)
        return None

    endpoint = 'api.xro/2.0/Reports/BalanceSheet'
    headers = {
        'Authorization': f'Bearer {access_token}',
        'Xero-Tenant-Id': tenant_id,
        'Accept': 'application/json'
    }
    params = {
        'date': date.strftime('%Y-%m-%d')
    }

    extracted = datetime.now()
    unsimplified = main_client.get(endpoint, headers, params=params)
    if not unsimplified:
        logger.error(
            'Could not get balance sheet for "%s" due to an API error.', xero_client_id)
        return None
    simplified = simplify_balance_sheet(unsimplified, date, extracted)

    return simplified


def get_tracking_categories(xero_client_id: int):
    """Get list of all tracking categories

    Args:
        xero_client_id (int): XeroClient ID to get tracking categories for

    Returns:
        dict: Tracking categories
    """

    client = XeroClient.get_by_id(xero_client_id)
    if not client:
        logger.error(
            'Could not get tracking categories for "%s" as the client was not found', xero_client_id)
        return None

    tenant_id, access_token = get_tokens(client)
    if tenant_id is None:
        logger.error(
            'Could not get tracking categories for "%s" as the client did not have a tenant ID.', xero_client_id)
        return None
    if access_token is None:
        logger.error(
            'Could not get tracking categories for "%s" as the client did not have an access token.', xero_client_id)
        return None

    endpoint = 'api.xro/2.0/TrackingCategories'
    headers = {
        'Authorization': f'Bearer {access_token}',
        'Xero-Tenant-Id': tenant_id,
        'Accept': 'application/json'
    }

    tracking_categories = main_client.get(endpoint, headers)
    if not tracking_categories:
        logger.error(
            'Could not get tracking categories for "%s" due to an API error.', xero_client_id)
        return None

    return tracking_categories


def get_manual_journal(xero_client_id, journal_id):
    """Gets manual journal and returns dict representation if successful, None otherwise.

    Args:
        xero_client_id (str): XeroClient to post manual journal for
        journal_id (str): ID of manual journal to get

    Returns:
        Optional[dict]: dict representation of manual journal if successful, None otherwise.
    """
    client = XeroClient.get_by_id(xero_client_id)
    if not client:
        logger.error(
            'Could not get manual journal for "%s" as the client was not found', xero_client_id)
        return None

    tenant_id, access_token = get_tokens(client)
    if tenant_id is None:
        logger.error(
            'Could not get manual journal for "%s" as the client did not have a tenant ID.', xero_client_id)
        return None
    if access_token is None:
        logger.error(
            'Could not get manual journal for "%s" as the client did not have an access token.', xero_client_id)
        return None

    endpoint = f'api.xro/2.0/ManualJournals/{journal_id}'
    headers = {
        'Authorization': f'Bearer {access_token}',
        'Xero-Tenant-Id': tenant_id,
        'Accept': 'application/json'
    }

    response = main_client.get(endpoint, headers)
    if not response:
        logger.error(
            'Could not get manual journal for "%s" due to an API error.', xero_client_id)
        return None

    return response


def post_manual_journal(xero_client_id: int, narration: str, journal_lines: list[dict], date: Optional[datetime] = None):
    """Posts new manual journal and returns dict representation if successful, None otherwise.

    Args:
        xero_client_id (str): XeroClient to post manual journal for
        narration (str): Description of journal
        journal_lines (list[dict]): At least 2 journal lines, where each line is a dict containing
            'LineAmount' and 'AccountCode' keys with corresponding values.
        date (Optional[datetime]): Date of journal, defaults to current date

    Returns:
        Optional[dict]: dict representation of newly created manual journal if successful, None otherwise.
    """
    client = XeroClient.get_by_id(xero_client_id)
    if not client:
        logger.error(
            'Could not post manual journal for "%s" as the client was not found', xero_client_id)
        return None

    tenant_id, access_token = get_tokens(client)
    if tenant_id is None:
        logger.error(
            'Could not post manual journal for "%s" as the client did not have a tenant ID.', xero_client_id)
        return None
    if access_token is None:
        logger.error(
            'Could not post manual journal for "%s" as the client did not have an access token.', xero_client_id)
        return None

    endpoint = 'api.xro/2.0/ManualJournals'
    headers = {
        'Authorization': f'Bearer {access_token}',
        'Xero-Tenant-Id': tenant_id,
        'Accept': 'application/json'
    }
    data = {
        'Status': 'POSTED',
        'Narration': narration,
        'JournalLines': journal_lines
    }
    if date:
        data['Date'] = date.strftime('%Y-%m-%d')

    response = main_client.post(endpoint, headers, data=json.dumps(data))
    if not response:
        logger.error(
            'Could not post manual journal for "%s" due to an API error.', xero_client_id)
        return None

    return response


def post_manual_journal_attachment(xero_client_id: int, journal_id: str, path: str):
    """Posts an attachment (currently only supports pdf files) to a specified manual journal

    Args:
        xero_client_id (str): XeroClient of manual journal
        journal_id (str): ID of manual journal to upload to
        path (str): Path to pdf file to be uploaded
    """
    client = XeroClient.get_by_id(xero_client_id)
    if not client:
        logger.error(
            'Could not post manual journal attachment for "%s" as the client was not found', xero_client_id)
        return None

    tenant_id, access_token = get_tokens(client)
    if tenant_id is None:
        logger.error(
            'Could not post manual journal attachment for "%s" as the client did not have a tenant ID.', xero_client_id)
        return None
    if access_token is None:
        logger.error(
            'Could not post manual journal attachment for "%s" as the client did not have an access token.', xero_client_id)
        return None

    endpoint = f'api.xro/2.0/ManualJournals/{journal_id}/Attachments/{os.path.basename(path)}'
    with open(path, 'rb') as file:
        data = file.read()
    headers = {
        'Authorization': f'Bearer {access_token}',
        'Xero-Tenant-Id': tenant_id,
        'Content-Type': 'application/pdf'
    }

    response = main_client.post(endpoint, headers, data=data, raw=True)
    if not response:
        logger.error(
            'Could not post manual journal attachment for "%s" due to an API error.', xero_client_id)
        return None

    return response


def post_manual_journal_attachment_bytes(xero_client_id: int, journal_id: str, file: bytes, name: str):
    """Posts an attachment (currently only supports pdf files) to a specified manual journal using raw bytes data

    Args:
        xero_client_id (str): XeroClient of manual journal
        journal_id (str): ID of manual journal to upload to
        path (str): Path to pdf file to be uploaded
    """
    client = XeroClient.get_by_id(xero_client_id)
    if not client:
        logger.error(
            'Could not post manual journal attachment for "%s" as the client was not found', xero_client_id)
        return None

    tenant_id, access_token = get_tokens(client)
    if tenant_id is None:
        logger.error(
            'Could not post manual journal attachment for "%s" as the client did not have a tenant ID.', xero_client_id)
        return None
    if access_token is None:
        logger.error(
            'Could not post manual journal attachment for "%s" as the client did not have an access token.', xero_client_id)
        return None

    endpoint = f'api.xro/2.0/ManualJournals/{journal_id}/Attachments/{name}'
    headers = {
        'Authorization': f'Bearer {access_token}',
        'Xero-Tenant-Id': tenant_id,
        'Content-Type': 'application/pdf'
    }

    response = main_client.post(endpoint, headers, data=file, raw=True)
    if not response:
        logger.error(
            'Could not post manual journal attachment for "%s" due to an API error.', xero_client_id)
        return None

    return response


def get_invoice(xero_client_id: int, invoice_id: str):
    """ Gets raw invoice for client with id `xero_client_id` with invoice id `invoice_id`. """

    client = XeroClient.get_by_id(xero_client_id)
    if not client:
        logger.error(
            'Could not get invoice for "%s" as the client was not found', xero_client_id)
        return None

    tenant_id, access_token = get_tokens(client)
    if tenant_id is None:
        logger.error(
            'Could not get invoice for "%s" as the client did not have a tenant ID.', xero_client_id)
        return None
    if access_token is None:
        logger.error(
            'Could not get invoice for "%s" as the client did not have an access token.', xero_client_id)
        return None

    endpoint = f'api.xro/2.0/Invoices/{invoice_id}'
    headers = {
        'Authorization': f'Bearer {access_token}',
        'Xero-Tenant-Id': tenant_id,
        'Accept': 'application/json'
    }

    invoice = main_client.get(endpoint, headers)
    if not invoice:
        logger.error(
            'Could not get invoice for "%s" due to an API error.', xero_client_id)
        return None

    return invoice


def get_invoice_attachment(xero_client_id: int, url: str, mime_type: str):
    """ Gets invoice attachment for client with id `xero_client_id` using endpoint provided by Xero API. """

    client = XeroClient.get_by_id(xero_client_id)
    if not client:
        logger.error(
            'Could not get invoice attachment for "%s" as the client was not found', xero_client_id)
        return None

    tenant_id, access_token = get_tokens(client)
    if tenant_id is None:
        logger.error(
            'Could not get invoice attachment for "%s" as the client did not have a tenant ID.', xero_client_id)
        return None
    if access_token is None:
        logger.error(
            'Could not get invoice attachment for "%s" as the client did not have an access token.', xero_client_id)
        return None

    endpoint = url[len('https://api.xero.com/'):]
    headers = {
        'Authorization': f'Bearer {access_token}',
        'Xero-Tenant-Id': tenant_id,
        'Accept': mime_type
    }

    raw_data = main_client.get(endpoint, headers, raw=True)
    if not raw_data:
        logger.error(
            'Could not get invoice attachment for "%s" due to an API error.', xero_client_id)
        return None

    return raw_data


def get_tokens(client: XeroClient):
    """ Returns `(tenant_id, access_token)` token for `client`, refreshes tokens for client if necessary. """
    if not client.access_token_expiry or datetime.now() > client.access_token_expiry:
        refresh_client_tokens(client)
    return client.tenant_id, client.get_access_token()


def register_new_client(xero_client_id, auth_code, redirect_uri, names: list):
    """ Takes the response from the Xero API authentication workflow and saves it to the relevant BAS client in the database.
        Returns `bool` representing success of function. """
    messages: list[AutomationMessage] = []

    client = XeroClient.get_by_id(xero_client_id)
    if not client:
        messages.append(AutomationMessage.error(
            'BAS client not found in database (it was somehow lost between starting and now).'))
        return messages

    # Get tokens from Xero Identity service
    data = {
        'grant_type': 'authorization_code',
        'code': auth_code,
        'redirect_uri': redirect_uri
    }
    tokens = auth_client.post('connect/token', data=data)
    if tokens is None:
        messages.append(AutomationMessage.error(
            'Xero token collection API error.'))
        return messages

    # Get tenant IDs from Xero API using access token
    access_token = tokens['access_token']
    headers = {
        'Authorization': 'Bearer ' + access_token,
        'Content-Type': 'application/json'
    }
    tenants: list[dict] = main_client.get('connections', headers)
    if tenants is None:
        messages.append(AutomationMessage.error(
            'Tenant ID collection API error.'))
        return messages

    # Find tenant ID in tenant IDs by cross referencing tenantName with all known names of client
    tenant_id = None
    for tenant in tenants:
        if not tenant.get('tenantName') or tenant['tenantName'] not in names:
            continue
        tenant_id = tenant['tenantId']
        break
    if not tenant_id:
        messages.append(AutomationMessage.error(f'{xero_client_id}\'s tenant ID could not be found, most likely '
                                                f'because its name in Xero is not present in the client list.'))
        return messages

    # Save found credentials to the database
    expiry_time = datetime.now() + timedelta(seconds=tokens['expires_in'])
    client.set_tokens(
        access_token, tokens['refresh_token'], expiry_time, tenant_id)

    return messages


@shared_task
def refresh_all_tokens():
    """ Refreshes all client refresh tokens. """
    clients = XeroClient.list_clients()
    for client in clients:
        if client.xero_refresh_token:
            refresh_client_tokens(client)
