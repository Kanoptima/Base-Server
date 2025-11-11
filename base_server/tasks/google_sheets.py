""" Module to provide access to Google Sheets. """

from builtins import TimeoutError
import logging
import os

from dataclasses import dataclass
from dotenv import load_dotenv
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

from base_server.helpers.dates import current_iso_timestamp
from base_server.helpers.files import log_json
from base_server.tasks.google_drive import get_google_suite_credentials

load_dotenv(override=True)

logger = logging.getLogger(__name__)

RETRIES = 5


def get(service, **kwargs):
    """ Makes a get request using `service`. """
    for i in range(RETRIES):
        try:
            data = service.get(**kwargs).execute()
            return data
        except (HttpError, TimeoutError) as e:
            if i < RETRIES - 1 and (isinstance(e, TimeoutError) or e.status_code in [500, 503]):
                continue
            logger.error('Google sheets get request API error: %s', e)
            break

    return None


def batch_update(service, **kwargs):
    """ Makes a batch update request using `service`. """
    for i in range(RETRIES):
        try:
            data = service.batchUpdate(**kwargs).execute()
            return data
        except (HttpError, TimeoutError) as e:
            if i < RETRIES - 1 and (isinstance(e, TimeoutError) or e.status_code in [500, 503]):
                continue
            logger.error('Google sheets batch update request API error: %s', e)
            break

    return None


def copy_to(service, **kwargs):
    """ Makes a batch update request using `service`. """
    for i in range(RETRIES):
        try:
            data = service.sheets().copyTo(**kwargs).execute()
            return data
        except (HttpError, TimeoutError) as e:
            if i < RETRIES - 1 and (isinstance(e, TimeoutError) or e.status_code in [500, 503]):
                continue
            logger.error('Google sheets copy to request API error: %s', e)
            break

    return None


@dataclass
class CellRange:
    """ Class for representing a range of cells in a Google Sheet tab. """
    start_row: int
    start_column: int
    depth: int = 1
    width: int = 1

    @property
    def end_row(self):
        """ Index of end row, the cell in this row is not affected by changes using this `CellRange`. """
        return self.start_row + self.depth

    @property
    def end_column(self):
        """ Index of end column, the cell in this row is not affected by changes using this `CellRange`. """
        return self.start_column + self.width

    def __str__(self):
        """Return a string representation of the range in A1 notation."""
        return f'{self._col_to_letter(self.start_column)}{self.start_row}:' \
            f'{self._col_to_letter(self.end_column)}{self.end_row}'

    def _col_to_letter(self, col_num):
        """ Convert a column number to its corresponding letter(s) in A1 notation. """
        result = ''
        while col_num > 0:
            col_num, remainder = divmod(col_num - 1, 26)
            result = chr(65 + remainder) + result
        return result


class Spreadsheet:
    """ A class for storing information about google sheets files that also provides methods for modifying the broader sheets file. """

    def __init__(self, preload):
        self.spreadsheet_id = preload['spreadsheetId']
        self.json = preload
        self.requests = []
        self.title_key = {}
        for sheet in preload['sheets']:
            self.title_key[sheet['properties']['sheetId']
                           ] = sheet['properties']['title']

    def __repr__(self) -> str:
        return f'<Google Spreadsheet {self.spreadsheet_id}>'

    @property
    def id_key(self) -> dict:
        """ `dict` containing the sheet titles as the keys and the sheet Ids as the values. """
        return {value: key for key, value in self.title_key.items()}

    @classmethod
    def get_by_id(cls, spreadsheet_id: str):
        """ Returns Spreadsheet object with data for google sheet file with id `spreadsheet_id`. Logs and returns `None` if API error. """
        service = start_service()
        data = get(service, spreadsheetId=spreadsheet_id)
        if not data:
            logger.error(
                'Could not get spreadsheet "%s" due to API error', spreadsheet_id)
            return None

        return Spreadsheet(data)

    def reload(self, preload: dict):
        """ Allows Spreadsheet object to replace its values with those in the preload """

        self.spreadsheet_id = preload['spreadsheetId']
        self.json = preload
        self.requests = []
        self.title_key = {}
        for sheet in preload['sheets']:
            self.title_key[sheet['properties']['sheetId']
                           ] = sheet['properties']['title']

    def commit_changes(self, update=False):
        """ Commit all queued changes, self updates in memory if `update` is `True`. Must self update in order to continue using object. """

        # Don't attempt to commit nothing, return early
        if len(self.requests) == 0:
            return True

        # Overall structure of batchUpdate request, include Sheet data in response only for update == True
        request_object = {
            'requests': self.requests,
            'includeSpreadsheetInResponse': update
        }

        # Execute batchUpdate with Sheets Service
        service = start_service()
        preload = batch_update(
            service, spreadsheetId=self.spreadsheet_id, body=request_object)
        if not preload:
            logger.error(
                'Could not commit changes to spreadsheet "%s" due to API error.', self.spreadsheet_id)
            return False

        # Update the Spreadsheet object if required.
        if update:
            self.reload(preload=preload['updatedSpreadsheet'])

        return True

    def add_sheet(self, sheet_name: str, hidden: bool = False, index: int = -1):
        """ Create a new tab in the spreadsheet with name `sheet_name`. Index will be
            set if provided and will be made hidden depending on `hidden` """
        request = {'addSheet': {'properties': {'title': sheet_name,
                                               'hidden': hidden}}}
        if index >= 0:
            request['addSheet']['properties']['index'] = index
        self.requests.append(request)

    def delete_sheet(self, sheet_id):
        """ Delete a tab within this Spreadsheet with id `sheet_id` """

        request = {'deleteSheet': {'sheetId': sheet_id}}
        self.requests.append(request)

    def hidden_request(self, sheet_id: int, hidden: bool):
        """ Set a sheet with id `sheet_id` to be hidden depending on `hidden` """

        request = {'updateSheetProperties': {'properties': {'sheetId': sheet_id,
                                                            'hidden': hidden},
                                             'fields': 'hidden'}}
        self.requests.append(request)

    def copy_sheet(self, sheet_name, new_name, destination_id=None, index=0, hidden=None, get_data=True):
        """ Copies a tab with name `sheet_name` from this spreadsheet to either itself or the spreadsheet with id `destination_id`. Sets index of
            new sheet to `index` and will either be hidden dependingon the original tab, or will use the `hidden` value if it is provided """

        service = start_service()

        if destination_id is None:
            destination_id = self.spreadsheet_id

        if sheet_name not in self.id_key.keys():
            logger.error(
                'Cannot copy sheet "%s" as it was not found in "%s"', sheet_name, self)
            return None

        body = {'destinationSpreadsheetId': destination_id}
        new_sheet_dict = copy_to(
            service, spreadsheetId=self.spreadsheet_id, sheetId=self.id_key[sheet_name], body=body)
        if not new_sheet_dict:
            logger.error('Copy sheet in spreadsheet "%s" failed.',
                         self.spreadsheet_id)
            return None

        # New sheet by default has no metadata set and no grid data retrieved, new request to rename and move new sheet.
        request = {
            'updateSheetProperties': {
                'properties': {
                    'sheetId': new_sheet_dict['sheetId'],
                    'title': new_name,
                    'index': index
                },
                'fields': 'title, index'
            }
        }

        # If hidden status is specified, include this in the request
        if hidden is not None:
            request['updateSheetProperties']['properties']['hidden'] = hidden
            request['updateSheetProperties']['fields'] += ', hidden'

        # Required format of batchUpdate body
        body = {
            'requests': [request],
            'includeSpreadsheetInResponse': True,
            'responseRanges': [new_name],
            'responseIncludeGridData': get_data
        }

        preload = batch_update(
            service, spreadsheetId=destination_id, body=body)
        if not preload:
            logger.error(
                'Rename copied sheet in spreadsheet "%s" failed.', self.spreadsheet_id)
            return None

        # Store new sheet in title key, auto updates id key
        new_sheet = Sheet(
            preload=preload['updatedSpreadsheet'], get_data=get_data)
        if destination_id == self.spreadsheet_id:
            self.title_key[new_sheet.sheet_id] = new_sheet.sheet_name

        return new_sheet

    def rename_sheet(self, sheet_id, new_name):
        """ Rename a sheet with id `sheet_id` to `new_name` """

        request = {
            'updateSheetProperties': {
                'properties': {
                    'sheetId': sheet_id,
                    'title': new_name},
                'fields': 'title'
            }
        }
        self.requests.append(request)

    def move_sheet(self, sheet_id: int, new_index: int):
        """Move a tab within this Spreadsheet to a new position in the tab order.

        Args:
            sheet_id (int): The ID of the sheet to move.
            new_index (int): The new index (0-based) where the sheet should appear.
                            0 = leftmost, increasing numbers move right.
        """
        request = {
            'updateSheetProperties': {
                'properties': {
                    'sheetId': sheet_id,
                    'index': new_index
                },
                'fields': 'index'
            }
        }
        self.requests.append(request)


class Sheet:
    # pylint: disable-msg=too-many-public-methods
    """ Class that stores data and metadata of a Google Sheet, and provides various
        methods for modifying the sheets, that can be executed using `commit_changes()` """

    def __init__(self, preload: dict, get_data=True):
        self.spreadsheet_id: str = preload['spreadsheetId']
        self.json: dict = preload
        self.sheet_id: int = self.json['sheets'][0]['properties']['sheetId']
        self.sheet_name: str = self.json['sheets'][0]['properties']['title']
        self.requests = []
        self.width = 0
        self.cell_array = []
        if get_data:
            if 'rowData' not in self.json['sheets'][0]['data'][0].keys():
                return
            rows: list[dict] = self.json['sheets'][0]['data'][0]['rowData']
            for row_object in rows:
                new_row = []
                if 'values' not in row_object.keys():
                    if len(self.cell_array) > 0:
                        for _ in range(len(self.cell_array[0])):
                            new_row.append({})
                    else:
                        new_row.append({})
                else:
                    self.width = max(self.width, len(row_object['values']))
                    for entry in row_object['values']:
                        new_row.append(entry)
                self.cell_array.append(new_row)

    def __repr__(self) -> str:
        return f'<Google Sheet {self.sheet_name} in Spreadsheet {self.spreadsheet_id}>'

    @classmethod
    def get_sheet(cls, spreadsheet_id, ranges, get_data=True):
        """ Provides `Sheet` object for name `ranges`, which will contain cell data if `get_data == True` """

        service = start_service()
        data = get(service, spreadsheetId=spreadsheet_id,
                   ranges=ranges, includeGridData=get_data)
        if not data:
            logger.error(
                'Could not get sheet "%s" from spreadsheet "%s" due to API error', ranges, spreadsheet_id)
            return None

        return cls(data, get_data=get_data)

    def reload(self, preload: dict):
        """ Allows Sheet object to replace its values with those in the preload """
        self.spreadsheet_id = preload['spreadsheetId']
        self.json = preload
        self.sheet_id = self.json['sheets'][0]['properties']['sheetId']
        self.sheet_name = self.json['sheets'][0]['properties']['title']
        self.requests = []
        self.cell_array = []
        if 'rowData' not in self.json['sheets'][0]['data'][0].keys():
            return
        row_object: dict
        for row_object in self.json['sheets'][0]['data'][0]['rowData']:
            new_row = []
            if 'values' not in row_object.keys():
                if len(self.cell_array) > 0:
                    for _ in range(len(self.cell_array[0])):
                        new_row.append({})
                else:
                    new_row.append({})
            else:
                for entry in row_object['values']:
                    new_row.append(entry)
            self.cell_array.append(new_row)

    def value_to_object(self, value):
        """ Creates a dict in the Google API expected format for whatever value is provided. """

        if isinstance(value, bool):
            return {'boolValue': value}
        if isinstance(value, (float, int)):
            return {'numberValue': value}
        if isinstance(value, str) and len(value):
            if len(value) > 0:
                if value[0] == '=':
                    return {'formulaValue': value}
            return {'stringValue': value}
        return None

    def get_value(self, row: int, column: int):
        """ Return the raw value for the cell at (`row`, `column`) in this tab. Returns `None` for any out of bounds or empty cells. """

        # If for some reason a value cannot be found, return None
        try:
            cell_json: dict = self.cell_array[row][column]
            value: dict = cell_json.get(
                'userEnteredValue', cell_json.get('effectiveValue'))
            if value is None:
                return None
        except IndexError:
            return None

        # In order of type preference, if the value is represented as that type, return that value.
        preferred_type_order = ['numberValue',
                                'stringValue', 'boolValue', 'formulaValue']
        for value_type in preferred_type_order:
            if value_type in value.keys():
                return value[value_type]

        # If nothing is found for some reason, return None
        return None

    def get_formatted_value(self, row: int, column: int):
        """ Return the formatted value for the cell at (`row`, `column`) in this tab. If formattedValue is not present for this cell,
            then attempt to return value using `get_value()`. For any errors getting the value, returns `None`. """

        try:
            if 'formattedValue' not in self.cell_array[row][column]:
                return self.get_value(row, column)
        except IndexError:
            return None
        return self.cell_array[row][column]['formattedValue']

    def commit_changes(self, update=False):
        """ Applies all queued changes to this Sheet in one API call. """

        # If there aren't any staged requests, don't attempt to call API, return early
        if len(self.requests) == 0:
            return True

        # Overall structure of batchUpdate request, only get updated sheet data for update == True
        request_object = {
            'requests': self.requests,
            'includeSpreadsheetInResponse': update,
            'responseRanges': [self.sheet_name],
            'responseIncludeGridData': update
        }

        # Use service to execute batchUpdate
        service = start_service()
        preload = batch_update(
            service, spreadsheetId=self.spreadsheet_id, body=request_object)
        if not preload:
            logger.error(
                'Could not commit changes to "%s" due to API error.', self.sheet_name)
            log_json(
                f'failed_sheets_calls/{current_iso_timestamp()}.json', self.requests)
            return False

        if update:
            self.reload(preload=preload['updatedSpreadsheet'])
        return True

    def insert_rows(self, start_index: int, number: int):
        """ Insert `number` rows at the index `start_index` """

        if number < 1:
            return
        row_request = {
            'range': {
                'sheetId': self.sheet_id,
                'dimension': 'ROWS',
                'startIndex': start_index,
                'endIndex': start_index + number
            },
            'inheritFromBefore': start_index != 0  # Yes unless this is the first row
        }
        self.requests.append({'insertDimension': row_request})

    def delete_rows(self, start_index: int, number: int):
        """ Delete `number` rows at the index `start_index` """

        if number < 1:
            return
        row_request = {'range': {'sheetId': self.sheet_id,
                                 'dimension': 'ROWS',
                                 'startIndex': start_index,
                                 'endIndex': start_index + number}}
        self.requests.append({'deleteDimension': row_request})

    def delete_columns(self, start_index: int, number: int):
        """ Delete `number` columns at the index `start_index` """

        if number < 1:
            return
        column_request = {
            'range': {
                'sheetId': self.sheet_id,
                'dimension': 'COLUMNS',
                'startIndex': start_index,
                'endIndex': start_index + number
            }
        }
        self.requests.append({'deleteDimension': column_request})

    def insert_column(self, start_index: int, number: int):
        """ Insert `number` columns at the index `start_index` """

        if number < 1:
            return
        column_request = {
            'range': {
                'sheetId': self.sheet_id,
                'dimension': 'COLUMNS',
                'startIndex': start_index,
                'endIndex': start_index + number
            },
            'inheritFromBefore': True
        }
        self.requests.append({'insertDimension': column_request})

    def set_row_height(self, start_row: int, height: int, number: int = 1):
        """ Sets row height of `number` rows starting at `start_row` to `width` """
        properties = {'pixelSize': height}
        dimension_range = {
            'sheetId': self.sheet_id,
            'dimension': 'ROWS',
            'startIndex': start_row,
            'endIndex': start_row + number
        }
        self._dimension_properties_request(
            properties, 'pixelSize', dimension_range)

    def set_column_width(self, start_column: int, width: int, number: int = 1):
        """ Sets column width of `number` columns starting at `start_column` to `width` """
        properties = {'pixelSize': width}
        dimension_range = {
            'sheetId': self.sheet_id,
            'dimension': 'COLUMNS',
            'startIndex': start_column,
            'endIndex': start_column + number
        }
        self._dimension_properties_request(
            properties, 'pixelSize', dimension_range)

    def set_text_format(self, cell_range: CellRange, **kwargs):
        """ Sets text formatting on `cell_range`. Available kwargs are fontSize, bold, italic, strikethrough, underline, colour.
            `colour` must be dict in format `{'red': 0-1, 'green': 0-1, 'blue': 0-1}. If no kwargs are provided, does nothing. """

        if len(kwargs) == 0:
            return

        colour = kwargs.pop('colour', None)
        fields = set(kwargs.keys())
        if colour:
            fields.add('foregroundColorStyle')
        field_mask = 'userEnteredFormat.textFormat(' + ','.join(fields) + ')'
        row_objects = []
        for _ in range(cell_range.depth):
            row_data = {'values': []}
            for _ in range(cell_range.width):
                value = {'userEnteredFormat': {'textFormat': kwargs}}
                if colour:
                    value['userEnteredFormat']['textFormat']['foregroundColorStyle'] = {
                        'rgbColor': colour}
                row_data['values'].append(value)
            row_objects.append(row_data)

        self._cells_request(cell_range.start_row,
                            cell_range.start_column, row_objects, field_mask)

    def set_number_format(self, cell_range: CellRange, number_type: str, pattern: str = None):
        """ Sets number formatting on `cell_range`. Available values of `number_type` are.
            TEXT        Text formatting, e.g 1000.12
            NUMBER      Number formatting, e.g, 1,000.12
            PERCENT     Percent formatting, e.g 10.12%
            CURRENCY    Currency formatting, e.g $1,000.12
            DATE        Date formatting, e.g 9/26/2008
            TIME        Time formatting, e.g 3:59:00 PM
            DATE_TIME   Date+Time formatting, e.g 9/26/08 15:59:00
            SCIENTIFIC  Scientific number formatting, e.g 1.01E+03

            `pattern` is a string that can be used to specify a custom number format as documented at
            https://developers.google.com/sheets/api/guides/formats. """

        field_mask = 'userEnteredFormat.numberFormat'
        row_objects = []
        for _ in range(cell_range.depth):
            row_data = {'values': []}
            for _ in range(cell_range.width):
                row_data['values'].append({'userEnteredFormat': {'numberFormat': {
                                          'type': number_type, 'pattern': pattern}}})
            row_objects.append(row_data)

        self._cells_request(cell_range.start_row,
                            cell_range.start_column, row_objects, field_mask)

    def set_horizontal_alignment(self, cell_range: CellRange, alignment: str):
        """ Sets horizontal alignment of `cell_range` to `alignment`. `alignment` can be 'LEFT', 'CENTER', 'RIGHT' """

        field_mask = 'userEnteredFormat.horizontalAlignment'
        row_objects = []
        for _ in range(cell_range.depth):
            row_data = {'values': []}
            for _ in range(cell_range.width):
                row_data['values'].append(
                    {'userEnteredFormat': {'horizontalAlignment': alignment}})
            row_objects.append(row_data)

        self._cells_request(cell_range.start_row,
                            cell_range.start_column, row_objects, field_mask)

    def set_vertical_alignment(self, cell_range: CellRange, alignment: str):
        """ Sets vertical alignment of `cell_range` to `alignment`. `alignment` can be 'TOP', 'MIDDLE', 'BOTTOM' """

        field_mask = 'userEnteredFormat.verticalAlignment'
        row_objects = []
        for _ in range(cell_range.depth):
            row_data = {'values': []}
            for _ in range(cell_range.width):
                row_data['values'].append(
                    {'userEnteredFormat': {'verticalAlignment': alignment}})
            row_objects.append(row_data)

        self._cells_request(cell_range.start_row,
                            cell_range.start_column, row_objects, field_mask)

    def merge_cells(self, cell_range: CellRange):
        """ Merges all cells in `cell_range` into a single cell. """

        merge_request = {
            'range': {
                'sheetId': self.sheet_id,
                'startRowIndex': cell_range.start_row,
                'endRowIndex': cell_range.start_row + cell_range.depth,
                'startColumnIndex': cell_range.start_column,
                'endColumnIndex': cell_range.start_column + cell_range.width
            },
            'mergeType': 'MERGE_ALL'
        }

        self.requests.append({'mergeCells': merge_request})

    def _cells_request(self, row: int, column: int, row_objects: list[dict[str, list[dict]]], fields: str):
        """ Stages a general cells request for the next commit. Not for external use, use a more specific cells request function. """

        cell_request = {
            'rows': row_objects,
            'fields': fields,
            'start': {
                'sheetId': self.sheet_id,
                'rowIndex': row,
                'columnIndex': column
            }
        }
        self.requests.append({'updateCells': cell_request})

    def _dimension_properties_request(self, properties: dict, fields: str, dimension_range: dict):
        """ Stages a `sheet` properties request for update over the range `dimension_range`.
            Not for external use, use an outward facing function for specific properties requests """

        dimension_request = {'properties': properties,
                             'fields': fields,
                             'range': dimension_range}
        self.requests.append({'updateDimensionProperties': dimension_request})

    def add_hide_box(self, row: int, column: int, formula: str):
        """ Applies conditional formatting to hide cells in `cell_range` given the provided Sheets formula. """

        grid_range = {
            'sheetId': self.sheet_id,
            'startRowIndex': row,
            'endRowIndex': row + 1,
            'startColumnIndex': column,
            'endColumnIndex': column+1
        }

        format_json = {
            'textFormat': {
                'foregroundColorStyle': {
                    'rgbColor': {
                        'red': 1,
                        'green': 1,
                        'blue': 1
                    }
                }
            },
            'backgroundColorStyle': {
                'rgbColor': {
                    'red': 1,
                    'green': 1,
                    'blue': 1
                }
            }
        }

        bool_rule = {
            'condition': {
                'type': 'CUSTOM_FORMULA',
                'values': [{'userEnteredValue': formula}]
            },
            'format': format_json
        }
        rule = {
            'ranges': [grid_range],
            'booleanRule': bool_rule
        }
        conditional_request = {
            'rule': rule,
            'index': 0
        }
        self.requests.append({'addConditionalFormatRule': conditional_request})

    def set_background_colour(self, cell_range: CellRange, colour: dict):
        """ Changes cell at (`row`, `column`) to have background colour, `colour`.
            `colour` must be dict in format `{'red': 0-1, 'green': 0-1, 'blue': 0-1} """
        field_mask = 'userEnteredFormat.backgroundColor'
        row_objects = []
        for _ in range(cell_range.depth):
            row_data = {'values': []}
            for _ in range(cell_range.width):
                row_data['values'].append(
                    {'userEnteredFormat': {'backgroundColor': colour}})
            row_objects.append(row_data)
        self._cells_request(cell_range.start_row,
                            cell_range.start_column, row_objects, field_mask)

    def set_borders(self, cell_range: CellRange, sides: list[str], style: str, colour: dict = None):
        """ Sets borders along `sides` of `cell_range` to `style`. Black only (this could be included later if requried). `sides` may contain
            'top', 'bottom', 'left', 'right', 'innerHorizontal', 'innerVertical' Style options are,
            'DOTTED'          The border is dotted.
            'DASHED'          The border is dashed.
            'SOLID'           The border is a thin solid line.
            'SOLID_MEDIUM'    The border is a medium solid line.
            'SOLID_THICK'     The border is a thick solid line.
            'NONE' 	        No border. Used only when updating a border in order to erase it.
            'DOUBLE'          The border is two solid lines.
            `colour` must be dict in format `{'red': 0-1, 'green': 0-1, 'blue': 0-1}.  """

        if colour is None:
            colour = {'red': 0, 'green': 0, 'blue': 0}

        update_borders_request = {
            'range': {
                'sheetId': self.sheet_id,
                'startRowIndex': cell_range.start_row,
                'endRowIndex': cell_range.end_row,
                'startColumnIndex': cell_range.start_column,
                'endColumnIndex': cell_range.end_column
            }
        }

        for side in sides:
            update_borders_request[side] = {'style': style, 'color': colour}

        self.requests.append({'updateBorders': update_borders_request})

    def set_wrap_strategy(self, cell_range: CellRange, wrap_strategy: str):
        """ Change the wrap strategy of cells within `cell_range` to be `wrap_strategy`,
            which can be either `'OVERFLOW_CELL'`, `'LEGACY_WRAP'`, `'CLIP'` or `'WRAP'` """

        row_objects = []
        field_mask = 'userEnteredFormat.wrapStrategy'

        for _ in range(cell_range.depth):
            row_data = {'values': []}
            for _ in range(cell_range.width):
                row_data['values'].append(
                    {'userEnteredFormat': {'wrapStrategy': wrap_strategy}})
            row_objects.append(row_data)

        self._cells_request(cell_range.start_row,
                            cell_range.start_column, row_objects, field_mask)

    def set_value(self, row, column, value):
        """ Set value of cell at (`row`, `column`) to `value`. Automatically determines type of value and correctly formats for API. """
        field_mask = 'userEnteredValue'

        value_object = self.value_to_object(value)
        row_objects = [{'values': [{'userEnteredValue': value_object}]}]

        self._cells_request(row, column, row_objects, field_mask)

    def set_values(self, cell_range: CellRange, value):
        """ Set values of cells within `cell_range` to `value`. Automatically determines type of value and correctly formats for API. """
        row_objects = []
        field_mask = 'userEnteredValue'

        value_object = self.value_to_object(value)
        for _ in range(cell_range.depth):
            row_data = {'values': []}
            for _ in range(cell_range.width):
                row_data['values'].append({'userEnteredValue': value_object})
            row_objects.append(row_data)

        self._cells_request(cell_range.start_row,
                            cell_range.start_column, row_objects, field_mask)

    def mass_set_value(self, start_row: int, start_column: int, values: list[list]):
        """ Set values of cells down and to the right of (`start_row`, `start_column`) to the array of `values`.
            Automatically determines type of value and correctly formats for API. """
        row_objects = []
        field_mask = 'userEnteredValue'

        for row in values:
            row_data = {'values': []}
            for value in row:
                value_object = self.value_to_object(value)
                row_data['values'].append({'userEnteredValue': value_object})
            row_objects.append(row_data)

        self._cells_request(start_row, start_column, row_objects, field_mask)

    def set_link_sets(self, start_row, start_column, link_set_array: list[list[dict]]):
        """
        Updates a range of cells in a Google Sheets spreadsheet with formatted hyperlinks.

        Parameters:
            start_row (int): The starting row index (zero-based) where the data should be written.
            start_column (int): The starting column index (zero-based) where the data should be written.
            link_set_array (list[list[dict[str]]]): A 2D list representing rows and columns, where each element is a dictionary
                containing the following keys:
                    - 'stringValue' (str): The full text content of the cell.
                    - 'links' (list[dict[str, str]]): A list of dictionaries defining hyperlinks, where each dictionary has:
                        - 'value' (str): The exact substring within 'stringValue' that should be hyperlinked.
                        - 'uri' (str): The hyperlink URL.

        Example:
            link_set_array = [
                [
                    {
                        "stringValue": "sample link1 sample link2 sample",
                        "links": [
                            {"value": "link1", "uri": "https://www.google.com/"},
                            {"value": "link2", "uri": "https://tanaikech.github.io/"},
                        ],
                    },
                    {
                        "stringValue": "link1",
                        "links": [{"value": "link1", "uri": "https://www.google.com/"}],
                    },
                ]
            ]
        """
        def create_text_format_runs(string_value, links):
            text_format_runs = []
            for link in links:
                temp = string_value.find(link['value'])
                if temp != -1:
                    text_format_runs.append({'startIndex': temp, 'format': {
                                            'link': {'uri': link['uri']}}})
                    if len(string_value) != temp + len(link['value']):
                        text_format_runs.append(
                            {'startIndex': temp + len(link['value']), 'format': {}})
            return text_format_runs

        rows = [
            [
                {'values': [
                    {
                        'userEnteredValue': {'stringValue': cell['stringValue']},
                        'textFormatRuns': create_text_format_runs(cell['stringValue'], cell['links']),
                    }
                    for cell in row
                ]}
                for row in link_set_array
            ]
        ]

        self._cells_request(start_row, start_column, rows,
                            'userEnteredValue,textFormatRuns')

    def set_data_validation(self, cell_range: CellRange, data_validation: dict | None):
        """ Set data validation of cells in `cell_range` to `data_validation`. `data_validation` must either be `None` or `dict` compliant with
            https://developers.google.com/sheets/api/reference/rest/v4/spreadsheets/cells#datavalidationrule """
        row_objects = []
        field_mask = 'dataValidation'

        for _ in range(cell_range.depth):
            row_data = {'values': []}
            for _ in range(cell_range.width):
                row_data['values'].append({'dataValidation': data_validation})
            row_objects.append(row_data)

        self._cells_request(cell_range.start_row,
                            cell_range.start_column, row_objects, field_mask)

    def replace_cells(self, cell_range: CellRange, cell_definitions: list[list[dict]]):
        """ Completely replace the cells in `cell_range` with the json representation of a cell specified in `cell_def`. """
        row_objects = []
        field_mask = '*'

        for i in range(cell_range.depth):
            row_data = {'values': cell_definitions[i]}
            row_objects.append(row_data)

        self._cells_request(cell_range.start_row,
                            cell_range.start_column, row_objects, field_mask)

    def set_rows_hidden(self, start_row: int, number: int, hidden=True):
        """ Hide `number` rows starting at `start_row` """

        properties = {'hiddenByUser': hidden}
        dimension_range = {
            'sheetId': self.sheet_id,
            'dimension': 'ROWS',
            'startIndex': start_row,
            'endIndex': start_row + number
        }
        self._dimension_properties_request(
            properties, 'hiddenByUser', dimension_range)

    def set_columns_hidden(self, start_column: int, number: int, hidden=True):
        """ Hide `number` columns starting at `start_column` """

        properties = {'hiddenByUser': hidden}
        dimension_range = {
            'sheetId': self.sheet_id,
            'dimension': 'COLUMNS',
            'startIndex': start_column,
            'endIndex': start_column + number
        }
        self._dimension_properties_request(
            properties, 'hiddenByUser', dimension_range)


def load_client_config():
    """ Shorthand for Google Sheets client config. """
    client_config = {
        'web': {
            'client_id': os.getenv('GOOGLE_SHEETS_CLIENT_ID'),
            'client_secret': os.getenv('GOOGLE_SHEETS_CLIENT_SECRET'),
            'auth_uri': 'https://accounts.google.com/o/oauth2/auth',
            'token_uri': 'https://oauth2.googleapis.com/token',
            'redirect_uris': [os.getenv('GOOGLE_REDIRECT_URI')]
        }
    }
    return client_config


def start_service():
    # pylint: disable-msg=no-member
    """ Starts the service that provides access to Sheets API functions """
    creds = get_google_suite_credentials()

    return build('sheets', 'v4', credentials=creds, cache_discovery=False).spreadsheets()
