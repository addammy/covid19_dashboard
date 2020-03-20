import itertools
import time
from functools import lru_cache

import gspread
import pandas as pd
from oauth2client.service_account import ServiceAccountCredentials

EXPORT_TEST = '1huoY7FSvP3MNwvvi6QtRk-Yh0WX0cebtgSd7fu-jGhY'


class SpreadsheetsHandler:
    def __init__(self, credentials_file, api_write=True):
        """
        Handler for opening and writing Google Sheets.

        :param credentials_file: Path to json file with service account
        credentials.
        :param api_write: bool, for debug purposes if False then all write
        actions are not executed.
        """
        scope = ['https://spreadsheets.google.com/feeds',
                 'https://www.googleapis.com/auth/drive',
                 'https://www.googleapis.com/auth/spreadsheets']
        credentials = ServiceAccountCredentials.from_json_keyfile_name(
            credentials_file, scope)
        self.client = gspread.authorize(credentials)
        self.api_write = api_write

    @lru_cache(maxsize=4)
    def get_spreadsheet(self, key):
        """
        Connect to Google APIs and retrieve spreadsheet with given key.
        Cached to prevent unnecessary queries.

        :param key: Google Sheet key
        :return: Spreadsheet object
        """
        return self.client.open_by_key(key)

    def save_df_to_spreadsheet(self, df: pd.DataFrame, key: str = None,
                               worksheet_no=0, *, spreadsheet=None,
                               new_worksheet=False) -> None:
        """
        Writes df to given spreadsheet. Target worksheet is cleared of all
        preexisting data.
        If both key and spreadsheet are provided, spreadsheet is used.

        :param df: DataFrame to be written
        :param key: Google Sheet key
        :param worksheet_no:
        :param spreadsheet: gspread Spreadsheet object
        :param new_worksheet: bool, if False, then df is written in the first
        worksheet. If true, then new worksheet
        with current time (ns since Epoch) is created.
        :return:
        """
        if spreadsheet is None:
            spreadsheet = self.get_spreadsheet(key)
        rows, cols = df.shape
        rows += 1  # headers
        if new_worksheet:
            worksheet = spreadsheet.add_worksheet(
                str(time.time_ns()), rows, cols)
        else:
            worksheet = spreadsheet.worksheets()[worksheet_no]
        if self.api_write:
            worksheet.clear()
        cells = worksheet.range(1, 1, rows, cols)
        types = set()
        for cell, val in zip(cells, itertools.chain(list(df.columns),
                             df.to_numpy().flatten())):
            types.add(type(val))

            if pd.isna(val):
                cell.value = ''
            elif type(val) in (int, float, str):
                cell.value = val
            else:
                cell.value = str(val)
        if self.api_write:
            worksheet.update_cells(cells)


def sheet_to_df(sheet_data):
    """Loads google sheet as pandas DataFrame."""
    data = sheet_data.get_all_values()
    df = pd.DataFrame(data)
    df.columns = df.iloc[0]
    df = df.iloc[1:]
    return df
