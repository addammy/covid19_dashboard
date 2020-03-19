import os
import sys
from configparser import ConfigParser
from pathlib import Path

parent_dir = Path(__file__).resolve().parent
src_dir = parent_dir / '../src'
sys.path.insert(0, str(src_dir))

from corona.comparisons import epidemic_summaries, sars_progress
from corona.statistics import get_big_numbers
from corona.epirisk import query_epirisk
from corona.hopkins import get_cases_as_df
from corona.spreadsheets import SpreadsheetsHandler, sheet_to_df

config = ConfigParser()
if len(sys.argv) == 2:
    settings_ini = Path(sys.argv[1])
else:
    settings_ini = parent_dir / 'settings.ini'

print(f"Using settings {settings_ini}")
if settings_ini.exists():
    config.read('settings.ini')
else:
    print("""
    Couldn't find settings file.
    
    Usage:
    update.py [SETTINGS]
    
    Updates the Coronavirus dashboard data using the SETTINGS file. 
    If SETTINGS not given, tries to load settings.ini in current directory.
    
    If path to credentials file is not given or empty in the SETTINGS file, 
    tries to read the path from an environment variable 
    (CORONA_READER_CREDENTIALS).
    
    """)
    sys.exit(1)

credentials_file = config['CREDENTIALS'].get(
    'CORONA_READER_CREDENTIALS') or os.getenv('CORONA_READER_CREDENTIALS')
sheets = SpreadsheetsHandler(credentials_file, api_write=True)
sheet_ids = config['SPREADSHEETS']

# Acquire current data
cases_df = get_cases_as_df()

# Create data for particular Tableau worksheets:

# - to compare epidemic progress with SARS:
final_df, sars_df = sars_progress(cases_df)
# - to compare parameters of various epidemics:
epidemic_days = epidemic_summaries(cases_df,sheets.get_spreadsheet(
    sheet_ids['EXPORT_EPIDEMIC_DAYS']))
# - to predict how the current epidemic might keep spreading:
connections_df, distribution_df, exported_df, risk_cases_df = \
    query_epirisk(cases_df)
# - to get current statistics
big_numbers_df = get_big_numbers(cases_df)

# Save data in Google Sheets for Tableau to use:
sheets.save_df_to_spreadsheet(cases_df, sheet_ids['EXPORT_FOR_TABLEAU'])
sheets.save_df_to_spreadsheet(epidemic_days, sheet_ids['EXPORT_EPIDEMIC_DAYS'])
sheets.save_df_to_spreadsheet(final_df, sheet_ids[
    'EXPORT_FOR_TABLEAU_WITH_SARS'])
sheets.save_df_to_spreadsheet(connections_df, sheet_ids['EXPORT_CONNECTIONS'])
sheets.save_df_to_spreadsheet(distribution_df, sheet_ids['EXPORT_RISKS'])
sheets.save_df_to_spreadsheet(risk_cases_df, sheet_ids['EXPORT_RISK_CASES'])
sheets.save_df_to_spreadsheet(big_numbers_df, sheet_ids['EXPORT_BIG_NUMBERS'])
