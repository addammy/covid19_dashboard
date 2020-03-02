import os
import sys
from configparser import ConfigParser
from pathlib import Path

from corona.epirisk import query_epirisk, risk_cases_alteration
from corona.hopkins import get_cases_as_df
from corona.spreadsheets import SpreadsheetsHandler

config = ConfigParser()
if len(sys.argv) == 2:
    settings_ini = Path(sys.argv[1])
else:

    parent_dir = Path(__file__).resolve().parent
    settings_ini = parent_dir / 'settings.ini'

print(f"Using settings {settings_ini}")
if settings_ini.exists():
    config.read('settings.ini')
else:
    print("""
    Couldn't find settings file.
    
    Usage:
    update.py [SETTINGS]
    
    Updates the Coronavirus dashboard data using the SETTINGS file. If SETTINGS not given, tries
    to load settings.ini in current directory.
    
    If path to credentials file is not given or empty in the SETTINGS file, tries to read the path from an
    environment variable (CORONA_READER_CREDENTIALS).
    
    """)
    sys.exit(1)

credentials_file = config['CREDENTIALS'].get('CORONA_READER_CREDENTIALS') or os.getenv('CORONA_READER_CREDENTIALS')
sheets = SpreadsheetsHandler(credentials_file, api_write=True)
sheet_ids = config['SPREADSHEETS']

# Acquire current data
cases_df = get_cases_as_df()

connections_df, distribution_df, exported_df, risk_cases_df = query_epirisk(cases_df)
sheets.save_df_to_spreadsheet(risk_cases_df, sheet_ids['EXPORT_RISK_CASES'])

altered_risk_cases_df = risk_cases_alteration(risk_cases_df)
sheets.save_df_to_spreadsheet(altered_risk_cases_df, sheet_ids['EXPORT_RISK_CASES'], 1)
