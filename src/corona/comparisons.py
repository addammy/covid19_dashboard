import pandas as pd
from importlib import resources


def epidemic_summaries(cases_df, epidemics_sheet):
    """
    Returns a per-date summary of confirmed cases and deaths from cases_df,
    concatenated with similar info for other epidemics. The info for other
    epidemics is read from a Google spreadsheet.

    :param cases_df: DataFrame with the progress of the Covid-19 epidemy.
    :param epidemics_sheet: gspread Spreadsheet object. The spreadsheet should
    contain a worksheet named 'base',
    containing data on other epidemics.
    :return: DataFrame with summaries for all epidemics.
    """
    df = cases_df[['Date', 'Confirmed', 'Deaths']].copy()
    df = df.groupby('Date').sum()
    df.reset_index(inplace=True)
    df['CFR'] = (df['Deaths'] / df['Confirmed']).astype(str)
    df['Epidemy'] = 'Corona Virus 2019-nCoV'
    df['Name'] = 'Corona'
    df['R0'] = '2,74'
    df['end_year'] = 'present'
    df['notes'] = ''
    df['start_year'] = 2019
    df['years'] = '2019-'
    df['R0min'] = '1,4'
    df['R0max'] = '3,9'
    others = pd.DataFrame(epidemics_sheet.worksheet('base').get_all_records())
    df = others.append(df, ignore_index=True, sort=True)
    for col in ['CFR', 'R0', 'R0min', 'R0max']:
        df[col] = df[col].str.replace(',', '.').astype(float)
    return df


def sars_progress(cases_df):
    """
    Append data on the progress of the SARS epidemic.

    :param cases_df: DataFrame with the progress of the Covid-19 epidemy.
    :return: both_df, sars_df: (DataFrame, DataFrame) tuple. both_df contains
    concatenated data from cases_df and sars_df.
    """
    sars_df = pd.read_csv(resources.open_text('corona.resources', 'SARS.csv'))
    both_df = pd.concat([cases_df, sars_df], ignore_index=True, sort=True)
    return both_df, sars_df
