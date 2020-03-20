from functools import reduce, partial

import pandas as pd

# How many columns in the time series data, before the time series
# columns begin.
from corona.countries import add_ISO3_from_name

_TIMESERIES_FIXED_COLS = 4

_URL_PREFIX = 'https://raw.githubusercontent.com/CSSEGISandData/COVID-19/' \
              'master/csse_covid_19_data/csse_covid_19_time_series/'
_SERIES = {
    'Confirmed':
        _URL_PREFIX + 'time_series_19-covid-Confirmed.csv',
    'Deaths':
        _URL_PREFIX + 'time_series_19-covid-Deaths.csv',
    'Recovered':
        _URL_PREFIX + 'time_series_19-covid-Recovered.csv'
}


def _get_category_df(value_name, url):
    df = pd.read_csv(url)
    dates = pd.to_datetime(df.columns[_TIMESERIES_FIXED_COLS:],
                           format='%m/%d/%y')
    dates = pd.Series(dates).dt.normalize().drop_duplicates(keep='last')
    df2 = pd.melt(df, id_vars=df.columns[:_TIMESERIES_FIXED_COLS],
                  value_vars=df.columns[_TIMESERIES_FIXED_COLS + dates.index],
                  var_name='Date', value_name=value_name)

    df2.dropna(subset=[value_name], inplace=True)
    df2 = df2[df2[value_name] > 0]
    df2[value_name] = df2[value_name].astype('Int64')

    df2['Date'] = pd.to_datetime(df2['Date'], format='%m/%d/%y') \
        .dt.strftime('%Y-%m-%d')

    return df2


def get_cases_as_df():
    """
    Retrieves the Confirmed, Deaths and Recovered time series from the csv
    files provided by JHU CSSE on GitHub. Joins
    the information into a single dataframe.

    :return: dataframe, each row describes the situation per
    country/province and day.
    """
    worksheets = [_get_category_df(value_name, url)
                  for (value_name, url) in _SERIES.items()]
    merge_columns = list(worksheets[0].columns[:(_TIMESERIES_FIXED_COLS + 1)])
    df = reduce(partial(pd.merge, how='outer', on=merge_columns), worksheets)
    for value_name in _SERIES:
        df[value_name].fillna(0, inplace=True)
    df['Epidemy'] = 'Corona'
    add_ISO3_from_name(df, 'Country/Region', 'Other')
    return df
