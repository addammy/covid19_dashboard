import pandas as pd
from importlib import resources
import pycountry_convert as pc


countries_replacer = {'Mainland China':'China', 'UK':'United Kingdom',
                      'US':'United States of America'}


def get_big_numbers(cases_df):
    """
    Returns number of confirmed cases, deaths, recoveries and countries with COVID-19 per-date.
    Numbers are calculated for the whole world and for Europe separately.

    :param cases_df: DataFrame with the progress of the Covid-19 epidemy.
    :return: DataFrame with accumulated numbers: Confirmed, Countries, Deaths, Recovered for each Region and Date
    """
    df = prepare_cases(cases_df)
    df['Continent'] = df['Country'].apply(get_continent)
    df_EU = df[df.Continent == 'EU']
    big_numbers = get_region_numbers(df, 'World').append(get_region_numbers(df_EU, 'EU'), sort=True)
    return big_numbers


def get_region_numbers(df, region):
    """
    Returns number of confirmed cases, deaths, recoveries and countries with COVID-19 per-date for a given region.

    :param df: DataFrame with the progress of the Covid-19 epidemy for the given region.
    :param region: String with name of the region
    :return: DataFrame with accumulated numbers: Confirmed, Countries, Deaths, Recovered for each Date for chosen region
    """
    region_numbers = df.groupby('Date').sum().reset_index()
    region_numbers['Region'] = region
    region_numbers['Countries'] = df.groupby('Date').count().reset_index()['Country']
    return region_numbers


def get_continent(country):
    """
    Returns continent for a given country.

    :param country: String with full name of the country.
    :return: String with two-letter continent code.
    """
    try:
        if country in ['Others','Saint Barthelemy']:
            return 'Other'
        elif country in ['Vatican City']:
            return 'EU'
        else:
            country_code = pc.country_name_to_country_alpha2(country)
            continent_name = pc.country_alpha2_to_continent_code(country_code)
            return continent_name
    except TypeError as e:
        print(e)
        print(country)
        return

def prepare_cases(df):
    """
    Cleans and formats DataFrame.

    :param df: DataFrame with row data.
    :return: DataFrame with corrected types and new columns.
    """

    cases_df = df[df.Epidemy=='Corona'].copy()
    cases_df['Confirmed'] = cases_df.Confirmed.astype(int)
    cases_df['Deaths'] = cases_df['Deaths'].astype(int)
    cases_df['Recovered'] = cases_df['Recovered'].astype(int)
    cases_df = cases_df[['Country/Region', 'Confirmed', 'Deaths', 'Recovered',
                         'Date']].groupby(['Country/Region',
                                           'Date']).sum().reset_index()
    cases_df['Country'] = cases_df['Country/Region'].replace(countries_replacer)
    cases_df = cases_df.drop('Country/Region', axis=1)
    return cases_df


