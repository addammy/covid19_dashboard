from corona.countries import join_countries_data


def get_big_numbers(cases_df):
    """
    Returns number of confirmed cases, deaths, recoveries and countries
    with COVID-19 per-date.
    Numbers are calculated for the whole world and for Europe separately.

    :param cases_df: DataFrame with the progress of the Covid-19 epidemic.
    :return: DataFrame with accumulated numbers: Confirmed, Countries, Deaths,
             Recovered for each Region and Date
    """
    df = prepare_cases(cases_df)
    df = join_countries_data(df, ['continent', 'name_short'])
    df_eu = df[df.continent == 'Europe']
    big_numbers = get_region_numbers(df, 'World').append(
        get_region_numbers(df_eu, 'EU'), sort=True)
    return big_numbers


def get_region_numbers(df, region):
    """
    Returns number of confirmed cases, deaths, recoveries and countries with
    COVID-19 per-date for a given region.

    :param df: DataFrame with the progress of the Covid-19 epidemic for the
               given region.
    :param region: String with name of the region
    :return: DataFrame with accumulated numbers: Confirmed, Countries, Deaths,
             Recovered for each Date for chosen region
    """
    region_numbers = df.groupby('Date').sum().reset_index()
    region_numbers['Region'] = region
    region_numbers['Countries'] = df.groupby('Date').count().reset_index()['name_short']
    return region_numbers


def prepare_cases(df):
    """
    Cleans and formats DataFrame.

    :param df: DataFrame with row data.
    :return: DataFrame with corrected types and new columns.
    """
    cases_df = df[df.Epidemy == 'Corona'].copy()
    cases_df['Confirmed'] = cases_df.Confirmed.astype(int)
    cases_df['Deaths'] = cases_df['Deaths'].astype(int)
    cases_df['Recovered'] = cases_df['Recovered'].astype(int)
    cases_df = cases_df[
        ['ISO3', 'Confirmed', 'Deaths', 'Recovered', 'Date']
    ].dropna().groupby(['ISO3', 'Date']).sum().reset_index()
    return cases_df
