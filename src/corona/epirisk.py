import json
from base64 import b64encode
from dataclasses import dataclass
from typing import List, Dict, Set

import pandas as pd
import requests

import corona.countries
from corona.countries import get_countries_df

# The Epirisk API is apparently limited to a country count or query length.
# This constant determines the number of largest confirmed cases counts which
# will be sent to Epirisk. It introduces potencial problems or inconsistencies,
# as countries with infections could be presented as only still at risk.
KEEP_TOP_CASES_COUNT = 110
_months = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep',
           'Oct', 'Nov', 'Dec']

def _get_epirisk_id_df():
    return get_countries_df(['ISO3', 'epirisk_id']) \
        .dropna() \
        .astype({'epirisk_id': int})

_epirisk_id_df = _get_epirisk_id_df()
id_from_iso3 = dict(_epirisk_id_df.itertuples(False, None))
iso3_from_id = dict(map(reversed, id_from_iso3.items()))


def _get_epirisk_id_df():
    return get_countries_df(['ISO3', 'epirisk_id']) \
        .dropna() \
        .astype({'epirisk_id': int})


@dataclass
class Distribution:
    values: Dict[int, float]
    residual: float

    def __init__(self, json):
        self.values = {int(k): float(v) for (k, v)
                       in json['distribution'].items()}
        self.residual = json['residual']


@dataclass
class ExportedCases:
    targets: Dict[int, Distribution]

    def __init__(self, json):
        self.targets = {}
        targets = json['targets']
        for target, dist_json in targets.items():
            target_id = -1 if target == 'world' else int(target)
            self.targets[target_id] = Distribution(dist_json)

    def df(self, names=False):
        data = []
        for target, distribution in self.targets.items():
            if names:
                target = ('world' if target == -1
                          else iso3_from_id[target])
            for v, p in distribution.values.items():
                data.append((target, v, p))
        return pd.DataFrame(data, columns=['where', 'value', 'probability'])


@dataclass
class ConnectionsRisk:
    connections: Dict[int, Set[int]]
    distribution: Dict[int, float]
    residual: float

    def __init__(self, json):
        self.connections = {int(k): set(v) for k, v
                            in json['connections'].items()}
        self.distribution = {int(k): v for k, v
                             in json['distribution'].items()}
        self.residual = json['residual']

    def connections_df(self):
        data = []
        for country_id, destinations in self.connections.items():
            country_name = iso3_from_id.get(country_id)
            data.extend(
                [(country_id, country_name, dest_id,
                  iso3_from_id.get(dest_id)) for dest_id in
                 destinations]
            )
        df = pd.DataFrame(data, columns=['country_id', 'ISO3',
                                         'dest_id', 'dest_ISO3']).dropna()
        return df

    def distribution_df(self):
        data = []
        for country_id, risk in self.distribution.items():
            data.append([country_id, iso3_from_id.get(country_id), risk])

        df = pd.DataFrame(data, columns=['CountryId', 'ISO3', 'Risk']).dropna()
        return df


class EpiriskQuery:
    def __init__(self, *, period=10, month=1,
                 travel_level=1.0, mute=False):
        """
        Class for building Epirisk queries.

        Add sources, countries or basins(cities) by name:

        >>> query = EpiriskQuery()
        >>> query['Poland'] = 100

        or by id:

        >>> query[169] = 100

        Id's can be translated the corona.epirisk.api module's dictionaries:

        >>> print(countries['Poland'])
        Location(id=169, label='Poland', lat=52.0,
        lng=20.0, population=37531600)
        >>> print(countries_by_id[169])
        Location(id=169, label='Poland', lat=52.0,
        lng=20.0, population=37531600)

        Init with options from Epirisk's form:
        :param geolevel: 'country' or 'basin'
        :param period: Days to onset of symptoms
        :param month: Month of travel
        :param travel_level: float, 0.0-1.0 range. 0.0 for completely
        restricted travel, 1.0 for no travel restrictions.
        :param mute: bool, if True then no exception is thrown if data for
        missing country is added
        """
        self.cases = {}
        self.period = period
        self.month = month
        self.travel_level = travel_level
        self.mute = mute

    @staticmethod
    def from_cases(cases, mute):
        """
        Creates an EpiriskQuery instance from a dataframe with country codes
        with corresponding case numbers
        :param cases: DataFrame with 'ISO3' and 'Confirmed' columns
        :param mute: if False, don't raise error for unknown code
        :return:
        """
        epirisk = EpiriskQuery(mute=mute)
        for iso3, count in cases.itertuples(False):
            epirisk[iso3] += count
        return epirisk

    def __setitem__(self, key, value):
        if isinstance(key, str):
            try:
                key = id_from_iso3[key]
            except KeyError:
                print(f"Unknown country: {key}")
                if self.mute:
                    return
                else:
                    raise
        if value == 0:
            if key in self.cases:
                del self.cases[key]
        else:
            self.cases[key] = int(value)

    def __getitem__(self, item):
        if isinstance(item, str):
            item = id_from_iso3.get(item, -1)
        return self.cases.get(item, 0)

    def build_query(self, **kwargs):
        """
        {"geolevel":"basin",
        "period":6,
        "sources":[1036,2852,2857],
        "cases":{"1036":30,"2852":20,"2857":10},
        "month":"Jan",
        "travel_level":1,
        "userdata":{}}
        """
        limited_cases = dict(
            sorted(self.cases.items(), key=lambda t: t[1]
                   )[-KEEP_TOP_CASES_COUNT:]
        )
        if len(limited_cases) < len(self.cases):
            print(
                f"Keeping only top {KEEP_TOP_CASES_COUNT} case entries due to"
                f" Epirisk limits.")

        # Eliminate non-countries from query response:
        limited_cases[53] = 0  # Northern Cyprus
        limited_cases[4] = 0  # Aland islands

        query = dict(
            geolevel='country',
            period=self.period,
            sources=list(limited_cases.keys()),
            cases=limited_cases,
            month=_months[self.month - 1],
            travel_level=self.travel_level,
            userdata={},
            **kwargs
        )
        return query

    def __b64_query(self, **kwargs):
        query = self.build_query(**kwargs)
        return b64encode(json.dumps(query).encode('utf-8'))

    def get_risk(self):
        url = 'http://epirisk.net/era/getrisk'
        r = requests.get(url, params={'q': self.__b64_query()})
        return ConnectionsRisk(r.json())

    def get_exported_cases(self):
        url = 'http://epirisk.net/era/getexportedcases'
        r = requests.get(url, params={'q': self.__b64_query()})
        return ExportedCases(r.json())

    def get_exported_case_per_target(self, targets: List[int]):
        url = 'http://epirisk.net/era/getexportedcases'
        r = requests.get(url, params={'q': self.__b64_query(targets=targets)})
        return ExportedCases(r.json())


def query_epirisk(cases, *, mute=True):
    """
    Sum up all reported cases per country on most recent date, query epirisk
    and save connections and per-country risks
    in corresponding spreadsheets.
    :param cases: data frame with numbers of Confirmed cases per location and
    date.
    Expected columns: 'Country/Region', 'Confirmed', 'Date'
    :param population_sheet: gspread Spreadsheet object;
    Expected columns: 'Country Name', 'Country Code', 'Year', 'Population'
    :param mute: bool, defines behavior on missing country names:
    if True - ignore, if False - throw exception
    """
    epirisk = setup_epirisk(cases, mute)
    risks = epirisk.get_risk()
    connections_df = risks.connections_df()
    distribution_df = risks.distribution_df()
    # population_df = pd.DataFrame(
    #     population_sheet.worksheet('population').get_all_records())

    exported = epirisk.get_exported_cases()

    latest_cases_df = latest_cases_per_country(cases)

    # Join risk and cases
    risk_cases_df = pd.merge(distribution_df[['ISO3', 'Risk']],
                             latest_cases_df[['ISO3', 'Confirmed']],
                             on='ISO3', how='outer')
    risk_cases_df['Confirmed'] = risk_cases_df['Confirmed'].fillna(0)
    risk_cases_df['Risk'] = risk_cases_df['Risk'].fillna(1)

    # Detect repetitions
    repet = risk_cases_df.groupby('ISO3').apply(len)
    if repet.max() > 1:
        repet = repet[repet > 1].index
        print("There are repetitions in risk_cases_df. "
              + str(list(repet)))

    risk_cases_ratio_df = corona.countries.join_countries_data(risk_cases_df)

    risk_cases_ratio_df['per_mil'] = risk_cases_ratio_df['Confirmed'].astype(
        float) / risk_cases_ratio_df['population'].astype(float) * 1000000
    bins = [0, 2, 5, 10, 50, 100, 400, 5000]
    labels = ['0-2', '2-5', '5-10', '10-50', '50-100', '100-400', '>400']

    risk_cases_ratio_df['bin'] = ''

    risk_cases_ratio_df = adds_bin_col(risk_cases_ratio_df)
    risk_cases_ratio_df['bin'].where(
        risk_cases_ratio_df.Confirmed.astype(int) == 0,
        pd.cut(risk_cases_ratio_df['per_mil'],
               bins=bins, labels=labels).astype(str),
        inplace=True
    )

    risk_cases_ratio_df = normalize_risk_cases(risk_cases_ratio_df)

    return connections_df, distribution_df, exported, risk_cases_ratio_df


def setup_epirisk(cases_df, mute=True):
    """
    Factory method for creating EpiriskQuery objects initiated with the
    provided cases. Data from the most recent date
    in cases_df is used.

    :param cases_df: DataFrame with the progress of the Covid-19 epidemic.
    :param mute: bool, if True then no exception is thrown if data for missing
    country is added
    :return: EpiriskQuery, query object initiated with the current state of
    the epidemy
    """

    return EpiriskQuery.from_cases(latest_cases_per_country(cases_df), mute)


def bin_risk(row, bins):
    """Creates 3 bins for risk and puts risk into appropriate bin"""
    if row['Risk'] != 1:
        if row['Risk'] < bins[0]:
            return '0, Low risk'
        if (row['Risk'] >= bins[0]) and (row['Risk'] < bins[1]):
            return '0, Medium risk'
        if row['Risk'] >= bins[1]:
            return '0, High risk'
    else:
        return row['bin']


def adds_bin_col(risk_cases_ratio_df):
    """Adds column with bins to dataframe"""
    df = risk_cases_ratio_df.copy()
    max_risk = df[df.Risk < 1].Risk.max()
    min_risk = df[df.Risk < 1].Risk.min()
    risk_step = (max_risk - min_risk) / 3
    bins = [min_risk + risk_step, min_risk + 2 * risk_step]
    df.loc[:, 'bin'] = df.apply(lambda row: bin_risk(row, bins), axis=1)
    return df


def latest_cases_per_country(cases_df: pd.DataFrame):
    cases = cases_df[cases_df['Date'] == cases_df['Date'].max()].copy()
    cases.dropna(subset=['ISO3'], inplace=True)
    cases = cases.groupby('ISO3').sum()
    cases = cases[['Confirmed']].reset_index()
    return cases


def normalize_risk_cases(risk_cases_df):
    risk_cases_df.loc[risk_cases_df.Confirmed > 0, 'Risk'] = 1.0
    select_remaining_risk = risk_cases_df.Risk < 1
    risk_cases_df.loc[select_remaining_risk, 'Risk'] /= \
        risk_cases_df.loc[select_remaining_risk, 'Risk'].sum()
    columns_rename = {'name_short': 'Country',
                      'name_pl': 'Kraj',
                      'ISO3': 'Country_ISO3'}
    return risk_cases_df.rename(columns=columns_rename)
