import json
from io import StringIO
from base64 import b64encode
from collections import namedtuple
from dataclasses import dataclass
from importlib.resources import read_text
from typing import List, Dict, Set

import pandas as pd
import requests
import country_converter as coco

_months = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']
Location = namedtuple('Location', 'id label lat lng population')
_init_data = json.loads(read_text('corona.resources', 'epirisk_getinitdata.json'))
cities = {city['label']: Location(**city) for city in _init_data['basins']}
countries = {city['label']: Location(**city) for city in _init_data['countries']}
locations = {'basin': cities, 'country': countries}
cities_by_id = {city.id: city for city in cities.values()}
countries_by_id = {country.id: country for country in countries.values()}


@dataclass
class Distribution:
    values: Dict[int, float]
    residual: float

    def __init__(self, json):
        self.values = {int(k): float(v) for (k, v) in json['distribution'].items()}
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
                target = 'world' if target == -1 else countries_by_id[target].label
            for v, p in distribution.values.items():
                data.append((target, v, p))
        return pd.DataFrame(data, columns=['where', 'value', 'probability'])


@dataclass
class ConnectionsRisk:
    connections: Dict[int, Set[int]]
    distribution: Dict[int, float]
    residual: float

    def __init__(self, json):
        self.connections = {int(k): set(v) for k, v in json['connections'].items()}
        self.distribution = {int(k): v for k, v in json['distribution'].items()}
        self.residual = json['residual']

    def connections_df(self):
        data = []
        for country_id, destinations in self.connections.items():
            country_name = countries_by_id[country_id].label
            data.extend(
                [(country_id, country_name, dest_id, countries_by_id[dest_id].label) for dest_id in destinations]
            )
        df = pd.DataFrame(data, columns=['country_id', 'country_name', 'dest_id', 'dest_name'])
        return df

    def distribution_df(self):
        data = []
        for country_id, risk in self.distribution.items():
            data.append([country_id, countries_by_id[country_id].label, risk])

        df = pd.DataFrame(data, columns=['CountryId', 'Country', 'Risk'])
        return df


class EpiriskQuery:
    def __init__(self, geolevel='country', *, period=10, month=1, travel_level=1.0, mute=False):
        """
        Class for building Epirisk queries.

        Add sources, countries or basins(cities) by name:

        >>> query = EpiriskQuery()
        >>> query['Poland'] = 100

        or by id:

        >>> query[169] = 100

        Id's can be translated the corona.epirisk.api module's dictionaries:

        >>> print(countries['Poland'])
        Location(id=169, label='Poland', lat=52.0, lng=20.0, population=37531600)
        >>> print(countries_by_id[169])
        Location(id=169, label='Poland', lat=52.0, lng=20.0, population=37531600)

        Init with options from Epirisk's form:
        :param geolevel: 'country' or 'basin'
        :param period: Days to onset of symptoms
        :param month: Month of travel
        :param travel_level: float, 0.0-1.0 range. 0.0 for completely restricted travel, 1.0 for no travel restrictions.
        :param mute: bool, if True then no exception is thrown if data for missing country is added
        """
        self.cases = {}
        self.geolevel = geolevel
        self.locations = locations[geolevel]
        self.period = period
        self.month = month
        self.travel_level = travel_level
        self.mute = mute

    @staticmethod
    def from_cases(cases, mute):
        epirisk = EpiriskQuery(mute=mute)
        for country, count in cases.itertuples(False):
            epirisk[country] += count
        return epirisk

    def __setitem__(self, key, value):
        if isinstance(key, str):
            try:
                key = self.locations[key].id
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
            if item in self.locations:
                item = self.locations[item].id
            else:
                item = -1
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
        query = dict(
            geolevel=self.geolevel,
            period=self.period,
            sources=list(self.cases.keys()),
            cases=self.cases,
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


def query_epirisk(cases, mute=True):
    """
    Sum up all reported cases per country on most recent date, query epirisk and save connections and per-country risks
    in corresponding spreadsheets.
    :param confirmed_latest: data frame with numbers of Confirmed cases per location and date.
    Expected columns: 'Country/Region', 'Confirmed', 'Date'
    :param mute: bool, defines behavior on missing country names: if True - ignore, if False - throw exception
    """
    epirisk = setup_epirisk(cases, mute)

    risks = epirisk.get_risk()
    connections_df = risks.connections_df()
    distribution_df = risks.distribution_df()

    exported = epirisk.get_exported_cases()

    latest_cases_df = latest_cases_per_country(cases)

    # Join risk and cases
    risk_cases_df = pd.merge(distribution_df[['Country', 'Risk']],
                             latest_cases_df[['Country', 'Confirmed']],
                             on='Country', how='outer')
    risk_cases_df['Confirmed'] = risk_cases_df['Confirmed'].fillna(0)
    risk_cases_df['Risk'] = risk_cases_df['Risk'].fillna(1)
    # Correct country names
    risk_cases_df = assign_country_codes(risk_cases_df)
    # Assign polish names
    iso_pl_df = pd.read_csv(StringIO(read_text("corona.resources", "ISO3_pl.csv")))
    risk_cases_df = pd.merge(risk_cases_df, iso_pl_df, how='left', on='Country_ISO3')

    return connections_df, distribution_df, exported, risk_cases_df


def setup_epirisk(cases_df, mute=True):
    """
    Factory method for creating EpiriskQuery objects initiated with the provided cases. Data from the most recent date
    in cases_df is used.

    :param cases_df: DataFrame with the progres of the Covid-19 epidemy.
    :param mute: bool, if True then no exception is thrown if data for missing country is added
    :return: EpiriskQuery, query object initiated with the current state of the epidemy
    """
    return EpiriskQuery.from_cases(latest_cases_per_country(cases_df), mute)


def latest_cases_per_country(cases_df: pd.DataFrame):
    cases = cases_df[cases_df['Date'] == cases_df['Date'].max()].copy()
    cases['Country/Region'] = adjust_country_names(cases['Country/Region'])
    cases = cases.groupby('Country/Region').sum()
    cases = cases[['Confirmed']].reset_index()
    cases.rename(columns={'Country/Region': 'Country'}, inplace=True)
    return cases


def adjust_country_names(country_names: pd.Series):
    """
    Translates Country/Region names from the ones used by JHU CSSE
    to those expected by te Epirisk API (as they can be found in corona/resources/epirisk_getinitdata.json).

    Currently realized as manual mapping of mismatched names.

    :param country_names: Series of Country/Region names, as in the spreadsheet with cases
    :return: Series of Country/Region names, as expected by the Epirisk API
    """

    mapping = {
        'Hong Kong': 'China',
        'Macau': 'China',
        'Mainland China': 'China',
        'South Korea': 'Korea, Rep.',
        'US': 'United States of America',
        'Russia': 'Russian Federation',
        'UK': 'United Kingdom',
        'Egypt': 'Egypt, Arab Rep.',
        'North Macedonia': 'Macedonia'
    }

    return country_names.replace(mapping)


def assign_country_codes(df):
    correct_codes_dict = {
        'UK': 'GBR',
        'XKX': 'KOS',
        'North Ireland': 'UK',
        'ALA': 'FIN'
    }
    correct_names_dict = {
        'North Ireland': 'UK',
        'Aland Islands': 'Finland'
    }
    country_names = df.loc[:, 'Country'].to_list()
    country_names = [correct_names_dict.get(cn, cn) for cn in country_names]
    country_codes = coco.convert(names=country_names, to='ISO3', not_found=None)
    country_codes = [correct_codes_dict.get(cc, cc) for cc in country_codes]
    df['Country_ISO3'] = country_codes
    df['Country'] = country_names
    return df


def risk_cases_alteration(risk_cases_df):
    altered = risk_cases_df.copy()
    indonesia = altered['Country'] == 'Indonesia'
    altered.loc[indonesia, 'Risk'] = 0.0

    at_risk = altered['Risk'] < 1
    altered.loc[at_risk, 'Risk'] = altered[at_risk].Risk / altered[at_risk].Risk.sum()

    return altered
