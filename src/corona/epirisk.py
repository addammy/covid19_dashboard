import json
from base64 import b64encode
from collections import namedtuple
from dataclasses import dataclass
from importlib.resources import read_text
from typing import List, Dict, Set

import pandas as pd
import requests

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

        df = pd.DataFrame(data, columns=['country_id', 'country_name', 'risk'])
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

        self.cases[key] = int(value)

    def __getitem__(self, item):
        if isinstance(item, str):
            item = self.locations[item].id
        return self.cases[item]

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

    return connections_df, distribution_df, exported


def setup_epirisk(cases_df, mute=True):
    """
    Factory method for creating EpiriskQuery objects initiated with the provided cases. Data from the most recent date
    in cases_df is used.

    :param cases_df: DataFrame with the progres of the Covid-19 epidemy.
    :param mute: bool, if True then no exception is thrown if data for missing country is added
    :return: EpiriskQuery, query object initiated with the current state of the epidemy
    """
    cases = cases_df[cases_df['Date'] == cases_df['Date'].max()].copy()
    cases['Country/Region'] = adjust_country_names(cases['Country/Region'])
    cases = cases.groupby('Country/Region').sum()
    cases = cases[['Confirmed']].reset_index()
    epirisk = EpiriskQuery(mute=mute)
    for country, count in cases.itertuples(False):
        epirisk[country] = count
    return epirisk


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
        'Egypt': 'Egypt, Arab Rep.'
    }

    return country_names.replace(mapping)