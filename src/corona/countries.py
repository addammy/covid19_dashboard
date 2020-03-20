"""
Country converter module

Outline:

INs :
    - Country Name (fuzzy)
    - iso 3letter code

OUTs:
    - country name EN
    - country name PL
    - epirisk id
    - iso 3letter code
    - continent
    - population

Notes:
- only repetition in mapping epirisk-->coco: Cyprus&North Cyprus
        (will be resolved to Cyprus)

"""
import json
from importlib.resources import read_text, open_text

import country_converter as coco
import pandas as pd

def _make_countries_df():
    population_data = pd.read_csv(
        open_text('corona.resources', 'population.csv'),
        index_col="ISO3")
    population_data.population = population_data.population.astype('Int64')

    epirisk_mapping = _map_epirisk_ids()
    epirisk_mapping = pd.DataFrame.from_dict(epirisk_mapping,
                                             orient='index',
                                             columns=['epirisk_id'])
    epirisk_mapping.epirisk_id = epirisk_mapping.epirisk_id.astype('Int64')

    coco_data = _conv.data.set_index('ISO3', drop=False)
    iso_pl_df = pd.read_csv(open_text("corona.resources", "ISO3_pl.csv"),
                            index_col='ISO3')

    all_data = coco_data.join([population_data, epirisk_mapping, iso_pl_df])
    return all_data


def _map_epirisk_ids():
    _epirisk_mapping = {}
    _epirisk_init_data = json.loads(
        read_text('corona.resources', 'epirisk_getinitdata.json'))
    for country in _epirisk_init_data['countries']:
        label = country['label']
        if label == 'Cyprus, Northern':
            print('Skipping Northern Cyprus')
            continue
        converted = _conv.convert(label, src='regex', not_found=-1)
        if converted == -1:
            raise KeyError(
                f'Epirisk country "{label}" not recognized by country_converter.')
        elif converted in _epirisk_mapping:
            raise KeyError(f'Already assigned ISO3 "{converted}" '
                           f'to another country id({_epirisk_mapping[converted]}).')
        else:
            _epirisk_mapping[converted] = country['id']
    return _epirisk_mapping


def iso3_from_name(name, not_found=None):
    nf_marker = object()
    iso3 = _conv.convert(name, src='regex', not_found=nf_marker)
    if iso3 is nf_marker:
        if not_found is None:
            raise KeyError(f'Couldn\'t recognize country "{name}".')
        iso3 = not_found
    return iso3


def add_ISO3_from_name(df: pd.DataFrame, name_column='country_name',
                       not_found=None):
    names = df[name_column].unique()
    name_to_iso3 = {name: iso3_from_name(name, not_found) for name in names}
    df['ISO3'] = df[name_column].map(name_to_iso3)


def get_countries_df(columns=None):
    if columns is None:
        return _countries_df.copy()
    else:
        return _countries_df[columns].copy()


def join_countries_data(df: pd.DataFrame,
                        add_columns=None):
    if add_columns is None:
        add_columns = ['name_short', 'name_pl', 'epirisk_id',
                       'population']
    df = df.copy()
    return df.join(get_countries_df(add_columns), on='ISO3')


_conv = coco.CountryConverter()
_conv.data.loc[_conv.data.name_short == 'Kosovo', 'ISO3'] = 'KOS'
_countries_df = _make_countries_df()
