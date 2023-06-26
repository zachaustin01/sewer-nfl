import numpy as np
import pandas as pd
import nfl_data_py as nflreadr
import requests as requests
import functools as ft

def setup_pbp(
        only_regular_season = True,
        starting_year = 2016,
        ending_year = 2022
):
    '''
    Function to run some preprocessing, import api data, and return useful dataframes
    Returns Next Gen Stats dataframes as well as API data
    ngs = {
        'rushing':<df>,
        'receiving':<df>,
        'passing':<df>
    }
    api_data
    Example call:

    api_data, ngs = setup_pbp()
    next_gen_stats_rush, next_gen_stats_rec, next_gen_stats_pass = ngs['rushing'], ngs['receiving'],\
        ngs['passing']
    '''

    years_included = range(starting_year, ending_year+1)
    api_data = nflreadr.import_pbp_data(years_included)
    api_data = api_data.assign(play_counter = 1)

    if only_regular_season:
        api_data = api_data[api_data['week']<=18]

    ngs = {pt: nflreadr.import_ngs_data(pt) for pt in ['rushing','receiving','passing']}
    ngs = {key: value[value['week'] > 0] for key, value in ngs.items()}
    for pt, stats in ngs.items():
        ngs[pt]['team_abbr'] = ngs[pt]['team_abbr'].replace(['LAR'], 'LA')

    # Adding defenses
    getting_schedule = api_data[['season','week','posteam','defteam']].drop_duplicates().reset_index(drop=True)
    ngs = {key: value.merge(getting_schedule, how='left',left_on=['season','week', 'team_abbr'],
                         right_on=['season','week', 'posteam']).drop(['posteam'], axis=1)
                         for key, value in ngs.items()}

    return ngs
