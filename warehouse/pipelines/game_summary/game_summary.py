"""

Pulling data relating to the game metadata

"""
import nfl_data_py as nflreadr
import numpy as np

def game_outcomes(pbp_api_data):

    xx = pbp_api_data
    week_spread_results = xx[['season','week','home_team','away_team','spread_line','home_score','away_score']].drop_duplicates().reset_index(drop=True)

    week_spread_results['spread_line'] = -1 * week_spread_results['spread_line']

    week_spread_results['home_cover'] = np.where((week_spread_results['home_score'] + week_spread_results['spread_line'])>=week_spread_results['away_score'], 1, 0)

    week_spread_results['within_three'] = np.where(abs(week_spread_results['home_score'] - week_spread_results['away_score'])<=3, 1, 0)

    return week_spread_results
