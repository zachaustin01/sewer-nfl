'''
Module with catalog dictionary that returns warehouse functions as well as requirements based on a
particular key. This allows the functions (and therefore variables) to be loaded in via a config
file.
'''

REPO_NAME = 'sewer-nfl'
import sys, os
cwd = str(os.getcwd())
repo_dir = cwd[:cwd.find(REPO_NAME)+len(REPO_NAME)]
sys.path.insert(0,repo_dir)

# Import setups
from warehouse.pipelines.pbp.setup import setup_pbp

# Import pipelines
from warehouse.pipelines.pbp.involvement import *
from warehouse.pipelines.pbp.performance import *
from warehouse.pipelines.pbp.combinations import *

# Import config
from warehouse.config import *

def build_catalog(config):

    FUNCTION_CATALOG = {
    ####################################################################################################
    # Team Performance Combinations (Higher Level Dependencies)
    ####################################################################################################
        'turnover_propensity':{
            'func': pipe_turnover_propensity,
            'params':(
                pbp_api_data,
                ngs['passing'],
                TRAILING_WEEKS
            )
        },
        'def_turnover_propensity':{
            'func': pipe_def_turnover_propensity,
            'params':(
                pbp_api_data,
                ngs['passing'],
                ngs['receiving'],
                TRAILING_WEEKS
            )
        },
        'epa_hhi_combo':{
            'func': pipe_epa_hhi_combo,
            'params':(
                pbp_api_data,
                TRAILING_WEEKS
            )
        },
        'points_per_epa':{
            'func': pipe_points_per_epa,
            'params':(
                pbp_api_data,
                TRAILING_WEEKS
            )
        },
        'offensive_coaching':{
            'func': pipe_offense_coaching_ability,
            'params':(
                pbp_api_data,
                TRAILING_WEEKS
            )
        },
        'defensive_coaching':{
            'func': pipe_defense_coaching_ability,
            'params':(
                pbp_api_data,
                TRAILING_WEEKS
            )
        },
        'def_burn_commit':{
            'func': pipe_def_burn_commit,
            'params':(
                ngs['receiving'],
                ngs['rushing'],
                TRAILING_WEEKS
            )
        },
        'offense_scoring_propensity':{
            'func': pipe_offense_scoring_propensity,
            'params':(
                pbp_api_data,
                TRAILING_WEEKS
            )
        },
        'defense_scoring_allowance':{
            'func': pipe_defense_scoring_allowance,
            'params':(
                pbp_api_data,
                TRAILING_WEEKS
            )
        },
        'offense_big_play':{
            'func': pipe_offense_big_play,
            'params':(
                pbp_api_data,
                ngs['passing'],
                TRAILING_WEEKS
            )
        },
        'defense_big_play':{
            'func': pipe_def_big_play,
            'params':(
                pbp_api_data,
                ngs['receiving'],
                ngs['rushing'],
                TRAILING_WEEKS
            )
        },
        'garbagetime_epa':{
            'func': pipe_garbagetime_epa,
            'params':(
                pbp_api_data,
                TRAILING_WEEKS
            )
        },
        'overall_coaching':{
            'func': pipe_overall_coaching,
            'params':(
                pbp_api_data,
                TRAILING_WEEKS
            )
        },

    ####################################################################################################
    # Team Performance (Lower level Dependencies)
    ####################################################################################################
        'get_yards_per_rush':{
            'func': get_yards_per_rush,
            'params':(
                pbp_api_data,
                TRAILING_WEEKS
            )
        },
        'get_yards_per_pass':{
            'func': get_yards_per_pass,
            'params':(
                pbp_api_data,
                TRAILING_WEEKS
            )
        },
        'get_epa_per_rush':{
            'func': get_epa_per_rush,
            'params':(
                pbp_api_data,
                TRAILING_WEEKS
            )
        },
        'get_epa_per_pass':{
            'func': get_epa_per_pass,
            'params':(
                pbp_api_data,
                TRAILING_WEEKS
            )
        },
        'get_offense_epa':{
            'func': get_offense_epa,
            'params':(
                pbp_api_data,
                TRAILING_WEEKS
            )
        },
        'get_pct_pass':{
            'func': get_pct_pass,
            'params':(
                pbp_api_data,
                TRAILING_WEEKS
            )
        },
        'get_pct_run':{
            'func': get_pct_run,
            'params':(
                pbp_api_data,
                TRAILING_WEEKS
            )
        },
        'get_team_hhi':{
            'func': get_team_hhi,
            'params':(
                pbp_api_data,
                TRAILING_WEEKS
            )
        },
        'get_hhi_by_type':{
            'func': get_hhi_by_type,
            'params':[
                pbp_api_data,
                TRAILING_WEEKS
            ]
        },
        'get_def_yards_per_pass':{
            'func': get_def_yards_per_pass,
            'params':[
                pbp_api_data,
                TRAILING_WEEKS
            ]
        },
        'get_def_yards_per_rush':{
            'func': get_def_yards_per_rush,
            'params':[
                pbp_api_data,
                TRAILING_WEEKS
            ]
        },
        'get_def_epa_per_pass':{
            'func': get_def_epa_per_pass,
            'params':[
                pbp_api_data,
                TRAILING_WEEKS
            ]
        },
        'get_def_epa_per_rush':{
            'func': get_def_epa_per_rush,
            'params':[
                pbp_api_data,
                TRAILING_WEEKS
            ]
        },
        'get_def_points_per_drive':{
            'func': get_def_points_per_drive,
            'params':[
                pbp_api_data,
                TRAILING_WEEKS
            ]
        },
        'get_points_per_RZ':{
            'func': get_points_per_RZ,
            'params':[
                pbp_api_data,
                TRAILING_WEEKS
            ]
        },
        'get_def_points_per_RZ':{
            'func': get_def_points_per_RZ,
            'params':[
                pbp_api_data,
                TRAILING_WEEKS
            ]
        },
        'get_points_per_game':{
            'func': get_points_per_game,
            'params':[
                pbp_api_data,
                TRAILING_WEEKS
            ]
        },
        'get_def_points_per_game':{
            'func': get_def_points_per_game,
            'params':[
                pbp_api_data,
                TRAILING_WEEKS
            ]
        },
        'get_pct_leading':{
            'func': get_pct_leading,
            'params':[
                pbp_api_data,
                TRAILING_WEEKS
            ]
        },
        'get_pct_leading_three':{
            'func': get_pct_leading_three,
            'params':[
                pbp_api_data,
                TRAILING_WEEKS
            ]
        },
        'get_drives_in_turnover':{
            'func': get_drives_in_turnover,
            'params':[
                pbp_api_data,
                TRAILING_WEEKS
            ]
        },
        'get_def_drives_in_turnover':{
            'func': get_def_drives_in_turnover,
            'params':[
                pbp_api_data,
                TRAILING_WEEKS
            ]
        },
        'get_actual_game_points':{
            'func': get_actual_game_points,
            'params':[
                pbp_api_data,
                TRAILING_WEEKS
            ]
        },
        'get_epa_sum':{
            'func': get_epa_sum,
            'params':[
                pbp_api_data,
                TRAILING_WEEKS
            ]
        },
        'get_qb_aggr':{
            'func': get_qb_aggr,
            'params':[
                ngs['passing'],
                TRAILING_WEEKS
            ]
        },
        'get_def_qb_aggr':{
            'func': get_def_qb_aggr,
            'params':[
                ngs['passing'],
                TRAILING_WEEKS
            ]
        },
        'get_def_box_stuff':{
            'func': get_def_box_stuff,
            'params':[
                ngs['rushing'],
                TRAILING_WEEKS
            ]
        },
        'get_def_cushion':{
            'func': get_def_cushion,
            'params':[
                ngs['receiving'],
                TRAILING_WEEKS
            ]
        },
        'get_def_separation':{
            'func': get_def_separation,
            'params':[
                ngs['receiving'],
                TRAILING_WEEKS
            ]
        },
        'get_avg_throw_dist':{
            'func': get_avg_throw_dist,
            'params':[
                ngs['passing'],
                TRAILING_WEEKS
            ]
        },
        'get_off_plays_25yd':{
            'func': get_off_plays_25yd,
            'params':[
                pbp_api_data,
                TRAILING_WEEKS
            ]
        },
        'get_off_td_25yd':{
            'func': get_off_td_25yd,
            'params':[
                pbp_api_data,
                TRAILING_WEEKS
            ]
        },
        'get_def_plays_25yd':{
            'func': get_def_plays_25yd,
            'params':[
                pbp_api_data,
                TRAILING_WEEKS
            ]
        },
        'get_def_td_25yd':{
            'func': get_def_td_25yd,
            'params':[
                pbp_api_data,
                TRAILING_WEEKS
            ]
        },
        'get_off_scr':{
            'func': get_off_scr,
            'params':[
                pbp_api_data,
                TRAILING_WEEKS
            ]
        },
        'get_def_scr_allowed':{
            'func': get_def_scr_allowed,
            'params':[
                pbp_api_data,
                TRAILING_WEEKS
            ]
        },
        'get_qb_comp_rate':{
            'func': get_qb_comp_rate,
            'params':[
                pbp_api_data,
                TRAILING_WEEKS
            ]
        },
        'qb_def_comp_rate_allowed':{
            'func': qb_def_comp_rate_allowed,
            'params':[
                pbp_api_data,
                TRAILING_WEEKS
            ]
        },
        'qb_hits_allowed_off':{
            'func': qb_hits_allowed_off,
            'params':[
                pbp_api_data,
                TRAILING_WEEKS
            ]
        },
        'get_def_qb_hits':{
            'func': get_def_qb_hits,
            'params':[
                pbp_api_data,
                TRAILING_WEEKS
            ]
        },
        'get_season_point_diff':{
            'func': get_season_point_diff,
            'params':[
                pbp_api_data
            ]
        },
        'get_first_drive_points_scored':{
            'func': get_first_drive_points_scored,
            'params':[
                pbp_api_data,
                TRAILING_WEEKS
            ]
        },
        'get_def_first_drive_points_allowed':{
            'func': get_def_first_drive_points_allowed,
            'params':[
                pbp_api_data,
                TRAILING_WEEKS
            ]
        },
        'get_yac_air_yards':{
            'func': get_yac_air_yards,
            'params':[
                pbp_api_data,
                TRAILING_WEEKS
            ]
        },
        'get_2h_first_drive_points_scored':{
            'func': get_2h_first_drive_points_scored,
            'params':[
                pbp_api_data,
                TRAILING_WEEKS
            ]
        },
        'get_2h_def_first_drive_points_allowed':{
            'func': get_2h_def_first_drive_points_allowed,
            'params':[
                pbp_api_data,
                TRAILING_WEEKS
            ]
        }
    }

    return FUNCTION_CATALOG