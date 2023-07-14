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

# Import pipelines
from warehouse.pipelines.pbp.involvement import *
from warehouse.pipelines.pbp.performance import *
from warehouse.pipelines.pbp.combinations import *

def build_catalog(config):

    FUNCTION_CATALOG = {
####################################################################################################
# Team Performance Combinations (Higher Level Dependencies)
####################################################################################################
    'turnover_propensity':{
        'func': pipe_turnover_propensity,
        'params':(
            config.pbp_api_data,
            config.ngs['passing'],
            config.TRAILING_WEEKS
        ),
        'agg_level':'team',
        'type':'pipeline',
        'ball_side':'off',
        'var_category':'boom_bust',
        'var_importance': 7,
        'var_counterpart':'def_turnover_propensity'
    },
    'def_turnover_propensity':{
        'func': pipe_def_turnover_propensity,
        'params':(
            config.pbp_api_data,
            config.ngs['passing'],
            config.ngs['receiving'],
            config.TRAILING_WEEKS
        ),
        'agg_level':'team',
        'type':'pipeline',
        'ball_side':'def',
        'var_category':'boom_bust',
        'var_importance':7,
        'var_counterpart':'turnover_propensity'
    },
    'balanced_player_efficacy':{
        'func': pipe_epa_hhi_combo,
        'params':(
            config.pbp_api_data,
            config.TRAILING_WEEKS
        ),
        'agg_level':'team',
        'type':'pipeline',
        'ball_side':'off',
        'var_category':'full_measure',
        'var_importance':7
    },
    'points_per_epa':{
        'func': pipe_points_per_epa,
        'params':(
            config.pbp_api_data,
            config.TRAILING_WEEKS
        ),
        'agg_level':'team',
        'type':'pipeline',
        'ball_side':'off',
        'var_category':'full_measure',
        'var_importance':6
    },
    'off_coaching':{
        'func': pipe_offense_coaching_ability,
        'params':(
            config.pbp_api_data,
            config.TRAILING_WEEKS
        ),
        'agg_level':'team',
        'type':'pipeline',
        'ball_side':'off',
        'var_category':'coaching_aptitude',
        'var_importance':5,
        'var_counterpart':'def_coaching'
    },
    'def_coaching':{
        'func': pipe_defense_coaching_ability,
        'params':(
            config.pbp_api_data,
            config.TRAILING_WEEKS
        ),
        'agg_level':'team',
        'type':'pipeline',
        'ball_side':'def',
        'var_category':'coaching_aptitude',
        'var_importance':5,
        'var_counterpart':'off_coaching'
    },
    'conservative_coverage':{
        'func': pipe_def_burn_commit,
        'params':(
            config.ngs['receiving'],
            config.ngs['rushing'],
            config.TRAILING_WEEKS
        ),
        'agg_level':'team',
        'type':'pipeline',
        'ball_side':'def',
        'var_category':'boom_bust',
        'var_importance':7
    },
    'offensive_scoring_ability':{
        'func': pipe_offense_scoring_propensity,
        'params':(
            config.pbp_api_data,
            config.TRAILING_WEEKS
        ),
        'agg_level':'team',
        'type':'pipeline',
        'ball_side':'off',
        'var_category':'scoring_success',
        'var_importance':7,
        'var_counterpart':'defensive_scoring_allow'
    },
    'defensive_scoring_allow':{
        'func': pipe_defense_scoring_allowance,
        'params':(
            config.pbp_api_data,
            config.TRAILING_WEEKS
        ),
        'agg_level':'team',
        'type':'pipeline',
        'ball_side':'def',
        'var_category':'scoring_success',
        'var_importance':7,
        'var_counterpart':'offensive_scoring_ability'

    },
    'off_big_play_propensity':{
        'func': pipe_offense_big_play,
        'params':(
            config.pbp_api_data,
            config.ngs['passing'],
            config.TRAILING_WEEKS
        ),
        'agg_level':'team',
        'type':'pipeline',
        'ball_side':'off',
        'var_category':'boom_bust',
        'var_importance':7,
        'var_counterpart':'defense_big_play_propensity'

    },
    'defense_big_play_propensity':{
        'func': pipe_def_big_play,
        'params':(
            config.pbp_api_data,
            config.ngs['receiving'],
            config.ngs['rushing'],
            config.TRAILING_WEEKS
        ),
        'agg_level':'team',
        'type':'pipeline',
        'ball_side':'def',
        'var_category':'boom_bust',
        'var_importance':7,
        'var_counterpart':'off_big_play_propensity'
    },
    'time_epa':{
        'func': pipe_garbagetime_epa,
        'output_columns' : ['normaltime_epa', 'garbagetime_epa'],
        'params':(
            config.pbp_api_data,
            config.TRAILING_WEEKS
        ),
        'agg_level':'team',
        'type':'pipeline',
        'ball_side':'off',
        'var_category':'full_measure',
        'var_importance':7
    },
    'overall_coaching':{
        'func': pipe_overall_coaching,
        'params':(
            config.pbp_api_data,
            config.TRAILING_WEEKS
        ),
        'agg_level':'team',
        'type':'pipeline',
        'ball_side':'off',
        'var_category':'coaching_aptitude',
        'var_importance':7
    },

####################################################################################################
# Team Performance (Lower level Dependencies)
####################################################################################################
    'yards_per_carry':{
        'func': get_yards_per_rush,
        'params':(
            config.pbp_api_data,
            config.TRAILING_WEEKS
        ),
        'agg_level':'team',
        'type':'warehouse',
        'ball_side':'off',
        'var_category':'play_success',
        'var_importance':2,
        'var_counterpart':'def_yards_per_rush'

    },
    'yards_per_pass':{
        'func': get_yards_per_pass,
        'params':(
            config.pbp_api_data,
            config.TRAILING_WEEKS
        ),
        'agg_level':'team',
        'type':'warehouse',
        'ball_side':'off',
        'var_category':'play_success',
        'var_importance':2,
        'var_counterpart':'def_yards_per_pass'
    },
    'epa_per_rush':{
        'func': get_epa_per_rush,
        'params':(
            config.pbp_api_data,
            config.TRAILING_WEEKS
        ),
        'agg_level':'team',
        'type':'warehouse',
        'ball_side':'off',
        'var_category':'play_success',
        'var_importance':2,
        'var_counterpart':'def_rush_epa'
    },
    'epa_per_pass':{
        'func': get_epa_per_pass,
        'params':(
            config.pbp_api_data,
            config.TRAILING_WEEKS
        ),
        'agg_level':'team',
        'type':'warehouse',
        'ball_side':'off',
        'var_category':'play_success',
        'var_importance':2,
        'var_counterpart':'def_pass_epa'
    },
    'off_epa':{
        'func': get_offense_epa,
        'params':(
            config.pbp_api_data,
            config.TRAILING_WEEKS
        ),
        'agg_level':'team',
        'type':'warehouse',
        'ball_side':'off',
        'var_category':'play_success',
        'var_importance':2
    },
    'pct_pass':{
        'func': get_pct_pass,
        'params':(
            config.pbp_api_data,
            config.TRAILING_WEEKS
        ),
        'agg_level':'team',
        'type':'warehouse',
        'ball_side':'off',
        'var_category':'play_style',
        'var_importance':1
    },
    'pct_run':{
        'func': get_pct_run,
        'params':(
            config.pbp_api_data,
            config.TRAILING_WEEKS
        ),
        'agg_level':'team',
        'type':'warehouse',
        'ball_side':'off',
        'var_category':'play_style',
        'var_importance':1
    },
    'team_HHI':{
        'func': get_team_hhi,
        'params':(
            config.pbp_api_data,
            config.TRAILING_WEEKS
        ),
        'agg_level':'team',
        'type':'warehouse',
        'ball_side':'off',
        'var_category':'play_style',
        'var_importance':3
    },
    'team_passing_HHI':{
        'func': get_hhi_by_type,
        'params':[
            config.pbp_api_data,
            config.TRAILING_WEEKS
        ],
        'agg_level':'team',
        'type':'warehouse',
        'ball_side':'off',
        'var_category':'play_style',
        'var_importance':2
    },
    'def_yards_per_pass':{
        'func': get_def_yards_per_pass,
        'params':[
            config.pbp_api_data,
            config.TRAILING_WEEKS
        ],
        'agg_level':'team',
        'type':'warehouse',
        'ball_side':'def',
        'var_category':'play_success',
        'var_importance':2,
        'var_counterpart':'yards_per_pass'
    },
    'def_yards_per_rush':{
        'func': get_def_yards_per_rush,
        'params':[
            config.pbp_api_data,
            config.TRAILING_WEEKS
        ],
        'agg_level':'team',
        'type':'warehouse',
        'ball_side':'def',
        'var_category':'play_success',
        'var_importance':2,
        'var_counterpart':'yards_per_carry'
    },
    'def_pass_epa':{
        'func': get_def_epa_per_pass,
        'params':[
            config.pbp_api_data,
            config.TRAILING_WEEKS
        ],
        'agg_level':'team',
        'type':'warehouse',
        'ball_side':'def',
        'var_category':'play_success',
        'var_importance':2,
        'var_counterpart':'epa_per_pass'
    },
    'def_rush_epa':{
        'func': get_def_epa_per_rush,
        'params':[
            config.pbp_api_data,
            config.TRAILING_WEEKS
        ],
        'agg_level':'team',
        'type':'warehouse',
        'ball_side':'def',
        'var_category':'play_success',
        'var_importance':2,
        'var_counterpart':'epa_per_rush'
    },
    'points_per_drive':{
        'func': get_points_per_drive,
        'params':[
            config.pbp_api_data,
            config.TRAILING_WEEKS
        ],
        'agg_level':'team',
        'type':'warehouse',
        'ball_side':'off',
        'var_category':'scoring_success',
        'var_importance':3,
        'var_counterpart':'def_points_per_drive'
    },
    'def_points_per_drive':{
        'func': get_def_points_per_drive,
        'params':[
            config.pbp_api_data,
            config.TRAILING_WEEKS
        ],
        'agg_level':'team',
        'type':'warehouse',
        'ball_side':'def',
        'var_category':'scoring_success',
        'var_importance':3,
        'var_counterpart':'points_per_drive'
    },
    'points_per_RZ':{
        'func': get_points_per_RZ,
        'params':[
            config.pbp_api_data,
            config.TRAILING_WEEKS
        ],
        'agg_level':'team',
        'type':'warehouse',
        'ball_side':'off',
        'var_category':'scoring_success',
        'var_importance':3,
        'var_counterpart':'def_points_per_RZ'

    },
    'def_points_per_RZ':{
        'func': get_def_points_per_RZ,
        'params':[
            config.pbp_api_data,
            config.TRAILING_WEEKS
        ],
        'agg_level':'team',
        'type':'warehouse',
        'ball_side':'def',
        'var_category':'scoring_success',
        'var_importance':3,
        'var_counterpart':'points_per_RZ'
    },
    'off_ppg':{
        'func': get_points_per_game,
        'params':[
            config.pbp_api_data,
            config.TRAILING_WEEKS
        ],
        'agg_level':'team',
        'type':'warehouse',
        'ball_side':'off',
        'var_category':'scoring_success',
        'var_importance':3,
        'var_counterpart':'def_ppg'
    },
    'def_ppg':{
        'func': get_def_points_per_game,
        'params':[
            config.pbp_api_data,
            config.TRAILING_WEEKS
        ],
        'agg_level':'team',
        'type':'warehouse',
        'ball_side':'def',
        'var_category':'scoring_success',
        'var_importance':3,
        'var_counterpart':'off_ppg'
    },
    'proportion_leading':{
        'func': get_pct_leading,
        'params':[
            config.pbp_api_data,
            config.TRAILING_WEEKS
        ],
        'agg_level':'team',
        'type':'warehouse',
        'ball_side':'off',
        'var_category':'scoring_success',
        'var_importance':3
    },
    'proportion_leading_three':{
        'func': get_pct_leading_three,
        'params':[
            config.pbp_api_data,
            config.TRAILING_WEEKS
        ],
        'agg_level':'team',
        'type':'warehouse',
        'ball_side':'off',
        'var_category':'scoring_success',
        'var_importance':3
    },
    'off_turnover_rate':{
        'func': get_drives_in_turnover,
        'params':[
            config.pbp_api_data,
            config.TRAILING_WEEKS
        ],
        'agg_level':'team',
        'type':'warehouse',
        'ball_side':'off',
        'var_category':'boom_bust',
        'var_importance':4,
        'var_counterpart':'def_turnover_rate'
    },
    'def_turnover_rate':{
        'func': get_def_drives_in_turnover,
        'params':[
            config.pbp_api_data,
            config.TRAILING_WEEKS
        ],
        'agg_level':'team',
        'type':'warehouse',
        'ball_side':'def',
        'var_category':'boom_bust',
        'var_importance':4,
        'var_counterpart':'off_turnover_rate'
    },
    'actual_off_points':{
        'func': get_actual_game_points,
        'params':[
            config.pbp_api_data,
            config.TRAILING_WEEKS
        ],
        'agg_level':'team',
        'type':'warehouse',
        'ball_side':'off',
        'var_category':'scoring_success',
        'var_importance':1
    },
    'total_off_epa_sum':{
        'func': get_epa_sum,
        'params':[
            config.pbp_api_data,
            config.TRAILING_WEEKS
        ],
        'agg_level':'team',
        'type':'warehouse',
        'ball_side':'off',
        'var_category':'scoring_success',
        'var_importance':2
    },
    'qb_aggr':{
        'func': get_qb_aggr,
        'params':[
            config.ngs['passing'],
            config.TRAILING_WEEKS
        ],
        'agg_level':'team',
        'type':'warehouse',
        'ball_side':'off',
        'var_category':'boom_bust',
        'var_importance':4,
        'var_counterpart':'def_aggr_forced'
    },
    'def_aggr_forced':{
        'func': get_def_qb_aggr,
        'params':[
            config.ngs['passing'],
            config.TRAILING_WEEKS
        ],
        'agg_level':'team',
        'type':'warehouse',
        'ball_side':'def',
        'var_category':'boom_bust',
        'var_importance':4,
        'var_counterpart':'qb_aggr'
    },
    'def_box_stuff_rate':{
        'func': get_def_box_stuff,
        'params':[
            config.ngs['rushing'],
            config.TRAILING_WEEKS
        ],
        'agg_level':'team',
        'type':'warehouse',
        'ball_side':'def',
        'var_category':'boom_bust',
        'var_importance':4
    },
    'def_cushion':{
        'func': get_def_cushion,
        'params':[
            config.ngs['receiving'],
            config.TRAILING_WEEKS
        ],
        'agg_level':'team',
        'type':'warehouse',
        'ball_side':'def',
        'var_category':'boom_bust',
        'var_importance':4
    },
    'def_separation':{
        'func': get_def_separation,
        'params':[
            config.ngs['receiving'],
            config.TRAILING_WEEKS
        ],
        'agg_level':'team',
        'type':'warehouse',
        'ball_side':'def',
        'var_category':'boom_bust',
        'var_importance':4
    },
    'off_avg_throw_dist':{
        'func': get_avg_throw_dist,
        'params':[
            config.ngs['passing'],
            config.TRAILING_WEEKS
        ],
        'agg_level':'team',
        'type':'warehouse',
        'ball_side':'off',
        'var_category':'boom_bust',
        'var_importance':3
    },
    'plays_over_25_yd':{
        'func': get_off_plays_25yd,
        'params':[
            config.pbp_api_data,
            config.TRAILING_WEEKS
        ],
        'agg_level':'team',
        'type':'warehouse',
        'ball_side':'off',
        'var_category':'boom_bust' ,
        'var_importance':3,
        'var_counterpart':'def_plays_over_25_yd'
    },
    'td_over_25_yd':{
        'func': get_off_td_25yd,
        'params':[
            config.pbp_api_data,
            config.TRAILING_WEEKS
        ],
        'agg_level':'team',
        'type':'warehouse',
        'ball_side':'off',
        'var_category':'boom_bust',
        'var_importance':3,
        'var_counterpart':'def_td_over_25_yd'
    },
    'def_plays_over_25_yd':{
        'func': get_def_plays_25yd,
        'params':[
            config.pbp_api_data,
            config.TRAILING_WEEKS
        ],
        'agg_level':'team',
        'type':'warehouse',
        'ball_side':'def',
        'var_category':'boom_bust',
        'var_importance':4,
        'var_counterpart':'plays_over_25_yd'
    },
    'def_td_over_25_yd':{
        'func': get_def_td_25yd,
        'params':[
            config.pbp_api_data,
            config.TRAILING_WEEKS
        ],
        'agg_level':'team',
        'type':'warehouse',
        'ball_side':'def',
        'var_category':'boom_bust' ,
        'var_importance':4,
        'var_counterpart':'td_over_25_yd'
    },
    'team_scr':{
        'func': get_off_scr,
        'params':[
            config.pbp_api_data,
            config.TRAILING_WEEKS
        ],
        'agg_level':'team',
        'type':'warehouse',
        'ball_side':'off',
        'var_category':'play_success',
        'var_importance':5,
        'var_counterpart':'defteam_scr'
    },
    'defteam_scr':{
        'func': get_def_scr_allowed,
        'params':[
            config.pbp_api_data,
            config.TRAILING_WEEKS
        ],
        'agg_level':'team',
        'type':'warehouse',
        'ball_side':'def',
        'var_category':'play_success',
        'var_importance':5,
        'var_counterpart':'team_scr'
    },
    'off_qb_comp':{
        'func': get_qb_comp_rate,
        'params':[
            config.pbp_api_data,
            config.TRAILING_WEEKS
        ],
        'agg_level':'team',
        'type':'warehouse',
        'ball_side':'off',
        'var_category':'play_style',
        'var_importance':2,
        'var_counterpart':'def_qb_comp'
    },
    'def_qb_comp':{
        'func': qb_def_comp_rate_allowed,
        'params':[
            config.pbp_api_data,
            config.TRAILING_WEEKS
        ],
        'agg_level':'team',
        'type':'warehouse',
        'ball_side':'def',
        'var_category':'play_style',
        'var_importance':2,
        'var_counterpart':'off_qb_comp'
    },
    'off_qbhit':{
        'func': qb_hits_allowed_off,
        'params':[
            config.pbp_api_data,
            config.TRAILING_WEEKS
        ],
        'agg_level':'team',
        'type':'warehouse',
        'ball_side':'off',
        'var_category':'boom_bust',
        'var_importance':3,
        'var_counterpart':'def_qbhit'
    },
    'def_qbhit':{
        'func': get_def_qb_hits,
        'params':[
            config.pbp_api_data,
            config.TRAILING_WEEKS
        ],
        'agg_level':'team',
        'type':'warehouse',
        'ball_side':'def',
        'var_category':'boom_bust',
        'var_importance':3,
        'var_counterpart':'off_qbhit'
    },
    'total_season_point_differential':{
        'func': get_season_point_diff,
        'params':[
            config.pbp_api_data
        ],
        'agg_level':'team',
        'type':'warehouse',
        'ball_side':'off',
        'var_category':'full_measure',
        'var_importance':3
    },
    'first_drive_pts_avg':{
        'func': get_first_drive_points_scored,
        'params':[
            config.pbp_api_data,
            config.TRAILING_WEEKS
        ],
        'agg_level':'team',
        'type':'warehouse',
        'ball_side':'off',
        'var_category':'coaching_aptitude',
        'var_importance':4,
        'var_counterpart':'first_drive_pts_avg_allowed'
    },
    'first_drive_pts_avg_allowed':{
        'func': get_def_first_drive_points_allowed,
        'params':[
            config.pbp_api_data,
            config.TRAILING_WEEKS
        ],
        'agg_level':'team',
        'type':'warehouse',
        'ball_side':'def',
        'var_category':'coaching_aptitude',
        'var_importance':4,
        'var_counterpart':'first_drive_pts_avg'
    },
    'yac_air_yards':{
        'func': get_yac_air_yards,
        'output_columns': ['trailing_pct_air_yards','trailing_pct_yac'],
        'params':[
            config.pbp_api_data,
            config.TRAILING_WEEKS
        ],
        'agg_level':'team',
        'type':'warehouse',
        'ball_side':'off',
        'var_category':'play_style',
        'var_importance':3
    },
    'h2_first_drive_pts_avg':{
        'func': get_2h_first_drive_points_scored,
        'params':[
            config.pbp_api_data,
            config.TRAILING_WEEKS
        ],
        'agg_level':'team',
        'type':'warehouse',
        'ball_side':'off',
        'var_category':'coaching_aptitude',
        'var_importance':3,
        'var_counterpart':'h2_first_drive_pts_avg_allowed'
    },
    'h2_first_drive_pts_avg_allowed':{
        'func': get_2h_def_first_drive_points_allowed,
        'params':[
            config.pbp_api_data,
            config.TRAILING_WEEKS
        ],
        'agg_level':'team',
        'type':'warehouse',
        'ball_side':'def',
        'var_category':'coaching_aptitude',
        'var_importance':3,
        'var_counterpart':'h2_first_drive_pts_avg'
    }
    }

    return FUNCTION_CATALOG