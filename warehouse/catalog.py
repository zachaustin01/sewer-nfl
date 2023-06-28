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

# Import config
from warehouse.config import *


FUNCTION_CATALOG = {
    # Performance
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
    }

}