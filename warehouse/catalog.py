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



REQUIREMENTS_CATALOG = {
    'pbp_api_data':{
        'type' : 'dataset',
        'value' : setup_pbp
    }
}

FUNCTION_CATALOG = {
    'players_involved':{
        'func': players_involved
    }
}