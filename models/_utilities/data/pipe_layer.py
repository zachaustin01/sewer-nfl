"""

Module to build dataset from config file.
Config attributes should already be imported before running the pipe layer

"""
import sys
import os
import copy

REPO_NAME = 'sewer-nfl'
CWD = str(os.getcwd())
REPO_DIR = CWD[:CWD.find(REPO_NAME)+len(REPO_NAME)]
sys.path.insert(0,REPO_DIR)

from warehouse.pipelines.game_summary.game_summary import game_outcomes
from warehouse.catalog import build_catalog

def build_training_dataset(
        config,
        auto_filter = .25, # Allowable percentage of columns that can be missing
        cache = True,
        cache_path = 'cache/training_datasets',
        cache_name = 'v1'
):
    '''

    Function to build training dataset using a multitude of model parameters
    Inputs:
    - config: Configuration class, unique at model level but containing variables critical for
              FEATURE_CATALOG creation

    '''

    # Pull ALL data at team and week level
    gd = game_outcomes(config.pbp_api_data)
    FUNCTION_CATALOG = build_catalog(config)
    catalog_results = [
    FUNCTION_CATALOG[key]['func']\
        (*FUNCTION_CATALOG[key]['params']) \
            for key in FUNCTION_CATALOG.keys()
    ]
    # Iterate over results and build final df
    df = catalog_results[0]
    for i in range(1,len(catalog_results)):
        on_cols = ['season','week','team']
        left_on = [oc for oc in on_cols if oc in df.columns]
        right_on = [oc for oc in on_cols if oc in catalog_results[i].columns]
        # If right missing left, reduce
        if len(right_on) < len(left_on):
            left_on = copy.deepcopy(right_on)
        if 'defteam' in catalog_results[i].columns:
            left_on.append('team')
            right_on.append('defteam')
        df = df.merge(catalog_results[i], how = "left", left_on = left_on, right_on = right_on)

    t_cols = [c for c in df.columns if c not in gd.columns]
    res = gd
    for side in ['home','away']:
        res = res.merge(df,
                how = "left",
                left_on = ['season','week',f'{side}_team'],
                right_on = ['season','week','team'])
        r_dict = {key:f'{side}_{key}' for key in t_cols}
        res.rename(columns = r_dict, inplace=True)

    # Add filter
    res['missing_N'] = res.isnull().sum(axis=1) / len(res.columns)
    res = res[res['missing_N'] < auto_filter]
    if cache:
        res.to_csv(f'{REPO_DIR}/{cache_path}/{cache_name}.csv')

    return res

