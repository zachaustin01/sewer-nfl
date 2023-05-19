MIN_YEAR = 2010
MAX_YEAR = 2022
MAX_WEEK = 18

REPO_NAME = 'sewer-nfl'
import sys, os
cwd = str(os.getcwd())
repo_dir = cwd[:cwd.find(REPO_NAME)+len(REPO_NAME)]
sys.path.insert(0,repo_dir)

import pandas as pd
import nfl_data_py as nflreadr
from warehouse.pipelines.pbp.epa import pre_elo_epa
from warehouse.utilities.elo import calculate_elo_metric

pbp_api_data = nflreadr.import_pbp_data(range(MIN_YEAR,MAX_YEAR + 1))
pbp_api_data = pbp_api_data[pbp_api_data['week']<=MAX_WEEK]

roster_api_data = nflreadr.import_rosters(years=range(MIN_YEAR, MAX_YEAR + 1))

# Receiving

receiving_off_epa , receiving_def_epa = pre_elo_epa(
    pbp_api = pbp_api_data,
    roster_api = roster_api_data, 
    position_filter = ['RB','WR','TE'],
    play_types = ['pass'],
    player_id_col = 'receiver_player_id',
    order_cols = ['season','week'],
    off_gb_cols = ['posteam','defteam','position','player_name'], # At what level are we AGGREGATING offensive performance
    gb_cols_z = ['season','position'], # What is aggregate level for z score
    def_gb_cols = ['posteam','defteam','position'], # Matched up against what level on defense to AGGREGATE to
    perf_cols = ['epa'], # What are we assessing for ELO
    off_appearance_columns = ['position','receiver_player_id','season'],
    def_appearance_columns = ['position','defteam','season']
)
receiving_elo_df = calculate_elo_metric(
    input_off_data = receiving_off_epa,
    input_def_data = receiving_def_epa,
    order_cols = ['season','week'],
    off_gb_cols = ['posteam','defteam','position','player_name'], # At what level are we AGGREGATING offensive performance
    def_gb_cols = ['posteam','defteam','position'], # Matched up against what level on defense to AGGREGATE to
    off_lookup_values = ['position','receiver_player_id'],
    def_lookup_values = ['position','defteam'],
    off_perf_col = 'z_epa_x',
    def_perf_col = 'z_epa_y',
    elo_multiplier = 5,
    elo_power = 1.7,
    elo_base = 2000
)

# Rushing

rushing_off_epa , rushing_def_epa = pre_elo_epa(
    pbp_api = pbp_api_data,
    roster_api = roster_api_data, 
    position_filter = ['QB','RB','WR','TE'],
    play_types = ['run'],
    player_id_col = 'rusher_player_id',
    order_cols = ['season','week'],
    off_gb_cols = ['posteam','defteam','position','player_name'], # At what level are we AGGREGATING offensive performance
    gb_cols_z = ['season','position'], # What is aggregate level for z score
    def_gb_cols = ['posteam','defteam','position'], # Matched up against what level on defense to AGGREGATE to
    perf_cols = ['epa'], # What are we assessing for ELO
    off_appearance_columns = ['position','rusher_player_id','season'],
    def_appearance_columns = ['position','defteam','season']
)
rushing_elo_df = calculate_elo_metric(
    input_off_data = rushing_off_epa,
    input_def_data = rushing_def_epa,
    order_cols = ['season','week'],
    off_gb_cols = ['posteam','defteam','position','player_name'], # At what level are we AGGREGATING offensive performance
    def_gb_cols = ['posteam','defteam','position'], # Matched up against what level on defense to AGGREGATE to
    off_lookup_values = ['position','rusher_player_id'],
    def_lookup_values = ['position','defteam'],
    off_perf_col = 'z_epa_x',
    def_perf_col = 'z_epa_y',
    elo_multiplier = 5,
    elo_power = 1.7,
    elo_base = 2000
)

# Passing

passing_off_epa , passing_def_epa = pre_elo_epa(
    pbp_api = pbp_api_data,
    roster_api = roster_api_data, 
    position_filter = ['QB'],
    play_types = ['pass'],
    player_id_col = 'passer_player_id',
    order_cols = ['season','week'],
    off_gb_cols = ['posteam','defteam','position','player_name'], # At what level are we AGGREGATING offensive performance
    gb_cols_z = ['season','position'], # What is aggregate level for z score
    def_gb_cols = ['posteam','defteam','position'], # Matched up against what level on defense to AGGREGATE to
    perf_cols = ['epa'], # What are we assessing for ELO
    off_appearance_columns = ['position','passer_player_id','season'],
    def_appearance_columns = ['position','defteam','season']
)
passing_elo_df = calculate_elo_metric(
    input_off_data = passing_off_epa,
    input_def_data = passing_def_epa,
    order_cols = ['season','week'],
    off_gb_cols = ['posteam','defteam','position','player_name'], # At what level are we AGGREGATING offensive performance
    def_gb_cols = ['posteam','defteam','position'], # Matched up against what level on defense to AGGREGATE to
    off_lookup_values = ['position','passer_player_id'],
    def_lookup_values = ['position','defteam'],
    off_perf_col = 'z_epa_x',
    def_perf_col = 'z_epa_y',
    elo_multiplier = 5,
    elo_power = 1.7,
    elo_base = 2000
)