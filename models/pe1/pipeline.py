"""Module Docstring"""
import sys
import os

REPO_NAME = 'sewer-nfl'
CWD = str(os.getcwd())
REPO_DIR = CWD[:CWD.find(REPO_NAME)+len(REPO_NAME)]
sys.path.insert(0,REPO_DIR)
import pandas as pd
import nfl_data_py as nflreadr
from warehouse.pipelines.pbp.epa import pre_elo_epa, META_COLUMNS
from warehouse.utilities.elo import calculate_elo_metric

MIN_YEAR = 2010
MAX_YEAR = 2022
MAX_WEEK = 18

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
    # At what level are we AGGREGATING offensive performance
    off_gb_cols = ['posteam','defteam','position','player_name'],
    # What is aggregate level for z score
    gb_cols_z = ['season','position'],
    # Matched up against what level on defense to AGGREGATE to
    def_gb_cols = ['posteam','defteam','position'],
    # What are we assessing for ELO
    perf_cols = ['epa'],
    off_appearance_columns = ['position','receiver_player_id','season'],
    def_appearance_columns = ['position','defteam','season']
)
receiving_elo_df = calculate_elo_metric(
    input_off_data = receiving_off_epa,
    input_def_data = receiving_def_epa,
    order_cols = ['season','week'],
    off_gb_cols = ['posteam','defteam','position','player_name'],
    # At what level are we AGGREGATING offensive performance
    def_gb_cols = ['posteam','defteam','position'],
    # Matched up against what level on defense to AGGREGATE to
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
    off_gb_cols = ['posteam','defteam','position','player_name'],
    # At what level are we AGGREGATING offensive performance
    gb_cols_z = ['season','position'],
    # What is aggregate level for z score
    def_gb_cols = ['posteam','defteam','position'],
    # Matched up against what level on defense to AGGREGATE to
    perf_cols = ['epa'],
    # What are we assessing for ELO
    off_appearance_columns = ['position','rusher_player_id','season'],
    def_appearance_columns = ['position','defteam','season']
)
rushing_elo_df = calculate_elo_metric(
    input_off_data = rushing_off_epa,
    input_def_data = rushing_def_epa,
    order_cols = ['season','week'],
    off_gb_cols = ['posteam','defteam','position','player_name'],
    # At what level are we AGGREGATING offensive performance
    def_gb_cols = ['posteam','defteam','position'],
    # Matched up against what level on defense to AGGREGATE to
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
    # At what level are we AGGREGATING offensive performance
    off_gb_cols = ['posteam','defteam','position','player_name'],
    gb_cols_z = ['season','position'], # What is aggregate level for z score
    def_gb_cols = ['posteam','defteam','position'],
    perf_cols = ['epa'], # What are we assessing for ELO
    off_appearance_columns = ['position','passer_player_id','season'],
    def_appearance_columns = ['position','defteam','season']
)
passing_elo_df = calculate_elo_metric(
    input_off_data = passing_off_epa,
    input_def_data = passing_def_epa,
    order_cols = ['season','week'],
    # At what level are we AGGREGATING offensive performance
    off_gb_cols = ['posteam','defteam','position','player_name'],
    # Matched up against what level on defense to AGGREGATE to
    def_gb_cols = ['posteam','defteam','position'],
    off_lookup_values = ['position','passer_player_id'],
    def_lookup_values = ['position','defteam'],
    off_perf_col = 'z_epa_x',
    def_perf_col = 'z_epa_y',
    elo_multiplier = 5,
    elo_power = 1.7,
    elo_base = 2000
)

# Aggregate final DFs to model level
agg_passing = passing_elo_df.groupby(['season','week','posteam','defteam','position'])\
    .head(1).groupby(['season','week','posteam','defteam','position'])[['off_elo','def_elo']]\
        .mean().reset_index()
df = pd.pivot_table(
    agg_passing,
    values = ['off_elo','def_elo'],
    columns = 'position',
    index = ['season','week','posteam','defteam']
).reset_index()
df.columns = [x[0] if x[1]=='' else f'{x[0]}_{x[1]}_pass' for x in df.columns]
agg_passing = df
agg_rushing = rushing_elo_df.groupby(['season','week','posteam','defteam','position'])\
    .head(2).groupby(['season','week','posteam','defteam','position'])[['off_elo','def_elo']]\
        .mean().reset_index()
df = pd.pivot_table(
    agg_rushing,
    values = ['off_elo','def_elo'],
    columns = 'position',
    index = ['season','week','posteam','defteam']
).reset_index()
df.columns = [x[0] if x[1]=='' else f'{x[0]}_{x[1]}_rush' for x in df.columns]
agg_rushing = df
agg_receiving = receiving_elo_df.groupby(['season','week','posteam','defteam','position'])\
    .head(3).groupby(['season','week','posteam','defteam','position'])[['off_elo','def_elo']]\
        .mean().reset_index()
df = pd.pivot_table(
    agg_receiving,
    values = ['off_elo','def_elo'],
    columns = 'position',
    index = ['season','week','posteam','defteam']
).reset_index()
df.columns = [x[0] if x[1]=='' else f'{x[0]}_{x[1]}_rec' for x in df.columns]
agg_receiving = df

game_data = pbp_api_data[META_COLUMNS].drop('posteam',axis = 1).drop_duplicates()

all_elo = agg_passing.merge(
    agg_receiving,
    on = ['season','week','posteam','defteam'],
    how = 'left'
).merge(
    agg_rushing,
    on = ['season','week','posteam','defteam'],
    how = 'left'
)

temp_df = pd.concat([game_data.merge(
    all_elo,
    how = 'left',
    left_on = ['season','week','home_team','away_team'],
    right_on = ['season','week','posteam','defteam']
),
game_data.merge(
    all_elo,
    how = 'left',
    left_on = ['season','week','away_team','home_team'],
    right_on = ['season','week','posteam','defteam']
)]).sort_values(['season','week','home_team'])

model_df = temp_df[temp_df['posteam']==temp_df['home_team']].merge(
    temp_df[temp_df['posteam']==temp_df['away_team']],
    on = ['game_id','old_game_id','season','week','home_team','away_team','spread_line']
)
