from scipy.stats import zscore
import pandas as pd

#  Functions to gather EPA metrics from play by play data

R_COLUMNS = [
    'season',
    'week',
    'team',
    'position',
    'player_name',
    'player_id'
]

META_COLUMNS = [
    'game_id',
    'old_game_id',
    'season',
    'week',
    'home_team',
    'away_team',
    'posteam',
    'spread_line'
]

BASE_DATA_COLS = [
    'play_id',
    'play_type',
    'yards_gained',
    'epa'
]

def pre_elo_epa(
    pbp_api,
    roster_api,
    position_filter = ['QB','RB','WR','TE'],
    play_types = ['run'],
    player_id_col = 'rusher_player_id',
    order_cols = ['season','week'], # Used in ELO function
    off_gb_cols = ['posteam','defteam','position','player_name'], # At what level are we AGGREGATING offensive performance
    gb_cols_z = ['season','position'], # What is aggregate level for z score
    def_gb_cols = ['posteam','defteam','position'], # Matched up against what level on defense to AGGREGATE to
    perf_cols = ['epa'], # What are we assessing for ELO
    off_appearance_columns = ['position','rusher_player_id','season'],
    def_appearance_columns = ['defteam','position','season']


):
    off_gb_cols = off_gb_cols + [player_id_col]

    epa_dataset = pbp_api[BASE_DATA_COLS + META_COLUMNS + [player_id_col]]
    mask = (epa_dataset['play_type'].isin( play_types))
    epa_dataset = epa_dataset.loc[mask].merge(
        roster_api[[
            'player_name',
            'position',
            'player_id'
        ]],
        left_on = [player_id_col],
        right_on = ['player_id'],
        how = 'inner'
    ).drop_duplicates()

    position_mask = (epa_dataset['position'].isin(position_filter))
    epa_dataset = epa_dataset[position_mask]
    epa_dataset['defteam'] = epa_dataset.apply(
        lambda x: x['home_team'] if x['away_team']==x['posteam'] else x['away_team'], 
        axis = 1
        )
    off_epa_dataset = epa_dataset.groupby(order_cols + off_gb_cols)[perf_cols].sum().reset_index()
    for col in perf_cols:
        off_epa_dataset[f'z_{col}'] = off_epa_dataset.groupby(order_cols + gb_cols_z)[col].transform(lambda x : zscore(x))
    off_epa_dataset.drop(perf_cols, axis = 1, inplace = True)

    def_epa_dataset = epa_dataset.groupby(order_cols + def_gb_cols)[perf_cols].sum().reset_index()
    for col in perf_cols:
        def_epa_dataset[f'z_{col}'] = def_epa_dataset.groupby(order_cols + gb_cols_z)[col].transform(lambda x : zscore(x))
    def_epa_dataset.drop(perf_cols, axis = 1, inplace = True)
    def_epa_dataset['def_appearance'] = def_epa_dataset.sort_values(order_cols).groupby(def_appearance_columns).cumcount() + 1
    off_epa_dataset['off_appearance'] = off_epa_dataset.sort_values(order_cols).groupby(off_appearance_columns).cumcount() + 1
    
    return off_epa_dataset, def_epa_dataset