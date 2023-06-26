"""
Module to assess player and team performance from
play by play data.

"""

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
    # At what level are we AGGREGATING offensive performance
    off_gb_cols = ['posteam','defteam','position','player_name'],
    gb_cols_z = ['season','position'], # What is aggregate level for z score
    # Matched up against what level on defense to AGGREGATE to
    def_gb_cols = ['posteam','defteam','position'],
    perf_cols = ['epa'], # What are we assessing for ELO
    off_appearance_columns = ['position','rusher_player_id','season'],
    def_appearance_columns = ['defteam','position','season']


):
    """Function Docstring"""
    off_gb_cols = off_gb_cols + [player_id_col]

    epa_dataset = pbp_api[BASE_DATA_COLS + META_COLUMNS + [player_id_col]]
    mask = epa_dataset['play_type'].isin( play_types)
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

    position_mask = epa_dataset['position'].isin(position_filter)
    epa_dataset = epa_dataset[position_mask]
    epa_dataset['defteam'] = epa_dataset.apply(
        lambda x: x['home_team'] if x['away_team']==x['posteam'] else x['away_team'],
        axis = 1
        )
    off_epa_dataset = epa_dataset.groupby(order_cols + off_gb_cols)[perf_cols].sum().reset_index()
    for col in perf_cols:
        off_epa_dataset[f'z_{col}'] = off_epa_dataset.groupby(order_cols + gb_cols_z)[col]\
            .transform(lambda x : zscore(x))
    off_epa_dataset.drop(perf_cols, axis = 1, inplace = True)

    def_epa_dataset = epa_dataset.groupby(order_cols + def_gb_cols)[perf_cols].sum().reset_index()
    for col in perf_cols:
        def_epa_dataset[f'z_{col}'] = def_epa_dataset.groupby(order_cols + gb_cols_z)[col]\
            .transform(lambda x : zscore(x))
    def_epa_dataset.drop(perf_cols, axis = 1, inplace = True)
    def_epa_dataset['def_appearance'] = def_epa_dataset.sort_values(order_cols)\
        .groupby(def_appearance_columns).cumcount() + 1
    off_epa_dataset['off_appearance'] = off_epa_dataset.sort_values(order_cols)\
        .groupby(off_appearance_columns).cumcount() + 1

    return off_epa_dataset, def_epa_dataset

# Pipeline functions for modular tasks
# Functions are built off api_data and other advanced stats generated in setup.py

def get_yards_per_rush(api_data, trailing_weeks=5):
    '''
    Yards per rush at team level
    '''
    yy = pd.DataFrame(api_data[api_data['play_type']=='run'].
                      groupby(['season','week','posteam'], as_index=False).agg({
                          'yards_gained':'sum', 'play_counter':'size'
                      })).sort_values(by=['season','posteam','week'])

    yy['yards_per_carry'] = (yy['yards_gained']/yy['play_counter'])
    yy = yy.reset_index(drop=True)
    yy['calc_ypc']=pd.DataFrame(yy.groupby(['season','posteam'], as_index=False)['yards_per_carry'].rolling(trailing_weeks).mean())['yards_per_carry']
    # df['week'] = df.groupby(['season','posteam']).cumcount() +1
    return(yy[['season','week','posteam','calc_ypc']].rename(columns={'calc_ypc':'yards_per_carry', 'posteam':'team'}))

def get_yards_per_pass(api_data, trailing_weeks=5):
    '''
    Yards per pass at team level
    '''
    yy = pd.DataFrame(api_data[api_data['play_type']=='pass'].
                      groupby(['season','week','posteam'], as_index=False).agg({
                          'yards_gained':'sum', 'play_counter':'size'
                      })).sort_values(by=['season','posteam','week'])

    yy['yards_per_pass'] = (yy['yards_gained']/yy['play_counter'])
    yy = yy.reset_index(drop=True)
    yy['calc_ypc']=pd.DataFrame(yy.groupby(['season','posteam'], as_index=False)['yards_per_pass'].rolling(trailing_weeks).mean())['yards_per_pass']
    # df['week'] = df.groupby(['season','posteam']).cumcount() +1
    return(yy[['season','week','posteam','calc_ypc']].rename(columns={'calc_ypc':'yards_per_pass', 'posteam':'team'}))

def get_epa_per_rush(api_data, trailing_weeks = 5):
    '''
    EPA per rush at team level
    '''
    mid_df = api_data[api_data['play_type'] == 'run'].groupby(by=['season','week','posteam'], as_index=False)['epa'].mean().sort_values(by=['season','posteam','week'])
    output_df = mid_df.assign(epa_per_rush = mid_df.groupby(['season','posteam'], as_index=False)['epa'].rolling(trailing_weeks).sum()['epa'])[['season','week','posteam','epa_per_rush']]
    return(output_df.reset_index(drop=True).rename(columns={'posteam':'team'}))

def get_epa_per_pass(api_data, trailing_weeks = 5):
    '''
    EPA per pass at team level
    '''
    mid_df = api_data[api_data['play_type'] == 'pass'].groupby(by=['season','week','posteam'], as_index=False)['epa'].mean().sort_values(by=['season','posteam','week'])
    output_df = mid_df.assign(epa_per_pass = mid_df.groupby(['season','posteam'], as_index=False)['epa'].rolling(trailing_weeks).sum()['epa'])[['season','week','posteam','epa_per_pass']]
    return(output_df.reset_index(drop=True).rename(columns={'posteam':'team'}))

def get_offense_epa(api_data, trailing_weeks = 5):
    '''
    Overall EPA (rush and pass) at team level
    '''
    mid_df = api_data[api_data['play_type'].isin(['run','pass'])].groupby(['season','week','posteam'], as_index = False)['epa'].mean().sort_values(['season','posteam','week'])
    output_df = mid_df.assign(off_epa = mid_df.groupby(['season','posteam'], as_index = False)['epa'].rolling(trailing_weeks).mean()['epa'])[['season','week','posteam','off_epa']]
    return(output_df.reset_index(drop=True).rename(columns={'posteam':'team'}))


def get_pct_pass(api_data, trailing_weeks = 5):
    '''
    Percentage of pass plays at team level
    '''
    run_pass_df = api_data[(api_data['play_type']=='pass')|(api_data['play_type']=='run')].groupby(['season','week','posteam'], as_index=False)['play_type'].value_counts(normalize=True).sort_values(by=['season','posteam','week'])
    just_pass = run_pass_df[run_pass_df['play_type']=='pass']
    just_pass = just_pass.assign(pct_pass = just_pass.
                     groupby(['season','posteam'], as_index=False)['proportion'].
                     rolling(trailing_weeks).mean()['proportion'])[['season','week','posteam','pct_pass']].reset_index(drop=True)
    return(just_pass.rename(columns={'posteam':'team'}))

def get_pct_run(api_data, trailing_weeks=5):
    '''
    Percentage of run plays at team level
    '''
    run_pass_df = api_data[(api_data['play_type']=='pass')|(api_data['play_type']=='run')].groupby(['season','week','posteam'], as_index=False)['play_type'].value_counts(normalize=True).sort_values(by=['season','posteam','week'])
    just_run = run_pass_df[run_pass_df['play_type']=='run']
    just_run = just_run.assign(pct_run = just_run.
                     groupby(['season','posteam'], as_index=False)['proportion'].
                     rolling(trailing_weeks).mean()['proportion'])[['season','week','posteam','pct_run']].reset_index(drop=True)

    return(just_run.rename(columns={'posteam':'team'}))

def get_team_hhi(api_data, trailing_weeks = 5):
    '''
    Calculate HHI (Proprietary metric that does ___ at team level)
    '''
    player_yards = api_data[(api_data['play_type']=='run')|(api_data['play_type']=='pass')]\
        .groupby(['season','week','posteam','fantasy_player_name'], as_index=False)['yards_gained']\
            .sum().rename(columns={'yards_gained':'player_yards_gained'})
    team_yards = api_data[(api_data['play_type']=='run')|(api_data['play_type']=='pass')]\
        .groupby(['season','week','posteam'], as_index=False)['yards_gained']\
            .sum()
    merged_df = pd.merge(player_yards, team_yards, on=['season','week','posteam'])
    merged_df['percent_team_yards'] = merged_df['player_yards_gained']/merged_df['yards_gained']
    merged_df['percent_team_yards_sq'] = merged_df['percent_team_yards']**2
    merged_df = merged_df.groupby(['season','week','posteam'], as_index=False)['percent_team_yards_sq']\
        .sum().sort_values(by=['season','posteam','week'])
    output_df = merged_df.assign(team_HHI = merged_df.groupby(['season','posteam'], as_index=False)\
                                 ['percent_team_yards_sq'].rolling(trailing_weeks)\
                                    .mean()\
                                        .rename(columns={'percent_team_yards_sq':'HHI'})['HHI'])\
                                            [['season','week','posteam','team_HHI']]
    output_df = output_df.rename(columns={'posteam':'team'}).reset_index(drop=True)

    return(output_df)

