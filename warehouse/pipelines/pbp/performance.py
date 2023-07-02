"""
Module to assess player and team performance from
play by play data.

"""

from scipy.stats import zscore
import pandas as pd
import numpy as np
from cacheout import Cache
cache = Cache()

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

@cache.memoize()
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

@cache.memoize()
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

@cache.memoize()
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

@cache.memoize()
def get_epa_per_rush(api_data, trailing_weeks = 5):
    '''
    EPA per rush at team level
    '''
    mid_df = api_data[api_data['play_type'] == 'run'].groupby(by=['season','week','posteam'], as_index=False)['epa'].mean().sort_values(by=['season','posteam','week'])
    output_df = mid_df.assign(epa_per_rush = mid_df.groupby(['season','posteam'], as_index=False)['epa'].rolling(trailing_weeks).sum()['epa'])[['season','week','posteam','epa_per_rush']]
    return(output_df.reset_index(drop=True).rename(columns={'posteam':'team'}))

@cache.memoize()
def get_epa_per_pass(api_data, trailing_weeks = 5):
    '''
    EPA per pass at team level
    '''
    mid_df = api_data[api_data['play_type'] == 'pass'].groupby(by=['season','week','posteam'], as_index=False)['epa'].mean().sort_values(by=['season','posteam','week'])
    output_df = mid_df.assign(epa_per_pass = mid_df.groupby(['season','posteam'], as_index=False)['epa'].rolling(trailing_weeks).sum()['epa'])[['season','week','posteam','epa_per_pass']]
    return(output_df.reset_index(drop=True).rename(columns={'posteam':'team'}))

@cache.memoize()
def get_offense_epa(api_data, trailing_weeks = 5):
    '''
    Overall EPA (rush and pass) at team level
    '''
    mid_df = api_data[api_data['play_type'].isin(['run','pass'])].groupby(['season','week','posteam'], as_index = False)['epa'].mean().sort_values(['season','posteam','week'])
    output_df = mid_df.assign(off_epa = mid_df.groupby(['season','posteam'], as_index = False)['epa'].rolling(trailing_weeks).mean()['epa'])[['season','week','posteam','off_epa']]
    return(output_df.reset_index(drop=True).rename(columns={'posteam':'team'}))

@cache.memoize()
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

@cache.memoize()
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

@cache.memoize()
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

@cache.memoize()
def get_hhi_by_type(api_data, trailing_weeks = 5,
                    play_type = "pass" # can be pass or run
                    ):


    player_yards = api_data[(api_data['play_type']==play_type)]\
        .groupby(['season','week','posteam','fantasy_player_name'], as_index=False)['yards_gained']\
            .sum().rename(columns={'yards_gained':'player_yards_gained'})

    team_yards = api_data[(api_data['play_type']==play_type)]\
        .groupby(['season','week','posteam'], as_index=False)['yards_gained']\
            .sum()

    merged_df = pd.merge(player_yards, team_yards, on=['season','week','posteam'])

    merged_df['percent_team_yards'] = merged_df['player_yards_gained']/merged_df['yards_gained']
    merged_df['percent_team_yards_sq'] = merged_df['percent_team_yards']**2

    merged_df = merged_df.groupby(['season','week','posteam'], as_index=False)['percent_team_yards_sq']\
        .sum().sort_values(by=['season','posteam','week'])

    if play_type == 'run': play_type = 'rush'
    c_name = f'team_{play_type}ing_HHI'

    output_df = merged_df.assign(team_passing_HHI = merged_df\
                                 .groupby(['season','posteam'], as_index=False)['percent_team_yards_sq']\
                                    .rolling(trailing_weeks).mean().rename(columns={'percent_team_yards_sq':'HHI'})\
                                        ['HHI'])[['season','week','posteam',c_name]]

    return(output_df.reset_index(drop=True).rename(columns={'posteam':'team'}))

@cache.memoize()
def get_def_yards_per_pass(api_data, trailing_weeks = 5):

    yy = pd.DataFrame(api_data[api_data['play_type']=='pass'].
                  groupby(['season','week','defteam'], as_index=False).agg({
                      'yards_gained':'sum', 'play_counter':'size'
                  })).sort_values(by=['season','defteam','week'])


    yy['def_yards_per_pass'] = (yy['yards_gained']/yy['play_counter'])

    yy = yy.reset_index(drop=True)

    yy['calc_ypc']=pd.DataFrame(yy.groupby(['season','defteam'], as_index=False)['def_yards_per_pass'].rolling(trailing_weeks).mean())['def_yards_per_pass']
    # df['week'] = df.groupby(['season','posteam']).cumcount() +1

    output_df = yy[['season','week','defteam','calc_ypc']].rename(columns={'calc_ypc':'def_yards_per_pass'})

    # output_df.groupby(['season','defteam'], as_index=False)['def_yards_per_pass'].mean().sort_values(by=['def_yards_per_pass'])

    return(output_df.reset_index(drop=True).rename(columns = {'defteam':'team'}))


# yards per rush on defense

@cache.memoize()
def get_def_yards_per_rush(api_data, trailing_weeks = 5):

    yy = pd.DataFrame(api_data[api_data['play_type']=='run'].
                  groupby(['season','week','defteam'], as_index=False).agg({
                      'yards_gained':'sum', 'play_counter':'size'
                  })).sort_values(by=['season','defteam','week'])


    yy['yards_per_rush'] = (yy['yards_gained']/yy['play_counter'])

    yy = yy.reset_index(drop=True)

    yy['calc_ypc']=pd.DataFrame(yy.groupby(['season','defteam'], as_index=False)['yards_per_rush'].rolling(trailing_weeks).mean())['yards_per_rush']
    # df['week'] = df.groupby(['season','posteam']).cumcount() +1

    output_df = yy[['season','week','defteam','calc_ypc']].rename(columns={'calc_ypc':'def_yards_per_rush'})

    output_df.groupby(['season','defteam'], as_index=False)['def_yards_per_rush'].mean().sort_values(by=['def_yards_per_rush'])

    return(output_df.reset_index(drop=True).rename(columns = {'defteam':'team'}))

# defensive EPA per pass allowed
@cache.memoize()
def get_def_epa_per_pass(api_data, trailing_weeks = 5):

    epa_df = api_data[api_data['play_type']=="pass"].groupby(['season','week','defteam'], as_index=False)['epa'].mean().sort_values(by=['season','defteam','week'])

    epa_df = epa_df.assign(def_pass_epa = epa_df.groupby(['season','defteam'], as_index=False)['epa'].rolling(trailing_weeks).mean()['epa'])[['season','week','defteam','def_pass_epa']]

    # epa_df.groupby(['season','defteam'], as_index=False)['def_pass_epa'].mean().sort_values(by=['def_pass_epa'], ascending=False)

    return(epa_df.reset_index(drop=True).rename(columns = {'defteam':'team'}))


# defensive EPA per rush allowed
@cache.memoize()
def get_def_epa_per_rush(api_data, trailing_weeks = 5):

    epa_df = api_data[api_data['play_type']=="run"].groupby(['season','week','defteam'], as_index=False)['epa'].mean().sort_values(by=['season','defteam','week'])

    epa_df = epa_df.assign(def_rush_epa = epa_df.groupby(['season','defteam'], as_index=False)['epa'].rolling(trailing_weeks).mean()['epa'])[['season','week','defteam','def_rush_epa']]

    return(epa_df.reset_index(drop=True).rename(columns = {'defteam':'team'}))


# average points per drive
@cache.memoize()
def get_points_per_drive(api_data, trailing_weeks = 5):

    drive_results = api_data[['season','week','posteam','fixed_drive','fixed_drive_result']].drop_duplicates().reset_index(drop=True)

    drive_results['resulting_points'] = [3 if a == 'Field goal' else 7 if a == 'Touchdown' else 0 for a in drive_results['fixed_drive_result']]

    grouped_drive_results = drive_results.groupby(['season','week','posteam'], as_index=False)['resulting_points'].mean().sort_values(by=['season','posteam','week'])

    grouped_drive_results = grouped_drive_results.assign(points_per_drive = grouped_drive_results.groupby(['season','posteam'], as_index=False)['resulting_points'].rolling(trailing_weeks).mean()['resulting_points'])[['season','week','posteam','points_per_drive']]

    return(grouped_drive_results.reset_index(drop=True).rename(columns = {'posteam':'team'}))


# average points per drive allowed
@cache.memoize()
def get_def_points_per_drive(api_data, trailing_weeks = 5):

    drive_results = api_data[['season','week','defteam','fixed_drive','fixed_drive_result']].drop_duplicates().reset_index(drop=True)

    drive_results['resulting_points'] = [3 if a == 'Field goal' else 7 if a == 'Touchdown' else 0 for a in drive_results['fixed_drive_result']]

    grouped_drive_results = drive_results.groupby(['season','week','defteam'], as_index=False)['resulting_points'].mean().sort_values(by=['season','defteam','week'])

    grouped_drive_results = grouped_drive_results.assign(def_points_per_drive = grouped_drive_results.groupby(['season','defteam'], as_index=False)['resulting_points'].rolling(trailing_weeks).mean()['resulting_points'])[['season','week','defteam','def_points_per_drive']]

    return(grouped_drive_results.reset_index(drop=True).rename(columns = {'defteam':'team'}))

# points per RZ trip
@cache.memoize()
def get_points_per_RZ(api_data, trailing_weeks = 5):

    api_data = api_data[api_data['yardline_100']<=20]

    drive_results = api_data[['season','week','posteam','fixed_drive','fixed_drive_result']].drop_duplicates().reset_index(drop=True)

    drive_results['resulting_points'] = [3 if a == 'Field goal' else 7 if a == 'Touchdown' else 0 for a in drive_results['fixed_drive_result']]

    grouped_drive_results = drive_results.groupby(['season','week','posteam'], as_index=False)['resulting_points'].mean().sort_values(by=['season','posteam','week'])

    join_data = api_data[['season','week','posteam']].drop_duplicates().reset_index(drop=True).sort_values(by=['season','posteam','week'])

    join_data.merge(grouped_drive_results, how='left', on=['season','week','posteam']).rename(columns={'posteam':'team'}).replace(np. nan,0)

    grouped_drive_results = grouped_drive_results.assign(points_per_RZ = grouped_drive_results.groupby(['season','posteam'], as_index=False)['resulting_points'].rolling(trailing_weeks).mean()['resulting_points'])[['season','week','posteam','points_per_RZ']]

    return(grouped_drive_results.reset_index(drop=True).rename(columns = {'posteam':'team'}))

# points per RZ trip
@cache.memoize()
def get_def_points_per_RZ(api_data, trailing_weeks = 5):

    api_data = api_data[api_data['yardline_100']<=20]

    drive_results = api_data[['season','week','defteam','fixed_drive','fixed_drive_result']].drop_duplicates().reset_index(drop=True)

    drive_results['resulting_points'] = [3 if a == 'Field goal' else 7 if a == 'Touchdown' else 0 for a in drive_results['fixed_drive_result']]

    grouped_drive_results = drive_results.groupby(['season','week','defteam'], as_index=False)['resulting_points'].mean().sort_values(by=['season','defteam','week'])

    join_data = api_data[['season','week','defteam']].drop_duplicates().reset_index(drop=True).sort_values(by=['season','defteam','week'])

    join_data.merge(grouped_drive_results, how='left', on=['season','week','defteam']).replace(np. nan,0)

    grouped_drive_results = grouped_drive_results.assign(def_points_per_RZ = grouped_drive_results.groupby(['season','defteam'], as_index=False)['resulting_points'].rolling(trailing_weeks).mean()['resulting_points'])[['season','week','defteam','def_points_per_RZ']]

    return(grouped_drive_results.reset_index(drop=True).rename(columns = {'defteam':'team'}))

# def points per game
@cache.memoize()
def get_points_per_game(api_data, trailing_weeks = 5):

    ppg_df = api_data[['season','week','posteam','posteam_score']].groupby(['season','week','posteam'], as_index=False)['posteam_score'].max().sort_values(by=['season','posteam','week'])

    return(ppg_df.assign(off_ppg = ppg_df.groupby(['season','posteam'], as_index=False)['posteam_score'].rolling(trailing_weeks).mean()['posteam_score'])[['season','week','posteam','off_ppg']].rename(columns={'posteam':'team'})
    )

# points per game allwed
@cache.memoize()
def get_def_points_per_game(api_data, trailing_weeks = 5):

    def_ppg_df = api_data[['season','week','defteam','posteam_score']].groupby(['season','week','defteam'], as_index=False)['posteam_score'].max().sort_values(by=['season','defteam','week'])

    def_ppg_df = def_ppg_df.assign(def_ppg = def_ppg_df.groupby(['season','defteam'], as_index=False)['posteam_score'].rolling(trailing_weeks).mean()['posteam_score'])[['season','week','defteam', 'def_ppg']]

    return(def_ppg_df.reset_index(drop=True).rename(columns = {'defteam':'team'}))


# QB rush yards per game
@cache.memoize()
def get_qb_rush_per_game(api_data, trailing_weeks = 5):

    passer_names = [x for x in api_data['passer_player_name'].value_counts()[api_data['passer_player_name'].value_counts()> 10].index]

    qb_rush_yds = api_data[(api_data['rusher_player_name'].isin(passer_names)) | (api_data['qb_scramble']==1)].groupby(['season','week','posteam'], as_index=False)['yards_gained'].sum().sort_values(by=['season','posteam','week'])

    join_data = api_data[['season','week','posteam']].drop_duplicates().reset_index(drop=True).sort_values(by=['season','posteam','week'])
    join_data = join_data[join_data[['posteam']].notnull().all(1)]

    qb_rush_yds = join_data.merge(qb_rush_yds, how='left', on=['season','week','posteam']).replace(np. nan,0)

    # qb_rush_yds.groupby(['season','posteam'], as_index = False)['yards_gained'].rolling(trailing_weeks).mean()['yards_gained']

    output_df = qb_rush_yds.assign(qb_rush_gain = qb_rush_yds.groupby(['season','posteam'], as_index = False)['yards_gained'].rolling(trailing_weeks).mean()['yards_gained'])[['season','week','posteam','qb_rush_gain']]

    # api_data[(api_data['rusher_player_name'].isin(passer_names))].groupby(['season','week','posteam'], as_index = False)['yards_gained'].sum().head(20)

    return(output_df)

# get percent leading games
@cache.memoize()
def get_pct_leading(api_data, trailing_weeks = 5):

    # percent of plays leading

    api_data['leading_team'] = np.where(api_data['posteam_score']>=api_data['defteam_score'], api_data['posteam'], api_data['defteam'])
    api_data['leading_team'] = ['TIED' if x == None else x for x in api_data['leading_team']]

    # maybe add another df for trailing team and average those

    mid_df = api_data.groupby(['season','week', 'game_id'], as_index=False)['leading_team'].value_counts(normalize=True)[['season','week','leading_team','proportion']]
    mid_df = mid_df[mid_df['leading_team']!='TIED']
    mid_df = mid_df.rename(columns = {'leading_team':'team'})

    # need to left join a list of all teams with the season

    team_list = api_data[['season','week','posteam']].drop_duplicates().reset_index(drop=True).rename(columns = {'posteam':'team'})
    team_list = team_list[team_list.team.notna()]

    mid_df_2 = team_list.merge(mid_df, how='left', on=['season','week', 'team']).replace(np.nan,0).sort_values(['season','team']).reset_index(drop=True)
    output_df = mid_df_2.assign(proportion_leading = mid_df_2.groupby(['season','team'], as_index = False)['proportion'].rolling(trailing_weeks).mean()['proportion'])[['season','week','team','proportion_leading']]

    return(output_df)

# percent of plays leading by more than three
@cache.memoize()
def get_pct_leading_three(api_data, trailing_weeks = 5):

    num = 3

    api_data['posteam_lead'] = api_data['posteam_score'] - api_data['defteam_score']

    api_data['team_lead_three'] = np.where(api_data['posteam_lead']>(num), api_data['posteam'], "NEUTRAL")

    api_data['team_lead_three'] = np.where(api_data['posteam_lead']<(-1*num), api_data['defteam'], api_data['team_lead_three'])

    mid_df = api_data.groupby(['season','week','game_id'], as_index = False)['team_lead_three'].value_counts(normalize = True)
    mid_df = mid_df[mid_df['team_lead_three']!="NEUTRAL"]
    mid_df = mid_df.rename(columns = {'team_lead_three':'team'}).drop(['game_id'], axis=1)

    # left joining with overall teams and weeks

    team_list = api_data[['season','week','posteam']].drop_duplicates().reset_index(drop=True).rename(columns = {'posteam':'team'})
    team_list = team_list[team_list.team.notna()]

    mid_df_2 = team_list.merge(mid_df, how='left', on=['season','week', 'team']).replace(np.nan,0).sort_values(['season','team']).reset_index(drop=True)
    output_df = mid_df_2.assign(proportion_leading_three = mid_df_2.groupby(['season','team'], as_index = False)['proportion'].rolling(trailing_weeks).mean()['proportion'])[['season','week','team','proportion_leading_three']]

    # output_df.groupby(['season','team'], as_index = False)['proportion_leading_three'].mean().sort_values('proportion_leading_three')

    return(output_df)

# percent of plays leading by more than seven
@cache.memoize()
def get_pct_leading_seven(api_data, trailing_weeks = 5):

    num = 7

    api_data['posteam_lead'] = api_data['posteam_score'] - api_data['defteam_score']

    api_data['team_lead_seven'] = np.where(api_data['posteam_lead']>(num), api_data['posteam'], "NEUTRAL")

    api_data['team_lead_seven'] = np.where(api_data['posteam_lead']<(-1*num), api_data['defteam'], api_data['team_lead_seven'])

    mid_df = api_data.groupby(['season','week','game_id'], as_index = False)['team_lead_seven'].value_counts(normalize = True)
    mid_df = mid_df[mid_df['team_lead_seven']!="NEUTRAL"]
    mid_df = mid_df.rename(columns = {'team_lead_seven':'team'}).drop(['game_id'], axis=1)

    # left joining with overall teams and weeks

    team_list = api_data[['season','week','posteam']].drop_duplicates().reset_index(drop=True).rename(columns = {'posteam':'team'})
    team_list = team_list[team_list.team.notna()]

    mid_df_2 = team_list.merge(mid_df, how='left', on=['season','week', 'team']).replace(np.nan,0).sort_values(['season','team']).reset_index(drop=True)
    output_df = mid_df_2.assign(proportion_leading_seven = mid_df_2.groupby(['season','team'], as_index = False)['proportion'].rolling(trailing_weeks).mean()['proportion'])[['season','week','team','proportion_leading_seven']]

    # output_df.groupby(['season','team'], as_index = False)['proportion_leading_three'].mean().sort_values('proportion_leading_three')

    return(output_df)

# percent of drives ending in turnover for offense
@cache.memoize()
def get_drives_in_turnover(api_data, trailing_weeks = 5):

    int_df = api_data[['season','week','posteam','fixed_drive_result']].groupby(['season','week','posteam'], as_index=False)['fixed_drive_result'].value_counts(normalize=True)

    rel_results = ['Turnover','Opp touchdown']

    mid_df = int_df[int_df['fixed_drive_result'].isin(rel_results)].groupby(['season','week','posteam'], as_index=False)['proportion'].sum().sort_values(['season','posteam','week'])

    # left join here

    join_data = api_data[['season','week','posteam']].drop_duplicates().reset_index(drop=True).sort_values(by=['season','posteam','week'])
    join_data = join_data[join_data[['posteam']].notnull().all(1)]

    mid_df = join_data.merge(mid_df, how='left', on=['season','week','posteam']).replace(np. nan,0)

    # --------------

    output_df = mid_df.assign(turnover_rate = mid_df.groupby(['season','posteam'],as_index = False)['proportion'].rolling(trailing_weeks).mean()['proportion'])[['season','week','posteam','turnover_rate']]

    return(output_df.rename(columns = {'posteam':'team', 'turnover_rate':'off_turnover_rate'}))

# defensive drives ending in turnover
@cache.memoize()
def get_def_drives_in_turnover(api_data, trailing_weeks = 5):

    int_df = api_data[['season','week','defteam','fixed_drive_result']].groupby(['season','week','defteam'], as_index=False)['fixed_drive_result'].value_counts(normalize=True)

    rel_results = ['Turnover','Opp touchdown']

    mid_df = int_df[int_df['fixed_drive_result'].isin(rel_results)].groupby(['season','week','defteam'], as_index=False)['proportion'].sum().sort_values(['season','defteam','week'])

    # left join here

    join_data = api_data[['season','week','defteam']].drop_duplicates().reset_index(drop=True).sort_values(by=['season','defteam','week'])
    join_data = join_data[join_data[['defteam']].notnull().all(1)]

    mid_df = join_data.merge(mid_df, how='left', on=['season','week','defteam']).replace(np. nan,0)

    # --------------

    output_df = mid_df.assign(turnover_rate = mid_df.groupby(['season','defteam'],as_index = False)['proportion'].rolling(trailing_weeks).mean()['proportion'])[['season','week','defteam','turnover_rate']]

    return(output_df.rename(columns = {'defteam':'team', 'turnover_rate':'def_turnover_rate'}))

# get actual points per week
@cache.memoize()
def get_actual_game_points(api_data, trailing_weeks = 5):

    return(api_data.groupby(['season','week','posteam'], as_index = False)['posteam_score'].max().rename(columns = {'posteam':'team', 'posteam_score':'actual_off_points'}))

# getting EPA sum
@cache.memoize()
def get_epa_sum(api_data, trailing_weeks = 5):

    return(api_data[api_data['play_type'].isin(['run','pass'])].groupby(['season','week','posteam'], as_index=False)[['epa']].sum().rename(columns = {'posteam':'team', 'epa':'total_off_epa_sum'}))

# QB aggressiveness by team
@cache.memoize()
def get_qb_aggr(next_gen_stats_pass, trailing_weeks = 5):

    top_qbs = next_gen_stats_pass[next_gen_stats_pass.groupby(['season','week','team_abbr'])['attempts'].rank(ascending=False)==1]
    mid_df = top_qbs.groupby(['season','week','team_abbr'], as_index = False)['aggressiveness'].mean().sort_values(['season','team_abbr','week'])
    output_df = mid_df.assign(qb_aggr = mid_df.groupby(['season','team_abbr'], as_index = False)['aggressiveness']
                  .rolling(trailing_weeks)
                  .mean()['aggressiveness'])[['season','week','team_abbr','qb_aggr']].reset_index(drop=True).rename(columns={'team_abbr':'team'})
    return(output_df)

# DEF QB aggr forced
@cache.memoize()
def get_def_qb_aggr(next_gen_stats_pass, trailing_weeks = 5):

    top_qbs = next_gen_stats_pass[next_gen_stats_pass.groupby(['season','week','team_abbr'])['attempts'].rank(ascending=False)==1]
    mid_df = top_qbs.groupby(['season','week','defteam'], as_index = False)['aggressiveness'].mean().sort_values(['season','defteam','week'])
    output_df = mid_df.assign(def_aggr = mid_df.groupby(['season','defteam'], as_index = False)['aggressiveness']
                  .rolling(trailing_weeks)
                  .mean()['aggressiveness'])[['season','week','defteam','def_aggr']].reset_index(drop=True).rename(columns={'defteam':'team', 'def_aggr':'def_aggr_forced'})

    return(output_df)

# box stuff rates by defense
@cache.memoize()
def get_def_box_stuff(next_gen_stats_rush, trailing_weeks = 5):

    mid_df = next_gen_stats_rush.groupby(['season','week','defteam'], as_index = False)['percent_attempts_gte_eight_defenders'].mean().sort_values(by=['season','defteam','week']).rename(columns = {'percent_attempts_gte_eight_defenders':'box_stuff_rate'})
    output_df = mid_df.assign(def_box_stuff_rate = mid_df.groupby(['season','defteam'], as_index = False)['box_stuff_rate'].rolling(trailing_weeks).mean()['box_stuff_rate'])[['season','week','defteam','def_box_stuff_rate']].reset_index(drop=True)

    return(output_df.rename(columns={'defteam':'team'}))

# WR cushion allowed by defense (time of snap)
@cache.memoize()
def get_def_cushion(next_gen_stats_rec, trailing_weeks = 5):

    mid_df = next_gen_stats_rec.groupby(['season','week','defteam'], as_index = False)['avg_cushion'].mean().sort_values(['season','defteam','week'])
    output_df = mid_df.assign(def_cushion = mid_df.groupby(['season','defteam'],as_index = False)['avg_cushion'].rolling(trailing_weeks).mean()['avg_cushion'])[['season','week','defteam','def_cushion']].reset_index(drop=True)

    return(output_df.rename(columns = {'defteam':'team'}).reset_index(drop=True))


# defensive separation (time of throw)
@cache.memoize()
def get_def_separation(next_gen_stats_rec, trailing_weeks = 5):

    mid_df = next_gen_stats_rec.groupby(['season','week','defteam'], as_index = False)['avg_separation'].mean().sort_values(['season','defteam','week'])
    output_df = mid_df.assign(def_separation = mid_df.groupby(['season','defteam'],as_index = False)['avg_separation'].rolling(trailing_weeks).mean()['avg_separation'])[['season','week','defteam','def_separation']].reset_index(drop=True)

    return(output_df.reset_index(drop=True).rename(columns={'defteam':'team'}))


# avg air distance per throw
@cache.memoize()
def get_avg_throw_dist(next_gen_stats_pass, trailing_weeks = 5):

    top_qbs = next_gen_stats_pass[next_gen_stats_pass.groupby(['season','week','team_abbr'])['attempts'].rank(ascending=False)==1]
    mid_df = top_qbs.groupby(['season','week','team_abbr'], as_index = False)['avg_intended_air_yards'].mean().sort_values(['season','team_abbr','week'])
    output_df = mid_df.assign(off_avg_throw_dist = mid_df.groupby(['season','team_abbr'], as_index = False)['avg_intended_air_yards'].rolling(trailing_weeks).mean()['avg_intended_air_yards'])[['season','week','team_abbr','off_avg_throw_dist']].reset_index(drop=True)

    return(output_df.rename(columns = {'team_abbr':'team'}))


# plays over 25 yards
@cache.memoize()
def get_off_plays_25yd(api_data, trailing_weeks = 5):

    index_cols = api_data[['season','week','posteam']].dropna().drop_duplicates().reset_index(drop=True).sort_values(by=['season','posteam','week']).rename(columns={'posteam':'team'})

    ovr_25 = pd.DataFrame(api_data[(api_data['play_type'].isin(['run','pass'])) & (api_data['yards_gained'] >= 25)].groupby(['season','week','posteam'])['posteam'].count()).rename(columns={'posteam':'plays_ovr_25'}).reset_index().sort_values(by=['season','posteam','week']).rename(columns={'posteam':'team'})

    mid_df = index_cols.merge(ovr_25, how='left', on=['season','week','team']).replace(np. nan,0)

    output_df = mid_df.assign(plays_over_25_yd = mid_df.groupby(['season','team'], as_index = False)['plays_ovr_25'].rolling(trailing_weeks).mean()['plays_ovr_25'])[['season','week','team','plays_over_25_yd']]

    return(output_df)

# touchdowns over 25 yards
@cache.memoize()
def get_off_td_25yd(api_data, trailing_weeks = 5):

    index_cols = api_data[['season','week','posteam']].dropna().drop_duplicates().reset_index(drop=True).sort_values(by=['season','posteam','week']).rename(columns={'posteam':'team'})

    ovr_25 = pd.DataFrame(api_data[(api_data['play_type'].isin(['run','pass'])) & (api_data['yards_gained'] >= 25) & (api_data['td_team'].notnull())].groupby(['season','week','posteam'])['posteam'].count()).rename(columns={'posteam':'plays_ovr_25'}).reset_index().sort_values(by=['season','posteam','week']).rename(columns={'posteam':'team'})

    mid_df = index_cols.merge(ovr_25, how='left', on=['season','week','team']).replace(np. nan,0)

    output_df = mid_df.assign(td_over_25_yd = mid_df.groupby(['season','team'], as_index = False)['plays_ovr_25'].rolling(trailing_weeks).mean()['plays_ovr_25'])[['season','week','team','td_over_25_yd']]

    return(output_df)

# plays over 25 yards allowed
@cache.memoize()
def get_def_plays_25yd(api_data, trailing_weeks = 5):

    index_cols = api_data[['season','week','posteam']].dropna().drop_duplicates().reset_index(drop=True).sort_values(by=['season','posteam','week']).rename(columns={'posteam':'team'})

    ovr_25 = pd.DataFrame(api_data[(api_data['play_type'].isin(['run','pass'])) & (api_data['yards_gained'] >= 25)].groupby(['season','week','defteam'])['defteam'].count()).rename(columns={'defteam':'plays_ovr_25'}).reset_index().sort_values(by=['season','defteam','week']).rename(columns={'defteam':'team'})

    mid_df = index_cols.merge(ovr_25, how='left', on=['season','week','team']).replace(np. nan,0)

    output_df = mid_df.assign(def_plays_over_25_yd = mid_df.groupby(['season','team'], as_index = False)['plays_ovr_25'].rolling(trailing_weeks).mean()['plays_ovr_25'])[['season','week','team','def_plays_over_25_yd']]

    return(output_df)

# td over 25 yards allowed
@cache.memoize()
def get_def_td_25yd(api_data, trailing_weeks = 5):

    index_cols = api_data[['season','week','posteam']].dropna().drop_duplicates().reset_index(drop=True).sort_values(by=['season','posteam','week']).rename(columns={'posteam':'team'})

    ovr_25 = pd.DataFrame(api_data[(api_data['play_type'].isin(['run','pass'])) & (api_data['yards_gained'] >= 25) & (api_data['td_team'].notnull())].groupby(['season','week','defteam'])['defteam'].count()).rename(columns={'defteam':'plays_ovr_25'}).reset_index().sort_values(by=['season','defteam','week']).rename(columns={'defteam':'team'})

    mid_df = index_cols.merge(ovr_25, how='left', on=['season','week','team']).replace(np. nan,0)

    output_df = mid_df.assign(def_td_over_25_yd = mid_df.groupby(['season','team'], as_index = False)['plays_ovr_25'].rolling(trailing_weeks).mean()['plays_ovr_25'])[['season','week','team','def_td_over_25_yd']]

    return(output_df)

# series conversion rate
@cache.memoize()
def get_off_scr(api_data, trailing_weeks = 5):

    mid_df = api_data[['season','week','posteam','series','series_success']].groupby(['season','week','posteam'], as_index=False).agg({'series':'max', 'series_success':'sum'})

    mid_df['conv_rate'] = (mid_df['series_success']/mid_df['series'])
    mid_df = mid_df.sort_values(by=['season','posteam','week'])


    output_df = mid_df.assign(team_scr = mid_df.groupby(['season','posteam'], as_index=False)['series_success'].rolling(trailing_weeks).mean()['series_success'])[['season','week','posteam','team_scr']]

#    output_df.groupby(['season','posteam'], as_index =False)['team_scr'].mean().sort_values(by=['team_scr'])

    return(output_df.rename(columns = {'posteam':'team'}))

# defensive series conversion rate allowed
@cache.memoize()
def get_def_scr_allowed(api_data, trailing_weeks = 5):

    mid_df = api_data[['season','week','defteam','series','series_success']].groupby(['season','week','defteam'], as_index=False).agg({'series':'max', 'series_success':'sum'})

    mid_df['conv_rate'] = (mid_df['series_success']/mid_df['series'])
    mid_df = mid_df.sort_values(by=['season','defteam','week'])


    output_df = mid_df.assign(defteam_scr = mid_df.groupby(['season','defteam'], as_index=False)['series_success'].rolling(trailing_weeks).mean()['series_success'])[['season','week','defteam','defteam_scr']]

    # output_df.groupby(['season','defteam'], as_index =False)['defteam_scr'].mean().sort_values(by=['defteam_scr'])

    return(output_df.reset_index(drop=True).rename(columns = {'defteam':'team'}))

# QB completion rate offense
@cache.memoize()
def get_qb_comp_rate(api_data, trailing_weeks = 5):

    mid_df = api_data[api_data['play_type']=='pass'].groupby(['season','week','posteam'], as_index=False)[['complete_pass','play_counter']].sum()

    mid_df['completion_rate'] = mid_df['complete_pass']/mid_df['play_counter']
    mid_df = mid_df.sort_values(by=['season','posteam','week'])

    output_df = mid_df.assign(off_qb_comp = mid_df.groupby(['season','posteam'], as_index=False)['completion_rate'].rolling(trailing_weeks).mean()['completion_rate'])[['season','week','posteam','off_qb_comp']]

    # output_df.groupby(['season','posteam'], as_index=False)['off_qb_comp'].mean().sort_values(by=['off_qb_comp'])

    return(output_df.rename(columns = {'posteam':'team'}))


# QB completion rate allowed on defense
@cache.memoize()
def qb_def_comp_rate_allowed(api_data, trailing_weeks = 5):

    mid_df = api_data[api_data['play_type']=='pass'].groupby(['season','week','defteam'], as_index=False)[['complete_pass','play_counter']].sum()

    mid_df['completion_rate'] = mid_df['complete_pass']/mid_df['play_counter']
    mid_df = mid_df.sort_values(by=['season','defteam','week'])

    output_df = mid_df.assign(def_qb_comp = mid_df.groupby(['season','defteam'], as_index=False)['completion_rate'].rolling(trailing_weeks).mean()['completion_rate'])[['season','week','defteam','def_qb_comp']]

    # output_df.groupby(['season','defteam'], as_index=False)['def_qb_comp'].mean().sort_values(by=['def_qb_comp'])

    return(output_df.reset_index(drop=True).rename(columns = {'defteam':'team'}))


# QB hits allowed on offense
@cache.memoize()
def qb_hits_allowed_off(api_data, trailing_weeks = 5):

    mid_df = api_data.groupby(['season','week','posteam'], as_index=False)[['qb_hit','play_counter']].sum()
    mid_df['qb_hitrate'] = mid_df['qb_hit']/mid_df['play_counter']
    mid_df = mid_df.sort_values(by=['season','posteam','week'])

    # left join here
    join_data = api_data[['season','week','posteam']].drop_duplicates().reset_index(drop=True).sort_values(by=['season','posteam','week'])
    join_data = join_data[join_data[['posteam']].notnull().all(1)]

    mid_df = join_data.merge(mid_df, how='left', on=['season','week','posteam']).replace(np. nan,0)
    # ----------

    output_df = mid_df.assign(off_qbhit = mid_df.groupby(['season','posteam'], as_index=False)['qb_hitrate'].rolling(trailing_weeks).mean()['qb_hitrate'])[['season','week','posteam','off_qbhit']]

    # output_df.groupby(['season','posteam'], as_index=False)['off_qbhit'].mean().sort_values(by=['off_qbhit'])

    return(output_df.rename(columns = {'posteam':'team'}))

# QB hits by defense
@cache.memoize()
def get_def_qb_hits(api_data, trailing_weeks = 5):

    mid_df = api_data.groupby(['season','week','defteam'], as_index=False)[['qb_hit','play_counter']].sum()
    mid_df['qb_hitrate'] = mid_df['qb_hit']/mid_df['play_counter']
    mid_df = mid_df.sort_values(by=['season','defteam','week'])

    # left join here
    join_data = api_data[['season','week','defteam']].drop_duplicates().reset_index(drop=True).sort_values(by=['season','defteam','week'])
    join_data = join_data[join_data[['defteam']].notnull().all(1)]

    mid_df = join_data.merge(mid_df, how='left', on=['season','week','defteam']).replace(np. nan,0)
    # ----------


    output_df = mid_df.assign(def_qbhit = mid_df.groupby(['season','defteam'], as_index=False)['qb_hitrate'].rolling(trailing_weeks).mean()['qb_hitrate'])[['season','week','defteam','def_qbhit']]

    # output_df.groupby(['season','defteam'], as_index=False)['def_qbhit'].mean().sort_values(by=['def_qbhit'])

    return(output_df.reset_index(drop=True).rename(columns={'defteam':'team'}))

# SEASON score differential
@cache.memoize()
def get_season_point_diff(api_data):

    api_data['home_score_diff'] = api_data['home_score']-api_data['away_score']
    api_data['away_score_diff'] = api_data['away_score']-api_data['home_score']

    h_teams = api_data[['season','week','home_team','home_score_diff']].drop_duplicates().reset_index(drop=True).rename(columns = {'home_team':'team', 'home_score_diff':'score_diff'})
    a_teams = api_data[['season','week','away_team','away_score_diff']].drop_duplicates().reset_index(drop=True).rename(columns = {'away_team':'team', 'away_score_diff':'score_diff'})

    mid_df = (pd.concat([h_teams, a_teams], axis=0)).sort_values(by=['season','team','week'])

    mid_df['score_diff'] = mid_df.groupby(['season','team','week'], as_index=False)['score_diff'].cumsum()
    output_df = mid_df
    # output_df.sort_values(by=['score_diff'])


    return(output_df.rename(columns = {'score_diff':'total_season_point_differential'}))

# points on the opening drive of a game
@cache.memoize()
def get_first_drive_points_scored(api_data, trailing_weeks = 5):

    mid_df = api_data[['season','week','posteam', 'drive','fixed_drive_result']].drop_duplicates().reset_index(drop=True)

    mid_df['points_scored'] = pd.Series([3 if a == 'Field goal' else 7 if a == 'Touchdown' else 0 for a in mid_df['fixed_drive_result']])

    mid_df = (mid_df[mid_df.groupby(['season','week','posteam'])['drive'].rank(ascending=True)==1]).sort_values(by=['season','posteam','week'])

    output_df = mid_df.assign(first_drive_pts_avg = mid_df.groupby(['season','posteam'], as_index=False)['points_scored'].rolling(trailing_weeks).mean()['points_scored'])[['season','week','posteam','first_drive_pts_avg']]

    # output_df.groupby(['season','posteam'], as_index=False)['first_drive_pts_avg'].mean().sort_values(by=['first_drive_pts_avg'])

    return(output_df.reset_index(drop=True).rename(columns={'posteam':'team'}))


# points ALLOWED first drive of game
@cache.memoize()
def get_def_first_drive_points_allowed(api_data, trailing_weeks = 5):

    mid_df = api_data[['season','week','defteam', 'drive','fixed_drive_result']].drop_duplicates().reset_index(drop=True)

    mid_df['points_scored'] = pd.Series([3 if a == 'Field goal' else 7 if a == 'Touchdown' else 0 for a in mid_df['fixed_drive_result']])

    mid_df = (mid_df[mid_df.groupby(['season','week','defteam'])['drive'].rank(ascending=True)==1]).sort_values(by=['season','defteam','week'])

    output_df = mid_df.assign(first_drive_pts_avg_allowed = mid_df.groupby(['season','defteam'], as_index=False)['points_scored'].rolling(trailing_weeks).mean()['points_scored'])[['season','week','defteam','first_drive_pts_avg_allowed']]

    # output_df.groupby(['season','defteam'], as_index=False)['first_drive_pts_avg_allowed'].mean().sort_values(by=['first_drive_pts_avg_allowed'])

    return(output_df.reset_index(drop=True).rename(columns={'defteam':'team'}))

# pct of passing yards from YAC versus actual receiving yards
@cache.memoize()
def get_yac_air_yards(api_data, trailing_weeks = 5):

    mid_df = api_data[(api_data['play_type']=='pass') & (api_data['complete_pass'] == 1)].groupby(['season','week','posteam'], as_index=False)[['air_yards','yards_after_catch']].sum()
    mid_df['pct_air_yards'] = mid_df['air_yards'] / (mid_df['air_yards'] + mid_df['yards_after_catch'])
    mid_df['pct_yac'] = 1 - mid_df['pct_air_yards']
    mid_df = (mid_df[['season','week','posteam','pct_air_yards','pct_yac']]).sort_values(by=['season','posteam','week'])

    add_df = mid_df.groupby(['season','posteam'], as_index = False)[['pct_air_yards','pct_yac']].rolling(trailing_weeks).mean()[['pct_air_yards','pct_yac']].rename(columns = {'pct_air_yards':'trailing_pct_air_yards', 'pct_yac':'trailing_pct_yac'})

    output_df = pd.concat([mid_df[['season','week','posteam']], add_df], axis=1)

    # output_df.groupby(['season','posteam'],as_index=False)['trailing_pct_air_yards'].mean().sort_values(by=['trailing_pct_air_yards'])

    return(output_df.reset_index(drop=True).rename(columns={'posteam':'team'}))

# points on the opening drive of the second half
@cache.memoize()
def get_2h_first_drive_points_scored(api_data, trailing_weeks = 5):

    api_data = api_data[api_data['game_half']=='Half2']

    mid_df = api_data[['season','week','posteam', 'drive','fixed_drive_result']].drop_duplicates().reset_index(drop=True)

    mid_df['points_scored'] = pd.Series([3 if a == 'Field goal' else 7 if a == 'Touchdown' else 0 for a in mid_df['fixed_drive_result']])

    mid_df = (mid_df[mid_df.groupby(['season','week','posteam'])['drive'].rank(ascending=True)==1]).sort_values(by=['season','posteam','week'])

    output_df = mid_df.assign(h2_first_drive_pts_avg = mid_df.groupby(['season','posteam'], as_index=False)['points_scored'].rolling(trailing_weeks).mean()['points_scored'])[['season','week','posteam','h2_first_drive_pts_avg']]

    # output_df.groupby(['season','posteam'], as_index=False)['first_drive_pts_avg'].mean().sort_values(by=['first_drive_pts_avg'])

    return(output_df.reset_index(drop=True).rename(columns={'posteam':'team'}))

# points ALLOWED first drive of 2h
@cache.memoize()
def get_2h_def_first_drive_points_allowed(api_data, trailing_weeks = 5):

    api_data = api_data[api_data['game_half']=='Half2']

    mid_df = api_data[['season','week','defteam', 'drive','fixed_drive_result']].drop_duplicates().reset_index(drop=True)

    mid_df['points_scored'] = pd.Series([3 if a == 'Field goal' else 7 if a == 'Touchdown' else 0 for a in mid_df['fixed_drive_result']])

    mid_df = (mid_df[mid_df.groupby(['season','week','defteam'])['drive'].rank(ascending=True)==1]).sort_values(by=['season','defteam','week'])

    output_df = mid_df.assign(h2_first_drive_pts_avg_allowed = mid_df.groupby(['season','defteam'], as_index=False)['points_scored'].rolling(trailing_weeks).mean()['points_scored'])[['season','week','defteam','h2_first_drive_pts_avg_allowed']]

    # output_df.groupby(['season','defteam'], as_index=False)['first_drive_pts_avg_allowed'].mean().sort_values(by=['first_drive_pts_avg_allowed'])

    return(output_df.reset_index(drop=True).rename(columns={'defteam':'team'}))
