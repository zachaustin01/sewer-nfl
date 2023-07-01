import sys, os
import scipy as scipy
import numpy as np
import pandas as pd
import requests as requests
import functools as ft


REPO_NAME = 'sewer-nfl'
cwd = str(os.getcwd())
repo_dir = cwd[:cwd.find(REPO_NAME)+len(REPO_NAME)]
sys.path.insert(0,repo_dir)

from warehouse.pipelines.pbp.performance import *

def pipe_turnover_propensity(api_data, next_gen_stats_pass, trailing_weeks = 5):

    # TURNOVER PROPENSITY

    # get_pct_pass
    # get_drives_in_turnover
    # get_qb_aggr
    # get_qb_comp_rate
    # qb_hits_allowed_off

    # get_pct_pass: -.35, / .4
    temp_pct_pass = get_pct_pass(api_data, trailing_weeks)
    # get_drives_in_turnover: 0, /.25
    temp_drives_in_turnover = get_drives_in_turnover(api_data, trailing_weeks)
    # get_qb_aggr: -6, /25
    temp_qb_aggr = get_qb_aggr(next_gen_stats_pass, trailing_weeks)
    # get_qb_comp_rate: -.4, /.4
    temp_qb_comp_rate = get_qb_comp_rate(api_data, trailing_weeks)
    # qb_hits_allowed_off: 0, /.13
    temp_qb_hits_allowed_off = qb_hits_allowed_off(api_data, trailing_weeks)

    # now combining

    dfs = [temp_pct_pass, temp_drives_in_turnover, temp_qb_aggr, temp_qb_comp_rate, temp_qb_hits_allowed_off]

    df_final = ft.reduce(lambda left, right: pd.merge(left, right, on=['season','week','team'], how='left'), dfs)

    names_df = df_final[['season','week','team']]

    mid_df = df_final.drop(['season','week','team'], axis=1)

    # subtract vector
    sub_vec = [.35, 0, 6, .4, 0]

    # mult vector
    denom_vec = [.4, .25, 25, .4, .13]
    denom_vec = list(np.reciprocal(denom_vec))

    # vector for negative / positive
    sign_vec = [1, 1, 1, -1, 1]

    # vector for weight
    weight_vec = [.2, .8, .6, .3, .6]

    # applying subtraction
    mid_df = mid_df.sub(sub_vec, axis='columns')

    # multiply them all together and then thru the matrix. Then perform a row sum

    mult_vec = [a*b*c for a,b,c in zip(denom_vec, sign_vec, weight_vec)]

    output_df = mid_df.multiply(mult_vec)

    output_df['turnover_propensity'] = output_df.sum(axis=1)

    output_df = pd.concat([names_df, output_df], axis = 1)

    output_df = output_df.reset_index(drop=True)[['season','week','team','turnover_propensity']]

    return(output_df)

def pipe_def_turnover_propensity(api_data, next_gen_stats_pass, next_gen_stats_rec, trailing_weeks = 5):

    # TURNOVER PROPENSITY def ability

    # get_def_epa_per_pass
    # get_def_qb_aggr
    # get_def_drives_in_turnover
    # get_def_cushion
    # get_def_separation
    # qb_def_comp_rate_allowed
    # get_def_qb_hits


    # creating the list of dataframes to join together ------------------------------------------------------

    # get_def_epa_per_pass: +.7, / 1.3
    temp_def_epa_pass = get_def_epa_per_pass(api_data, trailing_weeks)
    # get_def_qb_aggr: -5, /25
    temp_def_qb_aggr = get_def_qb_aggr(next_gen_stats_pass, trailing_weeks)
    # get_def_drives_in_turnover: /.3
    temp_def_turnover = get_def_drives_in_turnover(api_data, trailing_weeks)
    # get_def_cushion: -4, / 5
    temp_def_cushion = get_def_cushion(next_gen_stats_rec, trailing_weeks)
    # get_def_separation: -2, / 2.5
    temp_def_separation = get_def_separation(next_gen_stats_rec, trailing_weeks)
    # qb_def_comp_rate_allowed: -.4, / .4
    temp_def_comp_rate = qb_def_comp_rate_allowed(api_data, trailing_weeks)
    # get_def_qb_hits: / .15
    temp_def_qb_hits = get_def_qb_hits(api_data, trailing_weeks)

    dfs = [temp_def_epa_pass, temp_def_qb_aggr, temp_def_turnover, temp_def_cushion, temp_def_separation, temp_def_comp_rate, temp_def_qb_hits]

    df_final = ft.reduce(lambda left, right: pd.merge(left, right, on=['season','week','team']), dfs)

    names_df = df_final[['season','week','team']]

    mid_df = df_final.drop(['season','week','team'], axis=1)

    # manipulating the numbers to create a measure ----------------------------------------------------------
    # subtract vector
    sub_vec = [-.7, 5, 0, 4, 2, .4, 0]

    # mult vector
    denom_vec = [1.3, 25, .3, 5, 2.5, .4, .15]
    denom_vec = list(np.reciprocal(denom_vec))

    # vector for negative / positive
    sign_vec = [-1, 1, 1, -1, -1, -1, 1]

    # vector for weight
    weight_vec = [.2, .7, .6, .2, .4, .15, .7]

    # applying subtraction
    mid_df = mid_df.sub(sub_vec, axis='columns')

    # multiply them all together and then thru the matrix. Then perform a row sum

    mult_vec = [a*b*c for a,b,c in zip(denom_vec, sign_vec, weight_vec)]

    output_df = mid_df.multiply(mult_vec)

    output_df['def_turnover_propensity'] = output_df.sum(axis=1)

    output_df = pd.concat([names_df, output_df], axis = 1)

    output_df = output_df[['season','week','team','def_turnover_propensity']]

    return(output_df)

def pipe_epa_hhi_combo(api_data, trailing_weeks = 5):

    # EPA times HHI

    # get_epa_sum
    # get_team_hhi

    # creating the list of dataframes to join together ------------------------------------------------------

    # get_offense_epa
    temp_offense_epa = get_epa_sum(api_data, trailing_weeks)
    # get_team_hhi
    temp_team_hhi = get_team_hhi(api_data, trailing_weeks)

    dfs = [temp_offense_epa, temp_team_hhi]

    df_final = ft.reduce(lambda left, right: pd.merge(left, right, on=['season','week','team']), dfs)

    df_final['off_epa'] = ((df_final['off_epa'] + 70)/150) * .6

    df_final['team_HHI'] = 1 - ((df_final['team_HHI'] - .12)/.5)
    df_final = df_final.rename(columns = {'team_HHI':'inv_team_hhi'})

    df_final['balanced_player_efficacy'] = df_final['inv_team_hhi'] + df_final['off_epa']

    output_df = df_final.dropna().sort_values('balanced_player_efficacy')

    return(output_df[['season','week','team','balanced_player_efficacy']])

def pipe_points_per_epa(api_data, trailing_weeks = 5):

    # Points per EPA
    # get_actual_game_points
    # get_epa_sum
    # creating the list of dataframes to join together ------------------------------------------------------

    # get_actual_game_points
    temp_off_points = get_actual_game_points(api_data, trailing_weeks)
    # get_epa_sum
    temp_offense_epa = get_epa_sum(api_data, trailing_weeks)

    dfs = [temp_off_points, temp_offense_epa]

    df_final = ft.reduce(lambda left, right: pd.merge(left, right, on=['season','week','team']), dfs)

    df_final['off_epa'] = (df_final['off_epa'] + 70) / 150

    df_final['points_per_epa'] = df_final['posteam_score'] / df_final['off_epa']

    df_final = df_final.sort_values('points_per_epa')

    return(df_final.reset_index(drop=True)[['season','week','team','points_per_epa']])

def pipe_offense_coaching_ability(api_data, trailing_weeks = 5):

    # offensive coaching ability

    # get_team_hhi
    # get_first_drive_points_scored
    # get_2h_first_drive_points_scored

    # creating the list of dataframes to join together ------------------------------------------------------

    # get_team_hhi:
    temp_team_hhi = get_team_hhi(api_data, trailing_weeks)
    # get_first_drive_points_scored
    temp_first_drive = get_first_drive_points_scored(api_data, trailing_weeks)
    # get_2h_first_drive_points_scored:
    temp_2h_first_drive = get_2h_first_drive_points_scored(api_data, trailing_weeks)


    dfs = [temp_team_hhi, temp_first_drive, temp_2h_first_drive]

    df_final = ft.reduce(lambda left, right: pd.merge(left, right, on=['season','week','team']), dfs)

    df_final['team_HHI'] = (1 - ((df_final['team_HHI'] - .12)/.5)) * .25
    df_final['first_drive_pts_avg'] = (df_final['first_drive_pts_avg'] / 7 ) * .6
    df_final['h2_first_drive_pts_avg'] = (df_final['h2_first_drive_pts_avg'] / 7 ) * .7

    df_final['off_coaching'] = df_final['team_HHI'] + df_final['first_drive_pts_avg'] + df_final['h2_first_drive_pts_avg']

    df_final = df_final.reset_index(drop=True)[['season','week','team','off_coaching']]

    return(df_final)

def pipe_defense_coaching_ability(api_data, trailing_weeks = 5):

    # defensive coaching ability

    # get_def_scr_allowed
    # get_def_first_drive_points_allowed
    # get_2h_def_first_drive_points_allowed

    # creating the list of dataframes to join together ------------------------------------------------------

    # get_def_scr_allowed
    temp_team_scr = get_def_scr_allowed(api_data, trailing_weeks)
    # get_def_first_drive_points_allowed
    temp_first_drive = get_def_first_drive_points_allowed(api_data, trailing_weeks)
    # get_2h_def_first_drive_points_allowed
    temp_2h_first_drive = get_2h_def_first_drive_points_allowed(api_data, trailing_weeks)


    dfs = [temp_team_scr, temp_first_drive, temp_2h_first_drive]

    df_final = ft.reduce(lambda left, right: pd.merge(left, right, on=['season','week','team']), dfs)


    df_final['defteam_scr'] = ((df_final['defteam_scr'] - 20) / 50) * .3
    df_final['first_drive_pts_avg_allowed'] = (df_final['first_drive_pts_avg_allowed'] / 7) * .6
    df_final['h2_first_drive_pts_avg_allowed'] = (df_final['h2_first_drive_pts_avg_allowed'] / 7) * .7

    df_final['def_coaching'] = -1 * (df_final['defteam_scr'] + df_final['first_drive_pts_avg_allowed'] + df_final['h2_first_drive_pts_avg_allowed'])
    df_final = df_final.reset_index(drop=True)[['season','week','team','def_coaching']]

    return(df_final)

def pipe_def_burn_commit(next_gen_stats_rec, next_gen_stats_rush, trailing_weeks = 5):

    # mobile QB / burner susceptibility (press and box stuff rates)

    # get_def_cushion
    # get_def_box_stuff
    # get_def_separation

    # get_def_cushion
    temp_def_cushion = get_def_cushion(next_gen_stats_rec, trailing_weeks)
    # get_def_separation
    temp_def_separation = get_def_separation(next_gen_stats_rec, trailing_weeks)
    # get_def_box_stuff
    temp_def_box_stuff = get_def_box_stuff(next_gen_stats_rush, trailing_weeks)

    dfs = [temp_def_cushion, temp_def_separation, temp_def_box_stuff]

    df_final = ft.reduce(lambda left, right: pd.merge(left, right, on=['season','week','team'], how='left'), dfs)

    # changing def_box_stuff_rate NA to the median of all weeks
    med = df_final.groupby(['season','team'])['def_box_stuff_rate'].transform('median')
    df_final['def_box_stuff_rate'] = df_final['def_box_stuff_rate'].fillna(med)
    df_final['def_box_stuff_rate'] = np.where(df_final['week']<trailing_weeks, np.nan, df_final['def_box_stuff_rate'])

    df_final['def_cushion'] = .5 * ((df_final['def_cushion'] - 4) / 5)
    df_final['def_separation'] = .5 * ((df_final['def_separation'] - 1.5) / 3)
    df_final['def_box_stuff_rate'] = (df_final['def_box_stuff_rate']) / 70

    df_final['def_press_commit'] = df_final['def_cushion'] + df_final['def_separation'] + df_final['def_box_stuff_rate']

    df_final = df_final.reset_index(drop=True)[['season','week','team','def_press_commit']]

    df_final = df_final.rename(columns = {'def_press_commit':'conservative_coverage'})

    return(df_final)

def pipe_offense_scoring_propensity(api_data, trailing_weeks = 5):

    # Scoring propensity

    # get_points_per_drive
    # get_points_per_RZ
    # get_drives_in_turnover
    # get_epa_per_pass
    # get_epa_per_rush
    # get_off_scr

    # get_points_per_drive: -.5, /3
    temp_points_per_drive = get_points_per_drive(api_data, trailing_weeks)
    # get_points_per_RZ: -2, /5
    temp_points_per_RZ = get_points_per_RZ(api_data, trailing_weeks)
    # get_drives_in_turnover: /.25
    temp_drives_in_turnover = get_drives_in_turnover(api_data, trailing_weeks)
    # get_epa_per_pass: +4, /7
    temp_epa_per_pass = get_epa_per_pass(api_data, trailing_weeks)
    # get_epa_per_rush: +3, /5
    temp_epa_per_rush = get_epa_per_rush(api_data, trailing_weeks)
    # get_off_scr: -25, /50
    temp_off_scr = get_off_scr(api_data, trailing_weeks)

    # now combining

    dfs = [temp_points_per_drive, temp_points_per_RZ, temp_drives_in_turnover, temp_epa_per_pass, temp_epa_per_rush, temp_off_scr]

    df_final = ft.reduce(lambda left, right: pd.merge(left, right, on=['season','week','team'], how='left'), dfs)

    names_df = df_final[['season','week','team']]

    mid_df = df_final.drop(['season','week','team'], axis=1)

    # subtract vector
    sub_vec = [.5, 2, 0, -4, -3, 25]

    # mult vector
    denom_vec = [3, 5, .25, 7, 5, 50]
    denom_vec = list(np.reciprocal(denom_vec))

    # weight vec
    weight_vec = [.5, .6, .3, .5, .4, .6]

    # sign vec
    sign_vec = [1, 1, -1, 1, 1, 1]

    # applying subtraction
    mid_df = mid_df.sub(sub_vec, axis='columns')

    # multiply them all together and then thru the matrix. Then perform a row sum

    mult_vec = [a*b*c for a,b,c in zip(denom_vec, sign_vec, weight_vec)]

    output_df = mid_df.multiply(mult_vec)

    output_df['offensive_scoring_ability'] = output_df.sum(axis=1)

    output_df = pd.concat([names_df, output_df], axis = 1)

    output_df = output_df.reset_index(drop=True)[['season','week','team','offensive_scoring_ability']]

    return(output_df)

def pipe_defense_scoring_allowance(api_data, trailing_weeks = 5):

    # scoring allow defense

    # get_def_points_per_drive
    # get_def_points_per_RZ
    # get_def_drives_in_turnover
    # get_def_epa_per_pass
    # get_def_epa_per_rush
    # get_def_scr_allowed

    # get_def_points_per_drive: -.4, /3.2
    temp_def_ppd = get_def_points_per_drive(api_data, trailing_weeks)
    # get_def_points_per_RZ: -2, /5
    temp_def_pprz = get_def_points_per_RZ(api_data, trailing_weeks)
    # get_def_drives_in_turnover: /.3
    temp_def_turnovers = get_def_drives_in_turnover(api_data, trailing_weeks)
    # get_def_epa_per_pass: +.7, /1.3
    temp_def_pass_epa = get_def_epa_per_pass(api_data, trailing_weeks)
    # get_def_epa_per_rush: +.6, /.9
    temp_def_rush_epa = get_def_epa_per_rush(api_data, trailing_weeks)
    # get_def_scr_allowed: -25, /45
    temp_def_scr = get_def_scr_allowed(api_data, trailing_weeks)
    # now combining

    dfs = [temp_def_ppd, temp_def_pprz, temp_def_turnovers, temp_def_pass_epa, temp_def_rush_epa, temp_def_scr]

    df_final = ft.reduce(lambda left, right: pd.merge(left, right, on=['season','week','team'], how='left'), dfs)

    names_df = df_final[['season','week','team']]

    mid_df = df_final.drop(['season','week','team'], axis=1)

    # subtract vector
    sub_vec = [.4, 2, 0, -.7, -.6, 25]

    # mult vector
    denom_vec = [3.2, 5, .3, 1.3, .9, 45]
    denom_vec = list(np.reciprocal(denom_vec))

    # weight vec
    weight_vec = [.5, .6, .3, .5, .4, .6]

    # sign vec
    sign_vec = [1, 1, -1, 1, 1, 1]

    # applying subtraction
    mid_df = mid_df.sub(sub_vec, axis='columns')

    # multiply them all together and then thru the matrix. Then perform a row sum

    mult_vec = [a*b*c for a,b,c in zip(denom_vec, sign_vec, weight_vec)]

    output_df = mid_df.multiply(mult_vec)

    output_df['defensive_scoring_allow'] = output_df.sum(axis=1)

    output_df = pd.concat([names_df, output_df], axis = 1)

    output_df = output_df.reset_index(drop = True)[['season','week','team','defensive_scoring_allow']]

    return(output_df)

def pipe_offense_big_play(api_data, next_gen_stats_pass, trailing_weeks = 5):

    # Big play propensity

    # get_pct_pass
    # get_avg_throw_dist
    # get_epa_per_pass
    # get_off_plays_25yd
    # get_off_td_25yd

    # get_pct_pass
    temp_pct_pass = get_pct_pass(api_data, trailing_weeks)
    # get_avg_throw_dist
    temp_avg_throw_dist = get_avg_throw_dist(next_gen_stats_pass, trailing_weeks)
    # get_epa_per_pass
    temp_epa_per_pass = get_epa_per_pass(api_data, trailing_weeks)
    # get_off_plays_25yd
    temp_off_plays_25yd = get_off_plays_25yd(api_data, trailing_weeks)
    # get_off_td_25yd
    temp_off_td_25yd = get_off_td_25yd(api_data, trailing_weeks)

    dfs = [temp_pct_pass, temp_avg_throw_dist, temp_epa_per_pass, temp_off_plays_25yd, temp_off_td_25yd]

    df_final = ft.reduce(lambda left, right: pd.merge(left, right, on=['season','week','team'], how='left'), dfs)

    # setting baseline as zero for certain columns
    df_final['pct_pass'] = df_final['pct_pass'] - .3
    df_final['off_avg_throw_dist'] = df_final['off_avg_throw_dist'] - 4
    df_final['epa_per_pass'] = df_final['epa_per_pass'] + 5

    # vector for denominators
    # length needs to be 5
    denom_vec = [.5, 12, 10, 6, 2.5]
    denom_vec = list(np.reciprocal(denom_vec))

    # vector for negative / positive
    sign_vec = [1, 1, 1, 1, 1]

    # vector for weight
    weight_vec = [.3, .3, .5, .5, .5]

    # multiply them all together and then thru the matrix. Then perform a row sum

    mult_vec = [a*b*c for a,b,c in zip(denom_vec, sign_vec, weight_vec)]

    mult_df = df_final[['pct_pass', 'off_avg_throw_dist', 'epa_per_pass', 'plays_over_25_yd', 'td_over_25_yd']]

    output_df = mult_df.multiply(mult_vec)

    output_df['off_big_play_propensity'] = output_df.sum(axis=1)

    names_bind = df_final[['season','week','team']]

    output_df = pd.concat([names_bind, output_df], axis = 1)

    output_df = output_df.reset_index(drop=True)[['season','week','team', 'off_big_play_propensity']]

    return(output_df)


def pipe_def_big_play(api_data, next_gen_stats_rec, next_gen_stats_rush, trailing_weeks = 5):

    # Big play propensity allowed defense

    # get_def_yards_per_pass
    # get_def_plays_25yd
    # get_def_td_25yd
    # pipe_def_burn_commit

    # get_def_yards_per_pass: -3, / 7
    temp_def_yards_pass = get_def_yards_per_pass(api_data, trailing_weeks)
    # get_def_plays_25yd: -.4, / 5
    temp_def_plays_25 = get_def_plays_25yd(api_data, trailing_weeks)
    # get_def_td_25yd: 0, / 2
    temp_def_td_25 = get_def_td_25yd(api_data, trailing_weeks)
    # pipe_def_burn_commit: -.2, / 1.3
    temp_burn_commit = pipe_def_burn_commit(api_data, next_gen_stats_rec, next_gen_stats_rush, trailing_weeks)

    # now combining

    dfs = [temp_def_yards_pass, temp_def_plays_25, temp_def_td_25, temp_burn_commit]

    df_final = ft.reduce(lambda left, right: pd.merge(left, right, on=['season','week','team'], how='left'), dfs)

    names_df = df_final[['season','week','team']]

    mid_df = df_final.drop(['season','week','team'], axis=1)

    # subtract vector
    sub_vec = [3, .4, 0, .2]

    # mult vector
    denom_vec = [7, 5, 2, 1.3]
    denom_vec = list(np.reciprocal(denom_vec))

    # weight vec
    weight_vec = [.3, .5, .4, 1]

    # sign vec
    sign_vec = [1, 1, 1, -1]

    # applying subtraction
    mid_df = mid_df.sub(sub_vec, axis='columns')

    # multiply them all together and then thru the matrix. Then perform a row sum

    mult_vec = [a*b*c for a,b,c in zip(denom_vec, sign_vec, weight_vec)]

    output_df = mid_df.multiply(mult_vec)

    output_df['defense_big_play_propensity'] = output_df.sum(axis=1)

    output_df = pd.concat([names_df, output_df], axis = 1)

    return(output_df[['season','week','team','defense_big_play_propensity']])

def pipe_garbagetime_epa(api_data, trailing_weeks = 5):

    # pipeline (kind of) for normaltime and garbagetime epa

    prob_cutoff = .2

    # normal
    combined_standard = api_data[(api_data['play_type'].isin(['run','pass'])) & (api_data['wp'] > prob_cutoff) & (api_data['wp'] < (1-prob_cutoff))].groupby(['season','week','posteam'], as_index = False)['epa'].mean().rename(columns = {'epa':'standard_epa'}).sort_values(['season','posteam','week']).reset_index(drop=True)

    # garbage time
    combined_garbage = api_data[(api_data['play_type'].isin(['run','pass'])) & ((api_data['wp'] < prob_cutoff) | (api_data['wp'] > (1-prob_cutoff)))].groupby(['season','week','posteam'], as_index = False)['epa'].mean().rename(columns = {'epa':'garbage_epa'}).sort_values(['season','posteam','week']).reset_index(drop=True)

    # doing the same thing but for overall
    mid_combined_2 = combined_standard.merge(combined_garbage, how = 'inner',on = ['season','week','posteam'])

    # adding the rolling columns
    comb_df = mid_combined_2.groupby(['season','posteam'], as_index = False)[['standard_epa', 'garbage_epa']].rolling(trailing_weeks).mean().rename(columns = {'standard_epa':'normal_epa','garbage_epa':'garbagetime_epa'})

    output_df = pd.concat([mid_combined_2, comb_df[['normal_epa','garbagetime_epa']]], axis = 1)[['season','week','posteam','normal_epa','garbagetime_epa']].rename(columns = {'posteam':'team', 'normal_epa':'normaltime_epa'})

    return(output_df.reset_index(drop=True))

def pipe_overall_coaching(api_data, trailing_weeks = 5):

    # pipe_offense_coaching_ability
    # pipe_defense_coaching_ability

    # pipe_offense_coaching_ability
    temp_off_coaching = pipe_offense_coaching_ability(api_data, trailing_weeks)
    # pipe_defense_coaching_ability
    temp_def_coaching = pipe_defense_coaching_ability(api_data, trailing_weeks)

    mid_df = temp_off_coaching.merge(temp_def_coaching, on = ['season','week','team'], how = 'left')

    mid_df['def_coaching'] = 1.5 + mid_df['def_coaching']

    mid_df['overall_coaching'] = .35 * mid_df['def_coaching'] + .65 * mid_df['off_coaching']

    output_df = mid_df[['season','week','team','overall_coaching']]

    return(output_df)


