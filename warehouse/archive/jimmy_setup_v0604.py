only_regular_season = True
starting_year = 2016
ending_year = 2022

# setting up the pbp data

years_included = range(starting_year, ending_year+1)


api_data = nflreadr.import_pbp_data(years_included)
api_data = api_data.assign(play_counter = 1)

if only_regular_season:
    api_data = api_data[api_data['week']<=18]
    

 #DOING NEXTGEN STAT SHIT

next_gen_stats_rush = nflreadr.import_ngs_data("rushing", years_included)
next_gen_stats_rec = nflreadr.import_ngs_data("receiving", years_included)
next_gen_stats_pass = nflreadr.import_ngs_data("passing", years_included)

# removing week 0 which is a season summary

next_gen_stats_rush = next_gen_stats_rush[next_gen_stats_rush['week'] > 0]
next_gen_stats_rec = next_gen_stats_rec[next_gen_stats_rec['week'] > 0]
next_gen_stats_pass = next_gen_stats_pass[next_gen_stats_pass['week'] > 0]

# replacing some name errors
next_gen_stats_rec['team_abbr'] = next_gen_stats_rec['team_abbr'].replace(['LAR'], 'LA')

next_gen_stats_rush['team_abbr'] = next_gen_stats_rush['team_abbr'].replace(['LAR'], 'LA')

next_gen_stats_pass['team_abbr'] = next_gen_stats_pass['team_abbr'].replace(['LAR'], 'LA')

# adding defenses to all three
getting_schedule = api_data[['season','week','posteam','defteam']].drop_duplicates().reset_index(drop=True)

next_gen_stats_rush = next_gen_stats_rush.merge(getting_schedule, how='left',left_on=['season','week', 'team_abbr'],
                         right_on=['season','week', 'posteam']).drop(['posteam'], axis=1)

next_gen_stats_rec = next_gen_stats_rec.merge(getting_schedule, how='left',left_on=['season','week', 'team_abbr'],
                         right_on=['season','week', 'posteam']).drop(['posteam'], axis=1)

next_gen_stats_pass = next_gen_stats_pass.merge(getting_schedule, how='left',left_on=['season','week', 'team_abbr'],
                         right_on=['season','week', 'posteam']).drop(['posteam'], axis=1)

   
    