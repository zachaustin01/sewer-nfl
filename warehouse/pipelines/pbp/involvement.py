"""Module Docstring"""

import pandas as pd
pd.options.mode.chained_assignment = None

# players_involved(
#     player_involvement_df=player_involvement_df,
#     play_id=40.0,
#     game_id = '2021_01_ARI_TEN',
#     side = 'offense'
# )

def players_involved(player_involvement_df,
                     play_id,
                     game_id,
                     side_var = 'variable',
                     side = 'offense' # offense, defense, both
                     ):
    """Function Docstring"""
    res = player_involvement_df[
            (player_involvement_df['play_id']==play_id) & \
            (player_involvement_df['game_id']==game_id)
        ]

    if side != 'both':
        s = 'o' if side == 'offense' else 'd'
        res = res[(res[side_var].astype(str).str[0] == s)]

    return(
        list(res.iloc[:,-1])
    )


# player_involved(player_involvement_df=player_involvement_df,
#                 play_id = 40.0,
#                 game_id = '2021_01_ARI_TEN',
#                 player_id = '00-0032560')

def player_involved(
    player_involvement_df,
    player_id, # Required
    play_id = None, # Optional
    game_id = None, # Optional
    id_col = 'value' # Required
):
    """Function Docstring"""
    res = player_involvement_df[
        (player_involvement_df[id_col]==player_id)
    ]
    if play_id is not None:
        res = res[(res['play_id']==play_id)]
    if game_id is not None:
        res = res[(res['game_id']==game_id)]

    return res


# Function to generate a dataframe with involvement of offensive and defensive players
def player_involvement(
    pbp_data
):
    """Function Docstring"""
    columns = [
        'play_id',
        'game_id',
        'home_team',
        'away_team',
        'posteam',
        'posteam_type',
        'offense_players',
        'defense_players'
    ]
    reduced_data = pbp_data[columns]
    reduced_data[[
        f'o{i}' for i in range(1,12)
    ]] = reduced_data['offense_players'].str.split(';',expand = True).iloc[:,:11]
    reduced_data[[
        f'd{i}' for i in range(1,12)
    ]] = reduced_data['defense_players'].str.split(';',expand = True).iloc[:,:11]
    reduced_data.drop(['offense_players','defense_players'],
                      axis = 1,
                      inplace = True)


    return(
        pd.melt(
        reduced_data,
        id_vars = ['play_id',
            'game_id',
            'home_team',
            'away_team',
            'posteam',
            'posteam_type']
            )
        )
