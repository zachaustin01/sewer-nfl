import math as m
import pandas as pd

def calculate_elo_metric(
    input_off_data,
    input_def_data,
    order_cols = ['season','week'],
    off_gb_cols = ['posteam','defteam','position','rusher_player_id','player_name'], # At what level are we AGGREGATING offensive performance
    def_gb_cols = ['posteam','defteam','position'], # Unique level to AGGREGATE at for defense
    off_lookup_values = ['position','rusher_player_id'], 
    def_lookup_values = ['position','defteam'],
    off_perf_col = 'z_epa_x',
    def_perf_col = 'z_epa_y', # Note: function assumes it is inverted on defense  
    
    elo_base = 2000 # Arbitrary value
):
    off_df = input_off_data
    def_df = input_def_data

    min_gb_cols = def_gb_cols if len(def_gb_cols) <= len(off_gb_cols) else off_gb_cols

    # Join on smallest grouping
    elo_df = off_df.merge(
        def_df,
        on = order_cols + min_gb_cols,
        how = 'left' if len(def_gb_cols) <= len(off_gb_cols) else 'right'
    )
    elo_df['off_elo'], elo_df['def_elo'] = None, None
    elo_df['off_elo_next'], elo_df['def_elo_next'] = None, None

    caching_dict = {}

    def elo_multiplier(elo_1, elo_2, power = 1.1, multiplier = 10):
        return (elo_1 / elo_2) ** power * multiplier
    
    elo_df = elo_df.sort_values(by = order_cols)

    for index, row in elo_df.iterrows():
        off_elo_val = elo_base
        def_elo_val = elo_base
        # Check if elo is none
        if row['off_elo'] is None:
            off_tup = tuple([row[key] for key in off_lookup_values] + ['off'])
            # Check if tup in keys of caching_dict
            found = off_tup in caching_dict.keys()
            # If it is found in keys, set elo_val to the value
            if found: 
                off_elo_val = caching_dict[off_tup] if not m.isnan(caching_dict[off_tup]) else elo_base
        if row['def_elo'] is None:
            def_tup = tuple([row[key] for key in def_lookup_values] + ['def'])
            # Check if tup in keys of caching_dict
            found = def_tup in caching_dict.keys()
            # If it is found in keys, set elo_val to the value
            if found: 
                def_elo_val = caching_dict[def_tup] if not m.isnan(caching_dict[def_tup]) else elo_base  
        # Calculate ELO to update
        off_elo_val_next = elo_multiplier(off_elo_val, def_elo_val) * row[off_perf_col] + off_elo_val
        def_elo_val_next = -1 * elo_multiplier(def_elo_val, off_elo_val) * row[def_perf_col] + def_elo_val
        
        elo_df.at[index, 'off_elo'] = off_elo_val
        elo_df.at[index, 'def_elo'] = def_elo_val
        elo_df.at[index, 'off_elo_next'] = off_elo_val_next
        elo_df.at[index, 'def_elo_next'] = def_elo_val_next

        # Store off and defensive values in caching
        caching_dict[off_tup] = off_elo_val_next
        caching_dict[def_tup] = def_elo_val_next
    return elo_df




