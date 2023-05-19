import math as m

# Example of input_off_data used with default values below
# |------|--------|------|---------|---------|----------|------------------|--------------------------|-------------------------|
# |      | season | week | posteam | defteam | position | rusher_player_id | player_name              | z_epa                   |
# |------|--------|------|---------|---------|----------|------------------|--------------------------|-------------------------|
# |    0 |   2021 |    1 | ARI     | TEN     | QB       |       00-0035228 | Kyler Murray             |    -0.32008300916945287 |
# |------|--------|------|---------|---------|----------|------------------|--------------------------|-------------------------|
# ...
#
# Example of input_def_data used with default values below
# |------|--------|------|---------|---------|----------|------------------------|
# |      | season | week | posteam | defteam | position | z_epa                  |
# |------|--------|------|---------|---------|----------|------------------------|
# |    0 |   2021 |    1 | ARI     | TEN     | QB       |    -0.3047991929363788 |


def calculate_elo_metric(
    input_off_data,
    input_def_data,
    order_cols = ['season','week'], # Should be standard
    # Unique level that perf col was aggregated at for offense
    off_gb_cols = ['posteam','defteam','position','rusher_player_id','player_name'],
    # Unique level that perf col was aggregated at for defense
    def_gb_cols = ['posteam','defteam','position'],
    # Update elo from most recent record matching what criteria
    off_lookup_values = ['position','rusher_player_id'],
    # Update elo from most recent record matching what criteria
    def_lookup_values = ['position','defteam'],
    # Column with x extension will be whichever has more gb_cols between off and defense...
    # offense if equal
    off_perf_col = 'z_epa_x',
    # Note: function assumes it is inverted on defense
    def_perf_col = 'z_epa_y',
    elo_multiplier = 3,
    elo_power = 2,
    elo_season_reset = 0.5,
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

    def elo_adj(elo_1, elo_2, z_perf ,multiplier_units = 10, power = 2):
        # How much would I win / lose betting on offense?
        odds = elo_2 / elo_1
        win = 1 if z_perf > 0 else 0
        if win == 1:
            ret = (odds * multiplier_units)**power
        else:
            ret = -1 * (multiplier_units)**power
        off_ret = ret * abs(z_perf)
        # How much would I win / lose taking a bet on the defense?
        odds = elo_1 / elo_2
        win = 1 if z_perf < 0 else 0
        if win == 1:
            ret = -1 * (odds * multiplier_units)**power
        else:
            ret = (multiplier_units)**power
        def_ret = ret * abs(z_perf)
        return (off_ret + def_ret) / 2

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
                off_elo_val = caching_dict[off_tup] if not m.isnan(caching_dict[off_tup]) \
                    else elo_base
        if row['def_elo'] is None:
            def_tup = tuple([row[key] for key in def_lookup_values] + ['def'])
            # Check if tup in keys of caching_dict
            found = def_tup in caching_dict.keys()
            # If it is found in keys, set elo_val to the value
            if found:
                def_elo_val = caching_dict[def_tup] if not m.isnan(caching_dict[def_tup]) \
                    else elo_base
        # Reset ELO towards base if first appearance of season
        if row['def_appearance'] == 1: def_elo_val = (def_elo_val - elo_base) * elo_season_reset + \
            elo_base
        if row['off_appearance'] == 1: off_elo_val = (off_elo_val - elo_base) * elo_season_reset + \
            elo_base
        # Calculate ELO to update
        off_elo_val_next = off_elo_val + elo_adj(
            off_elo_val,
            def_elo_val,
            z_perf = row[off_perf_col],
            multiplier_units=elo_multiplier,
            power=elo_power
        )
        def_elo_val_next = def_elo_val + elo_adj(
            def_elo_val,
            off_elo_val,
            z_perf = row[def_perf_col],
            multiplier_units=elo_multiplier,
            power=elo_power
        )

        elo_df.at[index, 'off_elo'] = off_elo_val
        elo_df.at[index, 'def_elo'] = def_elo_val
        elo_df.at[index, 'off_elo_next'] = off_elo_val_next
        elo_df.at[index, 'def_elo_next'] = def_elo_val_next

        # Store off and defensive values in caching
        caching_dict[off_tup] = off_elo_val_next
        caching_dict[def_tup] = def_elo_val_next
    return elo_df




