

def calculate_elo_metric(
    input_data,
    offensive_entity_column = "gsis_id",
    defensive_entity_column = "opponent",
    performance_column = "z_perf",
    order_columnns = ['season','week'],
    meta_columns = ['full_name','team']
):
    return 0