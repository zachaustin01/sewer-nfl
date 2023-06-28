'''
Variable configurations (to be loaded as default args in catalog)
'''

# Set Variables
ONLY_REGULAR_SEASON = True
STARTING_YEAR = 2016
ENDING_YEAR = 2022

TRAILING_WEEKS = 5

from warehouse.pipelines.pbp.setup import setup_pbp

pbp_api_data, ngs = setup_pbp(
    only_regular_season=ONLY_REGULAR_SEASON,
    starting_year=STARTING_YEAR,
    ending_year=ENDING_YEAR
)

