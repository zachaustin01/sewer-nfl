'''
Variable configurations (to be loaded as default args in catalog)
'''

from warehouse.pipelines.pbp.setup import setup_pbp
# Set Variables

class Configuration():
    def __init__(self):

        self.ONLY_REGULAR_SEASON = True
        self.STARTING_YEAR = 2016
        self.ENDING_YEAR = 2022

        self.TRAILING_WEEKS = 5

        self.pbp_api_data, self.ngs = setup_pbp(
            only_regular_season=self.ONLY_REGULAR_SEASON,
            starting_year=self.STARTING_YEAR,
            ending_year=self.ENDING_YEAR
        )
