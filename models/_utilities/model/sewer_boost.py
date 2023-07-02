
import pickle
from xgboost import XGBClassifier
from sklearn.metrics import accuracy_score

class Cystal_Ball:
    '''

    Model object designed to generate predictions on a series of live or test data
    - Will be stored as a .pkl file

    '''

    def __init__(self,
                 training_data,
                 test_years = [2022],
                 response = 'home_cover', # Options: ['home_cover','spread_line','within_three']
                 ):

        self.training_data = training_data
        self.test_years = test_years
        self.train_test_split()
        self.model = XGBClassifier()

    def train_test_split(
            self,
            mode = 'years'
    ):
        if mode == 'years':
            mask = self.training_data['season'].isin(self.test_years)
            self.train_data = self.training_data[~mask]
            self.test_data = self.training_data[mask]



