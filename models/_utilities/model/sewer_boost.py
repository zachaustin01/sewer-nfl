
import pickle
import random
import string
from datetime import datetime
from copy import deepcopy
import xgboost as xgb
import numpy as np
import pandas as pd
from xgboost import XGBClassifier, plot_importance
from sklearn.metrics import accuracy_score
from sklearn.model_selection import GridSearchCV,RandomizedSearchCV,train_test_split
from sklearn.metrics import accuracy_score,f1_score,roc_auc_score,confusion_matrix,roc_curve

REPO_NAME = 'sewer-nfl'
import sys, os
cwd = str(os.getcwd())
repo_dir = cwd[:cwd.find(REPO_NAME)+len(REPO_NAME)]
sys.path.insert(0,repo_dir)

import warnings

def fxn():
    warnings.warn("deprecated", DeprecationWarning)

with warnings.catch_warnings():
    warnings.simplefilter("ignore")
    fxn()

import matplotlib.pyplot as plt

NUMERIC_META_COLS = [
    'season',
    'week',
    'home_score',
    'away_score',
    'home_cover',
    'within_three',
    'missing_N'
]

class Model:
    '''

    Model object designed to generate predictions on a series of live or test data
    - Will be stored as a .pkl file

    '''

    def __init__(self,
                 training_data,
                 test_years = [2022],
                 response = 'home_cover', # Options: ['home_cover','spread_line','within_three']
                 params = None
                 ):

        letters = string.ascii_lowercase

        self.model_name = f'{datetime.now().strftime("%Y%m%d%H%M%S")}_{"".join(random.choices(letters,k=5))}'

        self.training_data = training_data
        self.test_years = test_years
        self.response = response
        self.top_N = None # Will be filled later once assessed
        self.results_dict = {}
        self.best_params = None

        self.predictors = [c for c in self.training_data.columns if c not in \
                           NUMERIC_META_COLS and c in self.training_data.select_dtypes(np.number)]

        self.train_test_split()
        if params is not None: self.build_model(params=params)
        else: self.build_model()
        self.assess_on_test()

    def comparison_row(self):
        return self.get_test_accuracy_drop_off_data()

    def assess_predictor_importance(self):

        return pd.DataFrame({'importance':self.model.feature_importances_,'feature':self.predictors})\
            .sort_values('importance',ascending=False)

    def build_model(self,
        params = {
            'objective':'binary:logistic',
            'gamma':0.4,
            'learning_rate':0.005,
            'max_depth':10,
            'n_estimators':90,
            'tree_method':'hist',
            'grow_policy': 'lossguide',
            'reg_alpha': 0.5,
            'reg_lambda': 0.5
        }):

        self.best_params = params
        self.model = XGBClassifier()
        self.model.set_params(**self.best_params)
        self.model.fit(self.X_train, self.y_train)

    def train_test_split(
            self,
            mode = 'years'
    ):
        if mode == 'years':
            mask = self.training_data['season'].isin(self.test_years)
            self.train_data = self.training_data[~mask]
            self.test_data = self.training_data[mask]

        self.X_train = self.train_data[self.predictors]
        self.X_test = self.test_data[self.predictors]
        self.y_train = self.train_data[self.response]
        self.y_test = self.test_data[self.response]
        self.dtrain = xgb.DMatrix(self.X_train, self.y_train, enable_categorical=True)
        self.dtest = xgb.DMatrix(self.X_test, self.y_test, enable_categorical=True)

    def assess_on_test(self):
        self.y_preds = self.model.predict(self.X_test)
        self.y_proba = [x[1] for x in self.model.predict_proba(self.X_test)]

    def get_test_results(self, most_confident = 5, save = True):
        '''
        Return dataframe of test results
        '''
        cols = {'y_pred' : self.y_preds,'y_proba':self.y_proba}
        data_cols = {key:self.test_data[key] for key in self.test_data.columns}
        cols.update(data_cols)
        test_res = pd.DataFrame(cols)
        test_res['conf'] = test_res.apply(
            lambda x: 1 - x['y_proba'] if x['y_pred'] == 0 else x['y_proba'], axis = 1
        )
        mask = test_res.groupby('week')['conf'].nlargest(most_confident).index
        top_N = test_res[test_res.index.isin(mask.droplevel(0))]\
            [['season','week','y_pred','y_proba','conf','home_score',
              'away_score','home_cover','home_team','away_team']]

        top_N['win'] = top_N['y_pred']==top_N['home_cover']
        top_N['pick'] = top_N.apply(lambda x: x['home_team'] if x['y_pred']==1 else x['away_team'],
                                    axis=1)
        if save: self.top_N = top_N
        return top_N

    def get_test_accuracy(self, most_confident = 5, save = True):
        '''
        Return percentage of correct picks if using top N in confidence per week
        '''
        top_N = self.get_test_results(most_confident= most_confident, save = save)
        return round(sum(top_N['win'] / len(top_N['win'])),2)

    def get_test_accuracy_drop_off_data(self, top_N_range = list(range(1,7)), threshold = 0.5):
        '''
        Return percentage of correct picks across various top N values
        '''
        accuracies = []
        for N in top_N_range:
            accuracies.append(self.get_test_accuracy(most_confident = N, save = False))
        return accuracies

    def get_test_accuracy_drop_off(self, top_N_range = list(range(1,7)), threshold = 0.5):
        accuracies = self.get_test_accuracy_drop_off_data(top_N_range,threshold)
        return plot_bar(
            x_data=top_N_range,
            y_data=accuracies,
            title="Win Rate Using Top N Picks Per Week",
            xlabel='Top N',
            ylabel='Accuracy',
            h_line = threshold
        )

    def get_test_accuracy_by_week(self, most_confident = 5, threshold = 0.5):
        self.get_test_accuracy(most_confident = most_confident, save = True)
        top_N = self.top_N
        wins = top_N.groupby('week')['win'].mean()
        return plot_bar(
            x_data=list(wins.index),
            y_data=list(wins),
            title=f'Winning Percentage by Week Using Top {most_confident} Picks',
            xlabel='Week',
            ylabel='Accuracy',
            h_line=threshold
        )

    def register_model(self):
        '''

        Create pickle-able class from this class

        '''
        pk = Pickleable(model = self)
        with open(f'{repo_dir}/models/_registry/{pk.name}.pkl','wb') as file:
            pickle.dump(pk,file)
            print(f'Registered model as {pk.name}')

class Pickleable:
    def __init__(self,model):
        self.name = model.model_name
        self.model = model.model
        self.predictors = model.predictors
        self.test_years = model.test_years
        self.training_data = model.training_data
        self.best_params = model.best_params

def plot_bar(
        x_data,
        y_data,
        title,
        xlabel,
        ylabel,
        h_line = None
):
    y_data = [round(v,2) for v in y_data]
    freq_series = pd.Series(y_data)
    x_labels = list(x_data)
    plt.figure(figsize=(12, 8))
    ax = freq_series.plot(kind="bar")
    ax.set_title(title)
    ax.set_xlabel(xlabel)
    ax.set_ylabel(ylabel)
    ax.set_xticklabels(x_labels)
    rects = ax.patches
    labels = y_data
    for rect, label in zip(rects, labels):
        height = rect.get_height()
        ax.text(
            rect.get_x() + rect.get_width() / 2, height, label, ha="center", va="bottom"
        )
    if h_line is not None:    plt.axhline(y = h_line, color = 'r', linestyle = '--')
    return plt.show()

