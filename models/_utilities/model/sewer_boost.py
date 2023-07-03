
import pickle
import time
from copy import deepcopy
import xgboost as xgb
import numpy as np
import pandas as pd
from xgboost import XGBClassifier
from sklearn.metrics import accuracy_score
from sklearn.model_selection import GridSearchCV,RandomizedSearchCV,train_test_split
from sklearn.metrics import accuracy_score,f1_score,roc_auc_score,confusion_matrix,roc_curve
import seaborn as sns

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
                 ):

        self.training_data = training_data
        self.test_years = test_years
        self.response = response
        self.top_N = None # Will be filled later once assessed
        self.results_dict = {}
        self.best_params = None

        self.predictors = [c for c in self.training_data.columns if c not in \
                           NUMERIC_META_COLS and c in self.training_data.select_dtypes(np.number)]

        self.train_test_split()
        self.grid_search_for_params()
        self.build_model()
        self.assess_on_test()

    def build_model(self):

        best_auc = 0
        for m in self.results_dict.keys():
            if self.results_dict[m]['test roc auc score'] > best_auc:
                self.best_params = self.results_dict[m]['best_params']

        self.model = XGBClassifier(objective='binary:logistic',
                            booster='gbtree',
                            eval_metric='auc',
                            tree_method='hist',
                            grow_policy='lossguide')
        self.model.set_params(**self.best_params)
        self.model.fit(self.X_train, self.y_train)

    def grid_search_for_params(self):
        xgbc0 = XGBClassifier(objective='binary:logistic',
                            booster='gbtree',
                            eval_metric='auc',
                            tree_method='hist',
                            grow_policy='lossguide')
        self.params = {"objective": "multi:softprob", "tree_method": "gpu_hist", "num_class": 2}
        xgbc0.fit(self.X_train, self.y_train)

        # Pull attributes from self for easier references
        X_train = self.X_train
        X_test = self.X_test
        y_train = self.y_train
        y_test = self.y_test

        #extracting default parameters from benchmark model
        default_params = {}
        gparams = xgbc0.get_params()
        #default parameters have to be wrapped in lists - even single values - so GridSearchCV can take them as inputs
        for key in gparams.keys():
            gp = gparams[key]
            default_params[key] = [gp]
        #benchmark model. Grid search is not performed, since only single values are provided as parameter grid.
        #However, cross-validation is still executed
        clf0 = GridSearchCV(estimator=xgbc0, scoring='accuracy', param_grid=default_params, return_train_score=True, verbose=1, cv=3)
        clf0.fit(X_train, y_train.values.ravel())
        df = pd.DataFrame(clf0.cv_results_)
        #predictions - inputs to confusion matrix
        train_predictions = clf0.predict(X_train)
        test_predictions = clf0.predict(X_test)
        #confusion matrices
        cfm_train = confusion_matrix(y_train, train_predictions)
        cfm_test = confusion_matrix(y_test, test_predictions)
        #accuracy scores
        accs_train = accuracy_score(y_train, train_predictions)
        accs_test = accuracy_score(y_test, test_predictions)
        #Area Under the Receiver Operating Characteristic Curve
        test_ras = roc_auc_score(y_test, clf0.predict_proba(X_test)[:,1])
        #best parameters
        bp = clf0.best_params_
        #storing computed values in results dictionary
        self.results_dict['xgbc0'] = {'iterable_parameter': np.nan,
                         'classifier': deepcopy(clf0),
                         'cv_results': df.copy(),
                         'cfm_train': cfm_train,
                         'cfm_test': cfm_test,
                         'train_accuracy': accs_train,
                         'test_accuracy': accs_test,
                         'test roc auc score': test_ras,
                         'best_params': bp}
        params = deepcopy(default_params)
        #setting grid of selected parameters for iteration
        param_grid = {'gamma': [0,0.1,0.2,0.4,0.8],
                    'learning_rate': [0.01, 0.03, 0.06, 0.1,],
                    'max_depth': [5,6,7,8,9,10],
                    'n_estimators': [100,115,130,150],
                    'reg_alpha': [0,0.1,0.2,0.4,0.8,1.6,3.2,6.4],
                    'reg_lambda': [0,0.1,0.2,0.4,0.8,1.6,3.2,6.4]}
        #start time
        t0 = time.time()
        #No. of jobs
        gcvj = np.cumsum([len(x) for x in param_grid.values()])[-1]

        #iteration loop. Each selected parameter iterated separately
        for i,grid_key in enumerate(param_grid.keys()):

            #variable for measuring iteration time
            loop_start = time.time()

            #creating param_grid argument for GridSearchCV:
            #listing grid values of current iterable parameter and wrapping non-iterable parameter single values in list
            for param_key in params.keys():
                if param_key == grid_key:
                    params[param_key] = param_grid[grid_key]
                else:
                    #use best parameters of last iteration
                    try:
                        param_value = [clf.best_params_[param_key]]
                        params[param_key] = param_value
                    #use benchmark model parameters for first iteration
                    except:
                        param_value = [clf0.best_params_[param_key]]
                        params[param_key] = param_value

            #classifier instance of current iteration
            xgbc = xgb.XGBClassifier(**default_params)

            #GridSearch instance of current iteration
            clf = GridSearchCV(estimator=xgbc, param_grid=params, scoring='accuracy', return_train_score=True, verbose=1, cv=3)
            clf.fit(X_train, y_train.values.ravel())

            #results dataframe
            df = pd.DataFrame(clf.cv_results_)

            #predictions - inputs to confusion matrix
            train_predictions = clf.predict(X_train)
            test_predictions = clf.predict(X_test)
            #confusion matrices
            cfm_train = confusion_matrix(y_train, train_predictions)
            cfm_test = confusion_matrix(y_test, test_predictions)
            #accuracy scores
            accs_train = accuracy_score(y_train, train_predictions)
            accs_test = accuracy_score(y_test, test_predictions)
            #Area Under the Receiver Operating Characteristic Curve
            test_ras = roc_auc_score(y_test, clf.predict_proba(X_test)[:,1])
            #best parameters
            bp = clf.best_params_
            #storing computed values in results dictionary
            self.results_dict[f'xgbc{i+1}'] = {'iterable_parameter': grid_key,
                                        'classifier': deepcopy(clf),
                                        'cv_results': df.copy(),
                                        'cfm_train': cfm_train,
                                        'cfm_test': cfm_test,
                                        'train_accuracy': accs_train,
                                        'test_accuracy': accs_test,
                                        'test roc auc score': test_ras,
                                        'best_params': bp}

            #variable for measuring iteration time
            elapsed_time = time.time() - loop_start
            print(f'iteration #{i+1} finished in: {elapsed_time} seconds')

        #stop time
        t1 = time.time()


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
        if self.top_N is None:
            top_N = self.get_test_results(most_confident= most_confident, save = save)
        else:
            top_N = self.top_N
        return round(sum(top_N['win'] / len(top_N['win'])),2)

    def get_test_accuracy_drop_off(self, top_N_range = list(range(1,7)), threshold = 0.5):
        '''
        Return percentage of correct picks across various top N values
        '''
        accuracies = []
        for N in top_N_range:
            accuracies.append(self.get_test_accuracy(most_confident = N, save = False))

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

def plot_bar(
        x_data,
        y_data,
        title,
        xlabel,
        ylabel,
        h_line = None
):
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

