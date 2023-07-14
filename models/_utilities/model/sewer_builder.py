import sys,os
import random
import pandas as pd
import numpy as np

REPO_NAME = 'sewer-nfl'
CWD = str(os.getcwd())
REPO_DIR = CWD[:CWD.find(REPO_NAME)+len(REPO_NAME)]
sys.path.insert(0,REPO_DIR)

from models._utilities.data.pipe_layer import build_training_dataset
from models._utilities.model.sewer_boost import NUMERIC_META_COLS
from warehouse.archive.variable_selection import select_variable_subset

REGEN = False
if REGEN:
    from warehouse.config import Configuration # At model level, swictch this to model's config
    config = Configuration()
    T = build_training_dataset(config)
else: T = build_training_dataset()

from models._utilities.model.sewer_boost import Model

class Sewer:
    '''

    Class to generate, evaluate, and promote models meeting acceptance criteria for model registry.

    '''
    def __init__(
            self,
            FUNCTION_CATALOG,
            batch_size,
            var_selection_type="Random",
            param_selection_type="Fixed",
            n_vars=list(range(8,10)),
            model_promotion_acc_threshold = 6,
            max_acc_slope_threshold = 0
    ):
        self.batch_size = batch_size
        self.var_selection_type = var_selection_type
        self.param_selection_type = param_selection_type
        self.n_vars = n_vars
        self.function_catalog = FUNCTION_CATALOG
        self.models = []
        self.crs = None
        self.t = T
        self.promotion_threshold = model_promotion_acc_threshold
        self.max_slope = max_acc_slope_threshold

    def select_vars(self):
        '''
        Method designed to select variables based on different configurations
        '''
        if self.var_selection_type=="Random":
            vars = select_variable_subset(
                FUNCTION_CATALOG=self.function_catalog,
                total_variables=random.choice(self.n_vars)
            ) + ['team']
            vars2 = [item for sublist in [[f'home_{v}',f'away_{v}'] for v in vars] for item in sublist]
            vars2.extend(NUMERIC_META_COLS)
            return vars2

    def select_params(self):
        '''
        Method designed to select model parameters based on different configurations
        '''
        if self.param_selection_type=="Fixed":
            return {
                            'objective':'binary:logistic',
                            'gamma':0.4,
                            'learning_rate':0.005,
                            'max_depth':10,
                            'n_estimators':100,
                            'tree_method':'hist',
                            'grow_policy': 'lossguide',
                            'reg_alpha': 0.5,
                            'reg_lambda': 0.5
                        }

    def promote(self):
        if self.crs is None:
            print('Generate models before trying to promote')
        else:
            valid_crs = self.crs[(self.crs['Value']>self.promotion_threshold) &
                            (self.crs['Slope']<self.max_slope)]
            indices_to_promote = list(valid_crs.index)
            for index in indices_to_promote:
                model = self.models[index]
                model.register_model()

    def generate(self, verbose, auto_promote = False):
        for i in range(self.batch_size):
            if verbose and (i+1) % 5 == 0: print(i+1)
            try:
                vars = self.select_vars()
                params = self.select_params()
                m = Model(
                    training_data = self.t[vars],
                    params = params
                )
            except:
                if verbose: print(f'Failure generating model #{i}')
                pass
            self.models.append(m)
        if verbose: print('Assembling comparison rows')
        crs = [x.comparison_row() for x in self.models]
        left_scale_weight = 0.5
        res = pd.DataFrame(crs)
        # Value: assessed accuracy of the model based on its performance at differing values of top_N
        res['Value'] = res.apply(lambda x: np.dot(x,[(a + 1)**left_scale_weight for a in list(x.index)]), axis=1)
        # Slope: fitted slope of accuracy metrics at increasing values of N
        res['Slope'] = res.apply(lambda x: np.polyfit(list(x.index[:-1]),list(x[:-1]),3)[0],axis = 1)
        self.crs = res.sort_values('Value',ascending=False)
