import sys,os
import random

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
            n_vars=list(range(8,10))
    ):
        self.batch_size = batch_size
        self.var_selection_type = var_selection_type
        self.param_selection_type = param_selection_type
        self.n_vars = n_vars
        self.function_catalog = FUNCTION_CATALOG
        self.models = []
        self.t = T

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

    def generate(self, verbose):
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
