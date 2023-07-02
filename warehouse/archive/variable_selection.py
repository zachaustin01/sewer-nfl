import pandas as pd
import numpy as np

# the actual call to get the full catalog
from warehouse.catalog import FUNCTION_CATALOG


################################################################################################

# input variables into the function being the function catalog, the total number of variables per team ()

def select_variable_subset(inp_dict = FUNCTION_CATALOG, total_variables = 12, favor_pipes = 2.5):

    # Turning dictionary into df and dropping unnecessary columns
    catalog_df = pd.DataFrame.from_dict(FUNCTION_CATALOG).T.drop(['func','params'], axis = 1).reset_index().rename(columns = {'index':'variable'})

    # resetting the variable name into a new column
    catalog_df['output_columns'] = np.where(pd.isna(catalog_df['output_columns']), catalog_df['variable'], catalog_df['output_columns'])

    # turning the list variable columns into their own col (by exploding the ones that have two variables)
    catalog_df = catalog_df.explode('output_columns')
    
    off_num_vars = round(total_variables / 2)
    

    ########################################################## set up / cleaning done

    # applying the overweight to the pipe variables

    catalog_df['var_importance'] = np.where(catalog_df['type'] == 'pipeline', catalog_df['var_importance'] * favor_pipes, catalog_df['var_importance'])

    # first grabbing offensive variables, and randomly sampling

    off_df = catalog_df[catalog_df['ball_side'] == 'off'].sample(n=off_num_vars, weights = 'var_importance').reset_index(drop=True)
    off_variables = off_df['output_columns'].tolist()
    
    # directly pulling the counterpart defensive variables from offensive subset

    counterpart_variables = off_df[off_df['var_counterpart'].notnull()]['var_counterpart'].tolist()
    
    if len(counterpart_variables) != len(off_variables):
    
        # adjusting the variable weight of the defensive variables according to the category of variables chosen in the offensive subset

        freq_adjustment = pd.DataFrame(off_df['var_category'].value_counts(normalize = True)).reset_index().rename(columns = {'var_category':'adj_val', 'index':'var_category'})

        # joining the defensive variables with the freq adjustment, and sampling the number of variables remaining after counterparts have been chosen

        mid_def_df = catalog_df[(catalog_df['ball_side'] == 'def') & (~catalog_df['output_columns'].isin(counterpart_variables))].merge(freq_adjustment, on = ['var_category'], how = 'left')
        mid_def_df['var_importance'] = mid_def_df['var_importance'] * mid_def_df['adj_val']
        def_variables = mid_def_df.sample(n = len(off_variables) - len(counterpart_variables), weights = 'var_importance')['output_columns'].tolist()

        # all column variable output

        all_variables = [off_variables, counterpart_variables, def_variables]
        
    else:
        
        all_variables = [off_variables, counterpart_variables]

    all_variables = [item for sublist in all_variables for item in sublist]

    return(all_variables)




