""" ------------------------------------------------------------------------
-- Author:      Behzad Asadi                                                 
-- Description: data profiling using great expectations   
-- -------------------------------------------------------------------------
-- VERSIONS DATE         WHO                        DESCRIPTION            
-- 1.00     08/01/2024   Behzad Asadi               Initial release        
-- ----------------------------------------------------------------------"""

#%% importing libraries

import numpy as np
import pandas as pd
import great_expectations as gx
import json
import os
import sys
from great_expectations import validator

#%% defining custom expectations

# from great_expectations.dataset import PandasDataset, MetaPandasDataset

# class CustomPandasDataset(PandasDataset):

#     @MetaPandasDataset.multicolumn_map_expectation
#     def expect_multicolumn_values_to_not_be_null(self, column_list, ignore_row_if = 'never'):
#         return ~column_list.isnull().all(axis=1)

# class CustomPandasDataset(PandasDataset):

#     @MetaPandasDataset.multicolumn_map_expectation
#     def expect_column_pair_sum_to_be_less_than(self, column_list, value=None):
#         return column_list.sum(axis=1) < value

# df = pd.DataFrame({
#     'col1': [None, 2, 3, None],
#     'col2': [None, 5, 6, None],
#     'col3': [1, 8, 9, None]
# })

# df_ge = CustomPandasDataset(df)
# # Use your custom expectation
# df_ge.expect_column_pair_sum_to_be_less_than(column_list=['col1', 'col2'], value=10)
# results = df_ge.validate(result_format='COMPLETE')

#%%

def data_validation(input_dataframe):
    
    expectation_suite_path = './src/data_validation/pfr_expectation_suite.json'
    with open(expectation_suite_path, 'r') as json_file:
            expectation_suite = json.load(json_file)
    json_file.close()

    df = gx.from_pandas(input_dataframe, expectation_suite=expectation_suite)
    results = df.validate(result_format='COMPLETE')

    # Filter out the invalid rows

    invalid_rows = [result["result"]['unexpected_index_list'] for result in results['results'] if not result['success']]
    invalid_rows = [item for sublist in invalid_rows for item in sublist]  # Flatten the list
    output_dataframe = df.drop(invalid_rows)
    
    return output_dataframe


