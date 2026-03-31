# text preprocessing and new features generation
import re
import decimal
from scipy import sparse
from category_encoders import BinaryEncoder
import numpy as np 
import pandas as pd


# Define a custom function to convert data type


def convert_dtype(X):
    # Perform data type conversion here
    return X.astype("object")


def data_transformation(input_dataframe):
        """
        Function to perform data preparation steps on the given model_data DataFrame.

        Args:
        - input_dataframe: DataFrame containing the data to be prepared

        Returns:
        - model_data: DataFrame with the prepared data including node features
        """

        # Remove duplicates from input dataframe
        input_dataframe = input_dataframe.drop_duplicates(subset=['claim_number'], keep='first')

        # fill the missing value

        fill_zero_cols = [
                'not_fit_for_work_flag',
                'off_work_minor_injury_flag',
                'not_threshold_injury_flag',
                'days_of_minor_assess_to_lodgement',
                'suspacious_not_working_flag',
                'self_employed_flag',
                'pre_accident_weekly_earning',
                'late_treatment_days',
                'amt_fitness_certificate',
                'cof_holiday_weekend_flag',
                'cof_issue_to_received_over_2m_flag',
                'rejected_pay_over_8_flag',
                'gp05_pay_exceed_gp_consult'
        ]

        for x in fill_zero_cols:
                input_dataframe[x].fillna(0, inplace=True)

        # Column Selection
        # Numeric columns
        num_cols = [
                'days_of_minor_assess_to_lodgement',
                'pre_accident_weekly_earning',
                'amt_fitness_certificate',
                'late_treatment_days',
        ]
        #One hot encodin
        ohe_cols = [
                'not_fit_for_work_flag',
                'off_work_minor_injury_flag',
                'not_threshold_injury_flag',
                'suspacious_not_working_flag',
                'self_employed_flag',
                'cof_holiday_weekend_flag',
                'cof_issue_to_received_over_2m_flag',
                'rejected_pay_over_8_flag',
                'gp05_pay_exceed_gp_consult'
        ]
        #id_columns
        id_cols = [    
                'claim_number',
                'claim_exposure_id',
                'claim_exposure_lodgement_date',
                'claim_exposure_status_name',
                'contact_full_name',
                'fixed_contact_number',
                'full_address',
                'investigation_flag',
                'fraud_flag'
        ]

        features=  id_cols+ num_cols + ohe_cols
        df_node_feature = input_dataframe[features].copy()
        df_node_feature[ohe_cols]=df_node_feature[ohe_cols].astype(int)
        
        # df_node_id = input_dataframe[id_cols].copy()
        df_node_feature['claim_exposure_id']=  df_node_feature['claim_number'] + '_'+  df_node_feature['claim_exposure_id'].astype(str)
        df_node_feature.drop(columns=['claim_number'], inplace=True)

        return df_node_feature