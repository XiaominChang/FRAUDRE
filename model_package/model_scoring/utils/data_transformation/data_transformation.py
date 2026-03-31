import pandas as pd
import numpy as np
from scipy import sparse
import joblib
import json
import os
from pathlib import Path
from utils.model_config.input_config import SYNDICATE_CONFIG, DBENVS
from utils.shared_repo.gen_utils import save_data, load_data
from utils.model_logging.model_logging import log_utility

ROOT_DIR = Path(__file__).resolve().parents[2]

def node_process(input_dataframe):
    """
    Function to perform data preparation steps on the given model_data DataFrame.

    Args:
    - input_dataframe: DataFrame containing the data to be prepared

    Returns:
    - model_data: DataFrame with the prepared data including node features
    """

    # Remove duplicates from input dataframe 
    input_dataframe = (
        input_dataframe
        .drop_duplicates(subset=['claim_number', 'claim_exposure_id'], keep='first')
        .copy()
    )

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

    input_dataframe.loc[:, fill_zero_cols] = input_dataframe.loc[:, fill_zero_cols].fillna(0)

    # Column Selection
    # Numeric columns
    num_cols = [
        'days_of_minor_assess_to_lodgement',
        'pre_accident_weekly_earning',
        'amt_fitness_certificate',
        'late_treatment_days',
    ]

    # One hot encoding cols
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

    # id_columns
    id_cols = [
        'claim_number',
        'claim_exposure_id',
        'claim_exposure_lodgement_date',
        'claim_exposure_loss_date',
        'claim_exposure_status_name',
        'contact_full_name',
        'fixed_contact_number',
        'full_address',
        'investigation_flag',
        'fraud_flag'
    ]

    features = id_cols + num_cols + ohe_cols
    df_node_feature = input_dataframe.loc[:, features].copy()
    df_node_feature.loc[:, ohe_cols] = df_node_feature.loc[:, ohe_cols].astype('int64')
    # df_node_feature.loc[:, 'pre_accident_weekly_earning'] = pd.to_numeric(
    #     df_node_feature['pre_accident_weekly_earning'], errors='coerce'
    # ).astype(float)

    df_node_feature.loc[:, 'claim_exposure_id'] = (
        df_node_feature['claim_number'].astype(str) + '_' +
        df_node_feature['claim_exposure_id'].astype(str)
    )
    df_node_feature = df_node_feature.drop(columns=['claim_number'])
    df_node_feature = df_node_feature.sort_values(
        by=['claim_exposure_lodgement_date', 'claim_exposure_id'], ascending=True
    ).reset_index(drop=True)

    return df_node_feature


def connection_process (doc_lawyer_df, doc_psych_df, doc_repair_df, vehicle_df):
        """
        Function to process and combine connection dataframes, prepare edge data for graph/network construction.

        Args:
        - df_node_feature: DataFrame containing node features
        - doc_lawyer_df: DataFrame containing lawyer document data
        - doc_psych_df: DataFrame containing psychologist document data
        - doc_repair_df: DataFrame containing repair document data
        - vehicle_df: DataFrame containing vehicle data

        Returns:
        - edges_grouped: combined connection dataframes
        """
        try:
                doctor_map_file = 'doctor_name_map_clustered.json'
                lawyer_map_file = 'lawyer_name_map_clustered.json'
                doc_map_path = os.path.join(ROOT_DIR, 'trained_models', doctor_map_file )
                lawyer_map_path= os.path.join(ROOT_DIR, 'trained_models', lawyer_map_file )
                with open( doc_map_path, 'r', encoding='utf-8') as f:
                        doctor_map = json.load(f)
                with open(lawyer_map_path, 'r', encoding='utf-8') as f:
                        lawyer_map = json.load(f)
        except FileNotFoundError as e:
                print(f"Error: {e}. Please ensure the mapping files are in the correct directory.")
                raise
        doc_lawyer_df['doctor_contact_name'] = (
                doc_lawyer_df['doctor_contact_name'].map(lambda x: x.strip() if isinstance(x, str) else x)
                                                .map(doctor_map)
                                                .fillna(doc_lawyer_df['doctor_contact_name'])
        )

        doc_lawyer_df['lawyer_contact_name'] = (
                doc_lawyer_df['lawyer_contact_name'].map(lambda x: x.strip() if isinstance(x, str) else x)
                                                .map(lawyer_map)
                                                .fillna(doc_lawyer_df['lawyer_contact_name'])  # keep original if not in similar-only map
        )

        # processing for doc_lawyer_df to include weights and combined columns
        doc_freq=doc_lawyer_df['doctor_contact_name'].value_counts()
        doc_lawyer_df['doc_freq']= doc_lawyer_df['doctor_contact_name'].map(doc_freq)
        doc_lawyer_df['doc_weight'] = 1.0 / doc_lawyer_df['doc_freq']**2

        lawyer_freq=doc_lawyer_df['lawyer_contact_name'].value_counts()
        doc_lawyer_df['lawyer_freq']= doc_lawyer_df['lawyer_contact_name'].map(lawyer_freq)
        doc_lawyer_df['lawyer_weight'] = 1.0 / doc_lawyer_df['lawyer_freq']**2


        doc_lawyer_df['weight'] = doc_lawyer_df['doc_weight'] + doc_lawyer_df['lawyer_weight']

        # Create the combined “party_name” column
        doc_lawyer_df['party_name'] = doc_lawyer_df.apply(
        lambda row: f"(doctor: {row['doctor_contact_name']}, lawyer: {row['lawyer_contact_name']})",
        axis=1
        )

        # Create the combined “contact_number” column
        doc_lawyer_df['party_contact_number'] = doc_lawyer_df.apply(
        lambda row: f"(doctor: {row['doctor_contact_number']}, lawyer: {row['lawyer_contact_number']})",
        axis=1
        )

        doc_lawyer_df['claim_exposure_id_1']= doc_lawyer_df['claim_number_1'] + '_'+ doc_lawyer_df['claim_exposure_id_1'].astype(str)
        doc_lawyer_df['claim_exposure_id_2']= doc_lawyer_df['claim_number_2'] + '_'+ doc_lawyer_df['claim_exposure_id_2'].astype(str)

        # Drop the now-unneeded originals
        doc_lawyer_df = doc_lawyer_df.drop(columns=[
        'claim_number_1',
        'claim_number_2',
        'doctor_contact_name',
        'lawyer_contact_name',
        'doctor_contact_number',
        'lawyer_contact_number',
        'doc_freq',
        'lawyer_freq',
        'doc_weight',   
        'lawyer_weight'
        ]).reset_index(drop=True)

        # Repeat similar processing for doc_psych_df
        doc_psych_df['doctor_contact_name'] = (
        doc_psych_df['doctor_contact_name'].map(lambda x: x.strip() if isinstance(x, str) else x)
                                                .map(doctor_map)
                                                .fillna(doc_psych_df['doctor_contact_name'])
        )

        doc_freq=doc_psych_df['doctor_contact_name'].value_counts()
        doc_psych_df['doc_freq']= doc_psych_df['doctor_contact_name'].map(doc_freq)
        doc_psych_df['doc_weight'] = 1.0 / doc_psych_df['doc_freq']**2

        psych_freq=doc_psych_df['psych_contact_name'].value_counts()
        doc_psych_df['psych_freq']= doc_psych_df['psych_contact_name'].map(psych_freq)
        doc_psych_df['psych_weight'] = 1.0 / doc_psych_df['psych_freq']**2

        # doc_psych_df = doc_psych_df[(doc_psych_df['doc_weight']>0.01) & (doc_psych_df['psych_weight']>0.01)]

        doc_psych_df['weight'] = doc_psych_df['doc_weight'] + doc_psych_df['psych_weight']

        doc_psych_df['party_name'] = doc_psych_df.apply(
        lambda row: f"(doctor: {row['doctor_contact_name']}, psych: {row['psych_contact_name']})",
        axis=1
        )
        doc_psych_df['party_contact_number'] = doc_psych_df.apply(
        lambda row: f"(doctor: {row['doctor_contact_number']}, psych: {row['psych_contact_number']})",
        axis=1
        )

        doc_psych_df['claim_exposure_id_1']= doc_psych_df['claim_number_1'] + '_'+ doc_psych_df['claim_exposure_id_1'].astype(str)
        doc_psych_df['claim_exposure_id_2']= doc_psych_df['claim_number_2'] + '_'+ doc_psych_df['claim_exposure_id_2'].astype(str)

        doc_psych_df = doc_psych_df.drop(columns=[
        'claim_number_1',
        'claim_number_2',
        'doctor_contact_name',
        'psych_contact_name',
        'doctor_contact_number',
        'psych_contact_number',
        'doc_freq',
        'psych_freq',       
        'doc_weight',
        'psych_weight'
        ]).reset_index(drop=True)

        # Repeat similar processing for doc_repair_df
        doc_repair_df['doctor_contact_name'] = (
        doc_repair_df['doctor_contact_name'].map(lambda x: x.strip() if isinstance(x, str) else x)
                                                .map(doctor_map)
                                                .fillna(doc_repair_df['doctor_contact_name'])
        )

        doc_freq=doc_repair_df['doctor_contact_name'].value_counts()
        doc_repair_df['doc_freq']= doc_repair_df['doctor_contact_name'].map(doc_freq)
        doc_repair_df['doc_weight'] = 1.0 / doc_repair_df['doc_freq']**2

        repair_freq=doc_repair_df['repairer_name'].value_counts()
        doc_repair_df['repair_freq']= doc_repair_df['repairer_name'].map(repair_freq)
        doc_repair_df['repair_weight'] = 1.0 / doc_repair_df['repair_freq']**2

        # doc_repair_df = doc_repair_df[(doc_repair_df['doc_weight']>0.01) & (doc_repair_df['repair_weight']>0.01)]

        doc_repair_df['weight'] = doc_repair_df['doc_weight'] + doc_repair_df['repair_weight']


        doc_repair_df['party_name'] = doc_repair_df.apply(
        lambda row: f"(doctor: {row['doctor_contact_name']}, repairer_name: {row['repairer_name']})",
        axis=1
        )

        doc_repair_df.rename(
        columns={
                'doctor_contact_number': 'party_contact_number'
        },
        inplace=True
        )

        doc_repair_df['claim_exposure_id_1']= doc_repair_df['claim_number_1'] + '_'+ doc_repair_df['claim_exposure_id_1'].astype(str)
        doc_repair_df['claim_exposure_id_2']= doc_repair_df['claim_number_2'] + '_'+ doc_repair_df['claim_exposure_id_2'].astype(str)

        doc_repair_df = doc_repair_df.drop(columns=[
        'claim_number_1',
        'claim_number_2', 
        'doctor_contact_name',      
        'repairer_name',
        'doc_freq',
        'repair_freq',  
        'doc_weight',
        'repair_weight'
        ])
        # Repeat similar processing for vehicle_df
        vehicle_df.rename(columns={'rego_number': 'party_name'},inplace=True)
        vehicle_df['claim_exposure_id_1']= vehicle_df['claim_number_1'] + '_'+ vehicle_df['claim_exposure_id_1'].astype(str)
        vehicle_df['claim_exposure_id_2']= vehicle_df['claim_number_2'] + '_'+ vehicle_df['claim_exposure_id_2'].astype(str)

        party_freq = vehicle_df['party_name'].value_counts()
        vehicle_df['freq_party'] = vehicle_df['party_name'].map(party_freq)
        vehicle_df['weight'] = 1.0 / vehicle_df['freq_party']**2

        # Drop the now-unneeded originals
        vehicle_df = vehicle_df.drop(columns=[
        'claim_number_1',
        'claim_number_2',
        'freq_party'
        ])

        # indicate connection/relationship type for further use
        doc_lawyer_df['connection_type'] = 'doc_lawyer'
        doc_psych_df['connection_type'] = 'doc_psych'
        doc_repair_df['connection_type'] = 'doc_repair'
        vehicle_df['connection_type'] = 'vehicle'

        edges_all= pd.concat(
                [doc_lawyer_df, doc_psych_df, doc_repair_df, vehicle_df],
                ignore_index=True,
                sort=False  
        )
        #add edge column for undirected graph
        edges_all['edge']=edges_all.apply(lambda row: tuple([row['claim_exposure_id_1'], row['claim_exposure_id_2']]), axis=1)
        #combine multiple edges between same nodes by summing weights and aggregating other info into lists
        edges_grouped = edges_all.groupby(['edge']).agg({
        'weight': 'sum',
        'connection_type': lambda x: list(i for i in x if pd.notna(i)) if len(x) > 0 else None,
        'party_name': lambda x: list(i for i in x if pd.notna(i)) if len(x) > 0 else None,
        'party_contact_number': lambda x: list(i for i in x if pd.notna(i)) if len(x) > 0 else None,
        }).reset_index()

        #Split 'edge' back into 'source' and 'target'
        edges_grouped[['claim_exposure_id_1', 'claim_exposure_id_2']] = pd.DataFrame(edges_grouped['edge'].tolist(), index=edges_grouped.index)
        edges_grouped = edges_grouped.drop(columns='edge')

        return edges_grouped


def main (scoring_config: dict, df_node: pd.DataFrame = None, 
                         doc_lawyer_df: pd.DataFrame = None , doc_psych_df: pd.DataFrame = None, 
                         doc_repair_df: pd.DataFrame = None, vehicle_df: pd.DataFrame = None):
        
        logger = log_utility(model_id = scoring_config.get("model_id", "unknown_model"), component = 'data_transformation')
        logger.info("Starting data transformation")

        local_write = scoring_config.get('local_write', False)
        data_path = scoring_config['data_path']
        input_env =  scoring_config['input_env']
        
        # Bucket and path setup
        principle = scoring_config.get('principle', 'user')
        s_number = scoring_config.get('s_number', 's745998')
        project_id = scoring_config.get('project_id', 'ria-vul-bbcc')
        if principle == 'user':
            bucket_name = f"{project_id}-aap-{input_env}-{s_number}-bucket"
        elif principle == "service":
            bucket_name = f"{project_id}-aap-{input_env}-model-bucket"

        # load node data if not provided
        if df_node is None:
                df_node = load_data(data_path, 'node_data', bucket_name, data_extension='csv')
        # process node attributes
        try:
                df_node_feature = node_process(df_node)
        except Exception as e:
                logger.error(f"Failed to transform node features: {e}")
                raise

        if doc_lawyer_df is None:
                doc_lawyer_df = load_data(data_path, 'doctor_lawyer_data', bucket_name, data_extension='csv')
        if doc_psych_df is None:
                doc_psych_df = load_data(data_path, 'doctor_psych_data', bucket_name, data_extension='csv')              
        if doc_repair_df is None:
                doc_repair_df = load_data(data_path, 'doctor_repairer_data', bucket_name, data_extension='csv')
        if vehicle_df is None:
                vehicle_df = load_data(data_path, 'vehicle_data', bucket_name, data_extension='csv')
        
        try:
                edges_grouped = connection_process (doc_lawyer_df, doc_psych_df, doc_repair_df, vehicle_df)
        except Exception as e:
                logger.error(f"Failed to transform connection dataframe: {e}")
                raise

        print(f"Has new claims to score: {edges_grouped.shape[0]}")
        # Save transformed data
        if local_write:
                save_data(df_node_feature, data_path, 'processed_node', bucket_name, data_extension='pkl')
                save_data(edges_grouped, data_path, 'processed_edges', bucket_name, data_extension='pkl')
                logger.info("Transformed data saved locally" )
 
        logger.info("Data transformation completed")
        return df_node_feature, edges_grouped

if __name__ == "__main__":
    score_config = SYNDICATE_CONFIG
    main(score_config)



        


