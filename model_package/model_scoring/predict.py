"""----------------------------------------------------------------------------

#  Batch scoring code for CTP Syndicate Detection Model - Python
#  Created: Xiaomin Chang - Sep 2025

-----------------------------------------------------------------------------"""

#%% importing libraries

import pandas as pd
import numpy as np
import joblib
import pickle
import os
import sys
import pytz
import datetime
import torch
from pathlib import Path
import utils.gcn_network.network as net
from utils.data_extraction.data_extraction import data_extraction
from utils.data_transformation.data_transformation import data_transformation, connection_process
from utils.community_detection.graph_construction import build_network, communities_to_partition,build_warm_start_partition,recursive_community_detection
from utils.community_detection.model_scoring import generate_output
from utils.data_output.edh_writing import extract_scored_data, create_output, upload_data_to_edh
from utils.model_logging.model_logging import log_utility


#%% Instanciate log class and pipeline arguments
logger = log_utility(model_id = 'AMM-395', component = 'scoring')

# Model name
model_name = 'claim_mr1298_ctp_syndicate_detection'
# Model directory
model_dir = os.path.dirname(os.path.realpath(__file__)).replace('\\','/')

# Log - start scoring process
logger.info("Starting Job - AMM-395 (MR1298): CTP Syndicate Detection model")

#%% EDH configuration

# Set Database Configuration for input and output
dbenvs = {
  'iadpprod_prod':{'db':'EDH_PROD', 'dbase':'iadpprod', 'schema':'mod_analytics'},
  'iadpprod_test': {'db':'EDH_PROD', 'dbase':'iadpprod', 'schema':'dl_aai_sqd_claim','read_role':'dl_analytics_claims_read_role', 'wrt_role':'dl_analytics_claims_wrt_role', 'crt_role':'dl_analytics_claims_crt_role'},
  'sas_prod': {'db':'EDH_PROD', 'dbase':'iadpsas', 'schema':'sas_analytics_ama', 'read_role':'sas_analytics_ama_read_role', 'wrt_role':'sas_analytics_ama_wrt_role', 'crt_role':'sys_sas_di'},
  'sas_preprod': {'db':'EDH_NONPROD', 'dbase':'iapdsas_integration',  'schema':'sas_analytics_ama',  'read_role': 'iapdsas_integration_sas_analytics_ama_read_role', 'wrt_role':'iapdsas_integration_sas_analytics_ama_wrt_role', 'crt_role':'iapdsas_integration_sas_analytics_ama_crt_role'},
  'sas_test': {'db':'EDH_NONPROD', 'dbase':'iadpsas', 'schema':'sas_analytics_ama',  'read_role':'iadpsas_sas_analytics_ama_read_role', 'wrt_role':'iadpsas_sas_analytics_ama_wrt_role','crt_role':'iadpsas_sas_analytics_ama_crt_role'},
  'sas_dev': {'db':'EDH_NONPROD', 'dbase':'iadpsas', 'schema':'sas_analytics_ama_dev',  'read_role':'iadpsas_sas_analytics_ama_dev_read_role',  'wrt_role': 'iadpsas_sas_analytics_ama_dev_wrt_role','crt_role':'iadpsas_sas_analytics_ama_dev_crt_role'}}

db_config=dbenvs['iadpprod_test']
# db_config_iadpprod  = dbenvs['iadpprod_prod']
# db_config_iadpsas = dbenvs['sas_prod']

# Set object names for iadpprod and iadpsas
objs = {'iadpprod':{'scores':model_name+'_reporting', 
                    'scores_history':model_name+'_history', 
                    'scores_audit':model_name +'_audit'}}

objs_iadpprod = objs['iadpprod']

# Output destination
iadpprod_write = True
# iadpsas_write = False

# Lodgement date bounds
START_DATE = "2018-01-01"
END_DATE = datetime.datetime.now(pytz.timezone('Australia/Sydney')).strftime('%Y-%m-%d')

#################-#######AD model pipeline################################
#%%  Data Extraction -- CTP AD data
try: 
  node_data, doc_lawyer_df, doc_psych_df, doc_repair_df, vehicle_df = data_extraction(START_DATE, END_DATE)
  # Logs
  logger.info(f"Data Extraction Completed")

except (Exception) as error:
  logger.error(f"Data Extraction Failed: {error}")
  sys.exit("Pipeline Failed")

#%% Data Preparation 
try: 
  df_node_feature= data_transformation(node_data)
  edges_grouped_df = connection_process (doc_lawyer_df, doc_psych_df, doc_repair_df, vehicle_df)
  # Logs
  logger.info(f"Data Transformation Completed")
except(Exception) as error:
  logger.error(f"Data Transformation Failed: {error}")
  sys.exit("Pipeline Failed")

#%% Graph Construction
try: 
  graph_data, G, all_nodes_df = build_network (df_node_feature, edges_grouped_df)
  # Logs
  logger.info(f"Graph Construction Completed")
except(Exception) as error:
  logger.error(f"Graph Construction Failed: {error}")
  sys.exit("Pipeline Failed")

#%% Community Detection
try: 
  comm_path = Path("./trained_models/communities.pkl")
  with comm_path.open("rb") as f:                      # <<< binary mode
      init_communities = pickle.load(f)
  # process initial communities to partition dict with all nodes in G for warm start partition
  init_communities = communities_to_partition(init_communities)
  init_communities= build_warm_start_partition(G, init_communities,"edge_weight") 

  final_communities = []
  recursive_community_detection(
      G,
      final_communities,
      resolution=1.2,      # try a different resolution if you want more/smaller splits
      threshold=50,        # your stopping size
      seed_value=42,
      init_partition=init_communities
  )
  out_path = Path("./trained_models/communities.pkl")
  with out_path.open("wb") as f:          # binary mode!
    pickle.dump(final_communities, f)
  # Logs
  logger.info(f"Community Detection Completed")
except(Exception) as error:
  logger.error(f"Community Detection Failed: {error}")
  sys.exit("Pipeline Failed")

#%% Model Prediction 
# Load the Dominant model 
try: 
  sys.modules["network"] = net
  model = torch.load( "./trained_models/dominant_AD_model_2025-09-26-23-23.pth")
  anomaly_scores, y_emb=model.predict(graph_data, get_emb=True)

  # Prepare output data
  scored_data_out = generate_output(G, final_communities, all_nodes_df, anomaly_scores)
  # Logs
  logger.info(f"Model Scoring Completed")

except(Exception) as error:
  logger.error(f"Model Scoring Failed: {error}")
  sys.exit("Pipeline Failed")

#%% Create Output 
print(scored_data_out.columns)
#%% Create Output 
# Preparing data for comparison
scored_current = scored_data_out.copy()

if scored_current.shape[0] != 0:
  try:
    scored_latest  = extract_scored_data(db_config['db'], db_config['dbase'], db_config['schema'], objs_iadpprod['scores'])

    compare_cols =  [
        'community_id',
        'claim_exposure_1', 'exposure_1_lodgement_date', 'exposure_1_loss_date', 'exposure_1_status',
        'exposure_1_contact_name', 'exposure_1_contact_number', 'exposure_1_investigation_flag',
        'claim_exposure_2', 'exposure_2_lodgement_date', 'exposure_2_loss_date', 'exposure_2_status',
        'exposure_2_contact_name', 'exposure_2_contact_number', 'exposure_2_investigation_flag',
        'relationship_type', 'relationship_party', 'party_contact',
        'rank_by_community_anomaly_score', 'investigation_rate','community_size'
        ]

    scored_latest_new, df_diff, audit, df_execution = create_output(scored_current, scored_latest, model_name, compare_cols)

    output= {'ctp_communities_scored':scored_latest_new,
              'ctp_communities_scored_history_diff':df_diff,
              'audit':audit}
    logger.info(f"Creating Model Outputs Completed")
  except(Exception) as error:
    logger.error(f"Creating Model Outputs Failed: {error}")
    sys.exit("Pipeline Failed")

  if iadpprod_write:
    try:
      upload_data_to_edh( db        = db_config['db'], 
                          dbase     = db_config['dbase'],
                          schema    = db_config['schema'], 
                          tables    = output,
                          objs = objs_iadpprod)
      # Logs
      logger.info(f"Model Outputs Exporting to IADPPROD Completed")
    except(Exception) as error:
      logger.error(f"Model Outputs Exporting to IADPPROD Failed: {error}")
      sys.exit("Pipeline Failed")

