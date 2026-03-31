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
from utils.model_config.input_config import SYNDICATE_CONFIG
from utils.data_extraction.data_extraction import data_extraction
from utils.data_transformation.data_transformation import main as data_transformation, connection_process
from utils.community_detection.graph_construction import main as graph_construction
from utils.community_detection.model_scoring import main as model_scoring
from utils.data_output.edh_writing import main as edh_upload_data
from utils.model_logging.model_logging import log_utility

def main(scoring_config: dict):
        logger = log_utility(model_id = 'AMM-395', component = 'scoring')
        logger.info("Starting Job - AMM-395 (MR1298): CTP Syndicate Detection model")
        # Data Extraction -- CTP AD data
        node_data, doc_lawyer_df, doc_psych_df, doc_repair_df, vehicle_df = data_extraction(scoring_config)
        # Data Transformation
        df_node_feature, edges_grouped = data_transformation(scoring_config, node_data, doc_lawyer_df, doc_psych_df, doc_repair_df, vehicle_df)
        # Graph Construction and Community Detection
        graph_data, G, all_nodes_df, final_communities = graph_construction(scoring_config, df_node_feature, edges_grouped)
        # Model Scoring
        scored_data_out = model_scoring(scoring_config, graph_data, G, all_nodes_df, final_communities)
        # Output & Upload to EDH
        edh_upload_data(scoring_config, scored_data_out)

        logger.info("Job Completed - AMM-395 (MR1298): CTP Syndicate Detection model")

if __name__ == "__main__":
    score_config = SYNDICATE_CONFIG
    main(score_config)