import re
import sys
import time
import pathlib
from scipy import sparse
import torch
import numpy as np 
import pandas as pd
import matplotlib.pyplot as plt
from category_encoders import BinaryEncoder
from torch_geometric.data import Data
from torch_geometric.utils import to_networkxfrom, degree
import networkx as nx

# Add working directory to the path
cur_path = pathlib.Path(__file__).resolve().parent.parent.absolute()
src_loc = cur_path.joinpath("src")
sys.path.append(str(cur_path))
sys.path.append(str(src_loc))

# Load local modules
from src.conf import Conf
from src.eda.preprocess_eda import preprocess_data
from src.models.model_graph_ad import model_pipeline
from src.utils.sql import sql
from src.utils.utils import elapsed_time, load_data, save_data
from src.utils.feature_processing import data_transformation
from src.utils.graph_utils import build_network

def main(s_number=None, hptrial=10):
    """
    Run model training pipeline
    :param s_number: Optional identifier for GCP
    :param hptrial: Number of hyperparameter trials

    :return: Save artifacts locally
    """
    # Set configuration parameters
    confparam_path = cur_path.joinpath("src", "conf", "conf_dev.yml")
    dataparam_path = cur_path.joinpath("src", "data", "ctp_dbt_model", "dbt_project.yml")
    conf = Conf(confparam_path, dataparam_path)

    # Load data
    project_start_time = time.time()
    print('*' * 60)
    print("Loading data locally...")
    function_start_time = time.time()  
    cust_df = load_data(conf.data_path, 'customer_connection_data', data_extension='parquet')
    doc_lawyer_df = load_data(conf.data_path, 'doc_lawyer_connection_data', data_extension='parquet')
    doc_psych_df = load_data(conf.data_path, 'doc_psych_connection_data', data_extension='parquet')
    doc_repair_df = load_data(conf.data_path, 'doc_repair_connection_data', data_extension='parquet')
    payment_df = load_data(conf.data_path, 'payment_connection_data', data_extension='parquet')
    vehicle_df = load_data(conf.data_path, 'vehicle_connection_data', data_extension='parquet')
    node_df = load_data(conf.data_path, 'node_feature', data_extension='parquet')
    elapsed_time('Load raw data', project_start_time, function_start_time)

    # Perform node data processing and engineering
    input_dataframe = node_df.copy()
    df_encoded = data_transformation(input_dataframe)

    # Build CTP claim network
    df_edges = [cust_df, doc_lawyer_df, doc_psych_df, doc_repair_df, vehicle_df]
    print('*' * 60)
    print("Building CTP claim network...")
    function_start_time = time.time() 
    graph_data, G, all_nodes_df = build_network(df_edges, df_encoded, conf)
    elapsed_time('Build CTP claim network', project_start_time, function_start_time)

    # Hyperparameter tuning for deep graph learning model
    print('*' * 60)
    print("Hyperparameter tuning...")
    function_start_time = time.time()
    model_pipeline(conf, graph_data, all_nodes_df, modelling_mode='hptune', 
                   metric1='New T2I', metric2='New T2D', hptrial=hptrial)
    elapsed_time('Model HP tuning', project_start_time, function_start_time)

    # Model training and evaluation
    print('*' * 60)
    print("Model training and evaluation...")
    function_start_time = time.time()
    model_pipeline(conf, graph_data, all_nodes_df, modelling_mode='train', 
                   metric1='New T2I', metric2='New T2D', hptrial=hptrial)
    elapsed_time('Model training and evaluation', project_start_time, function_start_time)

if __name__ == "__main__":
    try:
        # Prompt user for input
        s_number = None
        while True:
            s_number_input = input("Please enter your s-number if running on GCP (e.g., sxxxxxx), otherwise press Enter: ").strip().lower()
            if s_number_input == "":
                break
            if not re.match(r"^s\d+$", s_number_input):
                print("Invalid s-number format. The s-number should start with 's' and be followed by numbers only.")
            else:
                s_number = s_number_input
                break

        # Input value checking for hptrial
        while True:
            try:
                hptrial = int(input("Enter number of trials for HP tuning (e.g., 10, 100): "))
                if hptrial <= 0:
                    print("Please enter a positive integer for the number of trials.")
                else:
                    break
            except ValueError:
                print("Please enter a valid integer.")
    except KeyboardInterrupt:
        print("\nInput interrupted. Exiting...")
        sys.exit(1)
    except Exception as e:
        print("An error occurred:", e)
        sys.exit(1)

    # Run the training
    main(s_number, hptrial)