import datetime
import os
import sys
import time
import pathlib
import warnings

import numpy as np
import pandas as pd
import pyarrow as pa
import pytz
import torch
from torch_geometric.data import Data
from torch_geometric.utils import to_networkx, degree
import networkx as nx

# Suppress warnings
warnings.filterwarnings("ignore")

# Add working directories to the system path
cur_path = pathlib.Path(__file__).resolve().parent.parent.parent.absolute()
src_loc = cur_path.joinpath("src")
util_loc = src_loc.joinpath("utils")
sys.path.extend([str(cur_path), str(src_loc), str(util_loc)])

from src.utils.utils import elapsed_time, save_data, load_data
from src.network import Dominant
from src.conf import Conf
from src.utils.model_utils.train_eval import networkTrainer, networkEvaluator
from utils.feature_processing import convert_dtype



# Model training pipeline
def model_pipeline(conf: Conf, graph_data: Data, id_df: pd.DataFrame, modelling_mode="train", metric1="New T2I", metric2="AUC-PR", hptrial=100):
    """
    Function for model training and evaluation.
    :param conf: Configuration object from Conf class
    :param graph_data: Constructed network data
    :param id_df: DataFrame including node IDs and target
    :param modelling_mode: Defaults to 'train', otherwise runs hyperparameter tuning
    :param metric1: Objective metric for hyperparameter tuning
    :param metric2: Metric for comparison in the plot
    :param hptrial: Number of trials for hyperparameter tuning
    :return: Saves artifacts locally
    """
    train_tag = datetime.datetime.now(pytz.timezone('Australia/Sydney')).strftime('%Y-%m-%d-%H-%M')
    
    if modelling_mode == 'train':
        # Load best parameters
        try:
            params = load_data(conf.artefact_path, "best_params", data_extension="pkl")
        except FileNotFoundError:
            params = None
        
        if params is None:
            params = {
                'hidden_size': 20,
                'dropout': 0,
                'num_epochs': 175,
                'learning_rate': 0.01,
                'weight_decay': 0.000182
            }
        
        print('*' * 60)
        print('Model training started...')
        
        model = Dominant(feat_size=graph_data.x.shape[1], hidden_size=params['hidden_size'], dropout=params['dropout'])
        model.train_model(graph_data, num_epochs=params['num_epochs'], learning_rate=params['learning_rate'], weight_decay=params['weight_decay'])
        
        print('Model training finished.')
        save_data(model, conf.artefact_path, f"ctp_ad_{train_tag}", data_extension="pkl")

        # Evaluate on train data
        thresh = 450
        print('*' * 60)
        print('Evaluating on training data...')
        metrics, combined_df = networkEvaluator.evaluate(conf, model, graph_data, id_df, thresh, train_tag, f"{conf.artefact_path}/metrics_train.csv", 'New T2I', 'AUC-PR')
        
        eval_artefacts = [metrics, combined_df]
        save_data(eval_artefacts, conf.artefact_path, 'eval_artefacts')
    
    else:
        print('*' * 60)
        print('Hyperparameter tuning started...')
        
        # Instantiate the trainer
        trainer = networkTrainer(conf, graph_data, id_df, metric1, metric2)
        
        # Train with Optuna
        best_params, study = trainer.train_with_optuna(n_trials=hptrial)
        trial_data = study.trials_dataframe()
        save_data(best_params, conf.artefact_path, "best_params", data_extension="pkl")
        
        print("Best Parameters:", best_params)
        print('Hyperparameter tuning finished.')
        print('*' * 60)

# Run the script if executed directly
if __name__ == "__main__":
    s_number = input("Your s_number (e.g., sxxxxxx, otherwise s_number will be None): ").lower()
    if not s_number.startswith("s"):
        s_number = None

    # Load configuration
    confparam_path = cur_path.joinpath("src", "conf", "conf_dev.yml")
    dataparam_path = cur_path.joinpath("src", "data", "dbt_model", "dbt_project.yml")
    conf = Conf(confparam_path, dataparam_path, s_number)

    # Start training
    project_start_time = time.time()
    modelling_mode = input("Enter modelling mode (e.g., train, hptune): ")
    hptrial = int(input("Enter number of trials to run (e.g., 10, 100): ")) if modelling_mode == "hptune" else 10
    
    print("*" * 60)
    print("Model training started...")
    
    # Load processed data
    # graph_data = load_data(conf.data_path, 'ctp_network', data_extension='pkl')
    graph_path = os.path.join(conf.data_path, 'ctp_pyg_data.pt')
    graph_data = torch.load(graph_path)
    id_df = load_data(conf.data_path, 'node_data', data_extension='csv')

    # Run the model pipeline
    function_start_time = time.time()
    model_pipeline(conf, graph_data, id_df, modelling_mode=modelling_mode, metric1="New T2I", metric2="AUC-PR", hptrial=hptrial)
    
    elapsed_time("Model training finished", project_start_time, function_start_time)
    print("*" * 60)