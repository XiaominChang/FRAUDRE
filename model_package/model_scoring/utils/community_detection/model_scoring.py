import pandas as pd
import numpy as np
from scipy import sparse
import joblib
import json
import torch
from torch_geometric.data import Data
from torch_geometric.utils import to_networkx, degree
import networkx as nx
import matplotlib.pyplot as plt
from torch_geometric.utils import to_undirected
import pytz
import datetime
import sys
import os
from pathlib import Path
from utils.model_config.input_config import SYNDICATE_CONFIG, DBENVS
from utils.shared_repo.gen_utils import save_data, load_data
from utils.model_logging.model_logging import log_utility
import utils.gcn_network.network as net
from utils.community_detection.graph_construction import load_latest_communities

ROOT_DIR = Path(__file__).resolve().parents[2]


# Function to compute conductance
def compute_conductance(graph, community):
    vol_community = sum(dict(graph.degree(community)).values())
    edge_boundary = len(list(nx.edge_boundary(graph, community)))
    vol_rest = sum(dict(graph.degree(graph.nodes - community)).values())

    if vol_community>0 and vol_rest>0:  
        return edge_boundary / min(vol_community, vol_rest)  
    else:
        return 0

def cal_metrics_community(G, communities):   
    # Collect metrics for each community
    community_metrics = []

    for i, community in enumerate(communities):
        subgraph = G.subgraph(community)

        edge_weights = [data.get('edge_weight', 0) for u, v, data in subgraph.edges(data=True)]
        total_weight = sum(edge_weights)
        avg_edge_weight = total_weight / len(edge_weights) if edge_weights else 0

        node_list=[node for node, data in subgraph.nodes(data=True)]
        # Retrieve claim numbers:
        claim_list=[data['claim_exposure_id']for node, data in subgraph.nodes(data=True)]

        # Clustering coefficient (average for nodes in the community)
        avg_clustering = nx.average_clustering(subgraph)
        
        # Conductance
        conductance = compute_conductance(G, community)
        
        # Density
        density = nx.density(subgraph)
        
        # Triangle count (sum of triangles in the community)
        triangles = sum(nx.triangles(subgraph).values()) // 3  # Each triangle is counted thrice

        
        # Add metrics to the list
        community_metrics.append({
            'Community': i,
            'Nodes': node_list,
            'Exposure list': claim_list,
            'Average edge weight':avg_edge_weight,
            'Clustering Coefficient': avg_clustering,
            'Conductance': conductance,
            'Density': density,
            'Triangle Count': triangles,
            'Size': len(community),
        })

    # Create a DataFrame
    df_metrics = pd.DataFrame(community_metrics)
    return df_metrics

def generate_output(G, final_communities, all_nodes_df, anomaly_scores):
        """
        Generate output DataFrame with community relationships and metrics.
        Args:
        - G: NetworkX graph
        - final_communities: List of sets, each containing node IDs in a community
        - all_nodes_df: DataFrame with node features and metadata       
        - anomaly_scores: List of anomaly scores per claim exposure
        Returns:
        - output_df: DataFrame with community relationships and metrics
        """

        # filter communities based on metrics
        df_metrics = cal_metrics_community(G, final_communities)
        filtered_metrics = df_metrics[
                        (df_metrics['Clustering Coefficient'] > 0.9) &  
                        (df_metrics['Conductance'] < 0.1) &             
                        (df_metrics['Density'] > 0.9) &                 
                        (df_metrics['Size'] >=4 )         
                        ].reset_index(drop=True)   
        
        ranked_metrics = filtered_metrics.sort_values(by=['Average edge weight','Conductance','Clustering Coefficient','Density','Size'], ascending=[False, True,False,False,False]).reset_index(drop=True)
        filtered_communities = ranked_metrics['Nodes'].tolist() 
        
        rows = []
        for community_id, community in enumerate(filtered_communities):
                sub = G.subgraph(community)
                seen = set()
                
                for u, v, edge_data in sub.edges(data=True):
                        conn_types = edge_data.get('connection_type', list())
                        for i, rel in enumerate(conn_types):
                                key = tuple(sorted((u, v))) + (rel,)
                                if key in seen:
                                        continue
                                seen.add(key)
                                party= edge_data.get('party_name', list())[i]
                                contact= edge_data.get('party_contact_number', list())[i]
                                # prepare party based on relationship type
                                        
                                rows.append({
                                'community_id':       community_id,
                                'claim_exposure_1':   G.nodes[u]['claim_exposure_id'],
                                'claim_exposure_2':   G.nodes[v]['claim_exposure_id'],
                                'relationship_type':  rel,
                                'relationship_party': party,
                                'party_contact': contact
                                })
        output_df = pd.DataFrame(rows)
        all_nodes_df['investigation_flag'].replace ({1:'True', 0:'False'}, inplace=True)
        # rename df_encoded for Claim 1
        df1 = all_nodes_df.rename(columns={
        'claim_exposure_id':           'claim_exposure_1',
        'claim_exposure_lodgement_date':   'exposure_1_lodgement_date',
        'claim_exposure_loss_date':   'exposure_1_loss_date',
        'claim_exposure_status_name':      'exposure_1_status',
        'contact_full_name': 'exposure_1_contact_name',
        'fixed_contact_number': 'exposure_1_contact_number',
        'investigation_flag': 'exposure_1_investigation_flag',
        })

        # then rename df_encoded for Claim 2
        df2 = all_nodes_df.rename(columns={
        'claim_exposure_id':           'claim_exposure_2',      
        'claim_exposure_lodgement_date':   'exposure_2_lodgement_date',
        'claim_exposure_loss_date':   'exposure_2_loss_date',
        'claim_exposure_status_name':      'exposure_2_status',
        'contact_full_name': 'exposure_2_contact_name',
        'fixed_contact_number': 'exposure_2_contact_number',
        'investigation_flag': 'exposure_2_investigation_flag',
        })

        # now merge onto output_df
        df_output_merged = (
        output_df
        .merge(
                df1[['claim_exposure_1',
                'exposure_1_lodgement_date',
                'exposure_1_loss_date',
                'exposure_1_status',
                'exposure_1_contact_name',
                'exposure_1_contact_number',
                'exposure_1_investigation_flag']],
                on='claim_exposure_1',
                how='left'
        )
        .merge(
                df2[['claim_exposure_2',
                'exposure_2_lodgement_date',
                'exposure_2_loss_date',
                'exposure_2_status',
                'exposure_2_contact_name',
                'exposure_2_contact_number',
                'exposure_2_investigation_flag']],
                on='claim_exposure_2',
                how='left'
        )
        )



        new_order = [
        'community_id',
        'claim_exposure_1', 'exposure_1_lodgement_date', 'exposure_1_loss_date', 'exposure_1_status',
        'exposure_1_contact_name', 'exposure_1_contact_number', 'exposure_1_investigation_flag',
        'claim_exposure_2', 'exposure_2_lodgement_date', 'exposure_2_loss_date', 'exposure_2_status',
        'exposure_2_contact_name', 'exposure_2_contact_number', 'exposure_2_investigation_flag',
        'relationship_type', 'relationship_party', 'party_contact'
        ]


        df_output_merged = df_output_merged[new_order]

        # Collect metrics for each community
        community_metrics = []

        for i, community in enumerate(filtered_communities):
                subgraph = G.subgraph(community)
                
                community_anomaly_scores = [anomaly_scores[node] for node in community]
                # avg_anomaly_score = sum(community_anomaly_scores) / len(community_anomaly_scores) if community_anomaly_scores else 0
        
                # Calculate metrics for anomaly scores within the community
                max_score = np.max(community_anomaly_scores)

                node_list=[node for node, data in subgraph.nodes(data=True)]
                # Retrieve claim numbers:

                investigation_list = [data['investigation_flag'] for node, data in subgraph.nodes(data=True) if data['investigation_flag'] != 0]

                # Calculate percentage of nodes with y == 1
                total_nodes = len(community)
                # y_count = sum(1 for node, data in subgraph.nodes(data=True) if data.get('y', 0) == 1)
                y_count =  len(investigation_list)
                y_percentage = (y_count / total_nodes * 100) if total_nodes > 0 else 0
                
                # Add metrics to the list
                community_metrics.append({
                'community_id': i,
                'nodes': node_list,
                'investigated count': len(investigation_list),
                'community_anomaly_score': max_score,
                'community_size': len(community),
                'investigation_rate': y_percentage # Optionally add community size
                })

        # Create a DataFrame
        df_community_metrics = pd.DataFrame(community_metrics)
        df_community_metrics['rank_by_community_anomaly_score'] = df_community_metrics['community_anomaly_score'].rank(method='first', ascending=False).astype(int).reset_index(drop=True)
        df_final_output =df_output_merged.merge(df_community_metrics[['community_id', 'community_anomaly_score', 'rank_by_community_anomaly_score', 'investigation_rate','community_size']],
                                        on='community_id', how='left')
        df_final_output['community_anomaly_score'] = df_final_output['community_anomaly_score'].apply(lambda x: float('%.3g' % x))
        df_final_output['insert_timestamp'] =pd.Timestamp.now(tz='Australia/Sydney').date()

        return df_final_output

def main (scoring_config: dict, graph_data: Data=None, G:nx.Graph=None, 
                 all_nodes_df: pd.DataFrame=None, final_communities: list=None):
        """
        Perform model scoring using the Dominant model and generate output DataFrame.
        Args:
        - scoring_config: Dictionary with configuration parameters
        - graph_data: PyG Data object with graph structure and node features
        - G: NetworkX graph
        - all_nodes_df: DataFrame with node features and metadata
        - final_communities: List of sets, each containing node IDs in a community
        Returns:
        - scored_data_out: DataFrame with community relationships and metrics
        """
        logger = log_utility(model_id = scoring_config.get("model_id", "unknown_model"), component = 'model_scoring')
        logger.info("Starting model scoring")
        # model_path = scoring_config['model_path']
        local_write = scoring_config['local_write']
        data_path = scoring_config['data_path']
        input_env =  scoring_config['input_env']
        model_path =  os.path.join(ROOT_DIR, 'trained_models')
        
        # Bucket and path setup
        principle = scoring_config.get('principle', 'user')
        s_number = scoring_config.get('s_number', 's745998')
        project_id = scoring_config.get('project_id', 'ria-vul-bbcc')
        if principle == 'user':
            bucket_name = f"{project_id}-aap-{input_env}-{s_number}-bucket"
        elif principle == "service":
            bucket_name = f"{project_id}-aap-{input_env}-model-bucket"
        
        # load graph data and processed node data
        if graph_data is None:
               graph_data = load_data(data_path, 'graph_data', bucket_name, data_extension='pt')
        if G is None:
               G = load_data(data_path, 'networkx_graph', bucket_name, data_extension='pkl')
        if all_nodes_df is None:
               all_nodes_df = load_data(data_path, 'all_nodes_df', bucket_name, data_extension='csv')
        if final_communities is None:
        #        final_communities = load_data(model_path, 'communities', data_extension='pkl')
               final_communities = load_latest_communities(data_path, bucket_name, logger)
        
        # Load the Dominant model 
        try: 
                sys.modules["network"] = net
                model = load_data(model_path, 'dominant_AD_model_2025-09-26-23-23', data_extension='pth')
                anomaly_scores, y_emb=model.predict(graph_data, get_emb=True)
                logger.info("Model prediction completed")
        except Exception as e:
                logger.error(f"Model scoring failed: {e}")
                raise

        # Prepare output data
        try:
                scored_data_out = generate_output(G, final_communities, all_nodes_df, anomaly_scores)
                logger.info("Output generation completed")
        except Exception as e:
                logger.error(f"Output generation failed: {e}")
                raise

        # Save scored data if local_write is True
        if local_write:
                try:
                        save_data(scored_data_out, data_path, 'ctp_communities_scored', bucket_name, data_extension='pkl')
                        logger.info("Scored data saved locally")
                except Exception as e:
                        logger.error(f"Saving scored data locally failed: {e}")
                        raise

        return scored_data_out

# Example usage:
if __name__ == "__main__":
        score_config = SYNDICATE_CONFIG
        scored_data_out = main(score_config)
        