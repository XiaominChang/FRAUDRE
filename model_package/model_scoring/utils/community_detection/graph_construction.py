import pandas as pd
import numpy as np
from scipy import sparse
import joblib
import json
import torch
import os
from pathlib import Path
from datetime import datetime
from torch_geometric.data import Data
from torch_geometric.utils import to_networkx, degree
import networkx as nx
import matplotlib.pyplot as plt
from torch_geometric.utils import to_undirected
from collections import Counter, defaultdict
from community import community_louvain
from typing import List, Set, Dict, Hashable, Optional
import random
from utils.model_config.input_config import SYNDICATE_CONFIG, DBENVS
from utils.shared_repo.gen_utils import save_data, load_data
from utils.model_logging.model_logging import log_utility

ROOT_DIR = Path(__file__).resolve().parents[2]

def build_network (df_node_feature, edges_grouped):
        """
        Function to build a network graph from the given node features and connection dataframes.

        Args:
        - df_node_feature: DataFrame containing node features
        - edges_grouped: DataFrame containing grouped edge data with weights and connection types

        Returns:
        - graph_data: PyG data with node features and edges
        - G: NetworkX graph object of graph data for visualization
        """

        # Create a mapping from node IDs to indices, including all nodes from df_node_features
        # Extract node IDs from node features
        feature_node_ids = df_node_feature['claim_exposure_id'].tolist()
        # make sure all nodes in edges are in node features
        edges_grouped = edges_grouped[
        edges_grouped['claim_exposure_id_1'].isin(feature_node_ids) &
        edges_grouped['claim_exposure_id_2'].isin(feature_node_ids)
                ].reset_index(drop=True)
        # Create mapping from node IDs to indices
        node_id_to_idx = {node_id: idx for idx, node_id in enumerate(feature_node_ids)}

        # Map claim number to indices in edges
        edges_grouped['source_idx'] = edges_grouped['claim_exposure_id_1'].map(node_id_to_idx)
        edges_grouped['target_idx'] = edges_grouped['claim_exposure_id_2'].map(node_id_to_idx)
        # Prepare node features, including nodes without edges
        df_node_feature['node_idx'] = df_node_feature['claim_exposure_id'].map(node_id_to_idx)


        all_nodes_df = df_node_feature.sort_values('node_idx').reset_index(drop=True).set_index('node_idx')
        
        id_cols = [    
                'claim_exposure_id',
                'claim_exposure_lodgement_date',
                'claim_exposure_loss_date',
                'claim_exposure_status_name',
                'contact_full_name',
                'fixed_contact_number',
                'full_address',
                'node_idx',
                'investigation_flag',
                'fraud_flag'
        ]
        feature_cols=df_node_feature.columns.drop(id_cols)
        # Extract node features as a tensor
        x = torch.tensor(all_nodes_df[feature_cols].values, dtype=torch.float)
        # Create edge_index tensor
        edge_index = torch.tensor([edges_grouped['source_idx'].values, edges_grouped['target_idx'].values], dtype=torch.long)
        # edge_index = to_undirected(edge_index)

        # Create edge_weight tensor
        edge_weight = torch.tensor(edges_grouped['weight'].values, dtype=torch.float)
        # Create investigation label tensor
        y = torch.tensor(all_nodes_df['investigation_flag'].values, dtype=torch.long)

        # Create PyG data object as CTP claim network
        graph_data= Data(x=x, edge_index=edge_index, edge_weight=edge_weight,y=y)
        # Create NetworkX graph for visualization
        G = to_networkx(graph_data, to_undirected=True, edge_attrs=['edge_weight'],node_attrs=['x'])
        # Add node attributes from all_nodes_df to the NetworkX graph
        nx.set_node_attributes(G, all_nodes_df['claim_exposure_id'].to_dict(), 'claim_exposure_id')
        nx.set_node_attributes(G, all_nodes_df['claim_exposure_lodgement_date'].to_dict(), 'claim_exposure_lodgement_date')
        nx.set_node_attributes(G, all_nodes_df['claim_exposure_loss_date'].to_dict(), 'claim_exposure_loss_date')
        nx.set_node_attributes(G, all_nodes_df['claim_exposure_status_name'].to_dict(), 'claim_exposure_status_name')
        nx.set_node_attributes(G, all_nodes_df['contact_full_name'].to_dict(), 'contact_full_name')
        nx.set_node_attributes(G, all_nodes_df['fixed_contact_number'].to_dict(), 'fixed_contact_number')
        nx.set_node_attributes(G, all_nodes_df['full_address'].to_dict(), 'full_address')
        nx.set_node_attributes(G, all_nodes_df['investigation_flag'].to_dict(), 'investigation_flag')
        nx.set_node_attributes(G, all_nodes_df['fraud_flag'].to_dict(), 'fraud_flag')
        # Add edge attributes from edges_grouped to the NetworkX graph
        for _, row in edges_grouped.iterrows():
                source_idx = row['source_idx']
                target_idx = row['target_idx']

                if G.has_edge(source_idx, target_idx):
                        G.edges[source_idx, target_idx]['connection_type'] = row['connection_type']
                        G.edges[source_idx, target_idx]['party_name'] = row['party_name']
                        G.edges[source_idx, target_idx]['party_contact_number'] = row['party_contact_number']

        # Remove isolated nodes (nodes without edges)
        G.remove_nodes_from(list(nx.isolates(G)))

        return graph_data, G, all_nodes_df


def communities_to_partition(final_communities: List[Set[Hashable]]) -> Dict[Hashable, int]:
        """
        Convert a list of node-sets into a partition mapping: node -> community_id.
        Community IDs are 0..K-1 in the order they appear.
        """
        partition = {}
        for cid, nodes in enumerate(final_communities):
            for u in nodes:
                # If overlaps exist, the last assignment wins. Change to `raise` if you prefer strictness.
                partition[u] = cid
        return partition


def build_warm_start_partition(
    G: nx.Graph,
    init_partition: dict,
    weight ="edge_weight",
    ):
    
        """
        G: CTP network which is nx.Graph instance,
        init_partition: dict | None,
        weight: str | None = "edge_weight",
        
        Return a partition dict covering *all* nodes in G.
        - Start from init_partition (filtered to nodes still in G).
        - For new nodes, pick the weighted-majority community of neighbors;
        if none, assign a new unique community id.
        """
        # 1) Start with existing labels, filter to nodes still in G
        part = {}
        if init_partition:
            part = {u: int(c) for u, c in init_partition.items() if u in G}

        # 2) Track max community id so we can create new ones
        next_cid = (max(part.values()) + 1) if part else 0

        # 3) Assign communities for nodes missing in init_partition
        for u in G.nodes:
            if u in part:
                continue

            # Collect neighbor communities with weights
            nbr_com_weights = Counter()
            for v, attr in G[u].items():
                if v in part:
                    w = attr.get(weight, 1.0) if (weight and isinstance(attr, dict)) else 1.0
                    nbr_com_weights[part[v]] += float(w)

            if nbr_com_weights:
                # Weighted majority among neighbors
                best_com, _ = nbr_com_weights.most_common(1)[0]
                part[u] = best_com
            else:
                # Isolated or only-new-neighbor node → give it a new singleton community
                part[u] = next_cid
                next_cid += 1

        # 4) (Optional) Drop labels for nodes no longer in G (already filtered above)
        # Ensure all nodes are covered
        assert len(part) == G.number_of_nodes(), "Warm start must include all nodes."

        return part

def recursive_community_detection(
    G: nx.Graph,
    communities_out: list,
    resolution: float,
    threshold: int,
    seed_value: int = 42,
    init_partition: dict =None
    ):
        """
        Recursively split G into communities using Louvain (best_partition), until each
        resulting subgraph has <= threshold nodes. Appends each final community's node set
        into `communities_out`.

        Parameters
        ----------
        G : nx.Graph
            (Preferably undirected; if directed, convert with `G.to_undirected()`)
            Must carry edge weights in 'edge_weight' if you want weighted Louvain.
        communities_out : list
            A list that will be appended with `set(node_ids)` for each final community.
        resolution : float
            Louvain resolution parameter (higher → more/smaller communities).
        threshold : int
            Max size of a leaf community; if |G| <= threshold, stop splitting.
        seed_value : int
            Random seed for determinism.
        init_partition : dict | None
            Optional warm start mapping node -> community_id. Can be partial; nodes
            not present will be initialized by the algorithm.
        """
        # Seed for reproducibility
        random.seed(seed_value)
        np.random.seed(seed_value)  

        n = G.number_of_nodes()
        if n == 0:
            return
        if n <= threshold:
            communities_out.append(set(G.nodes))
            return

        # If graph is directed, Louvain expects undirected; convert on the fly.
        if G.is_directed():
            G = G.to_undirected()

        # Filter the initial partition to nodes in this subgraph (if provided)
        sub_init = None
        if init_partition:
            sub_init = {u: c for u, c in init_partition.items() if u in G}
        

        # Run Louvain with warm start
        part = community_louvain.best_partition(
            G,
            partition=sub_init,                # warm start (can be None)
            weight="edge_weight",             # your edge weight field
            resolution=resolution,
            random_state=seed_value
        )
        # Group nodes by community id
        label2nodes = defaultdict(set)
        for u, lbl in part.items():
            label2nodes[lbl].add(u)

        # If no split happened (all nodes in one community), stop here
        if len(label2nodes) <= 1:
            communities_out.append(set(G.nodes))
            return

        # Recurse into each community
        for nodes in label2nodes.values():
            subG = G.subgraph(nodes).copy()   # copy to avoid view surprises
            # Warm start for the child call:
            # You can pass `part` (filtered inside) or `None`. Passing it can speed up refinement.
            recursive_community_detection(
                subG,
                communities_out,
                resolution=resolution,
                threshold=threshold,
                seed_value=seed_value,
                init_partition=part
            )
        return

def load_latest_communities(data_path: str, bucket_name: str, logger, use_init: bool = False) -> Optional[list]:
        """
        Load the latest versioned communities file or initial communities.
        
        Args:
            data_path: Path to data directory
            bucket_name: Bucket name for storage
            logger: Logger instance
            use_init: If True, load initial communities file instead of latest version
            
        Returns:
            List of communities or None if not found
        """
        try:
            if use_init:
                # Load the baseline initial communities file
                init_file = 'communities_init'
                logger.info(f"Loading initial communities from {init_file}")
                init_communities = load_data(data_path, init_file, bucket_name, data_extension='pkl')
                if init_communities is None or len(init_communities) == 0:
                    logger.warning(f"Initial communities not found in bucket. Trying local artifacts...")
                    local_communities = ROOT_DIR / "artifacts"
                    init_communities = load_data(local_communities, init_file, None, data_extension='pkl')
                return init_communities
            
            # Try to load the latest versioned file
            latest_file = 'communities_latest'
            logger.info(f"Attempting to load latest communities from {latest_file}")
            latest_communities = load_data(data_path, latest_file, bucket_name, data_extension='pkl')
            if latest_communities is None or len(latest_communities) == 0:
                logger.warning(f"Latest communities file is empty or not found. Falling back to initial communities...")
                # Fallback to initial communities if latest not available
                init_file = 'communities_init'
                latest_communities = load_data(data_path, init_file, bucket_name, data_extension='pkl')
                if latest_communities is None or len(latest_communities) == 0:
                    logger.warning(f"Initial communities not found in bucket either. Trying local artifacts...")
                    local_communities = ROOT_DIR / "artifacts"
                    latest_communities = load_data(local_communities, init_file, None, data_extension='pkl')
            return latest_communities
            
        except FileNotFoundError:
            logger.warning(f"No communities file found. Will start with empty partition.")
            return None
        except Exception as e:
            logger.warning(f"Failed to load communities: {e}. Will start with empty partition.")
            return None

def save_versioned_communities(final_communities: list, data_path: str, bucket_name: str, logger) -> None:
        """
        Save communities with versioning:
        1. Save with timestamp for history
        2. Update the 'latest' file
        
        Args:
            final_communities: List of community sets to save
            data_path: Path to data directory
            bucket_name: Bucket name for storage
            logger: Logger instance
        """
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        
        # Save versioned file with timestamp
        versioned_filename = f'communities_{timestamp}'
        save_data(final_communities, data_path, versioned_filename, bucket_name, data_extension='pkl')
        logger.info(f"Saved versioned communities to {versioned_filename}.pkl")
        
        # Update the latest file
        latest_filename = 'communities_latest'
        save_data(final_communities, data_path, latest_filename, bucket_name, data_extension='pkl')
        logger.info(f"Updated latest communities file: {latest_filename}.pkl")

def main(scoring_config: dict, df_node_feature: pd.DataFrame = None, 
                        edges_grouped: pd.DataFrame = None, use_init_communities: bool = False):
        """
        Function to perform community detection on the graph using the Louvain method.

        Args:
        - scoring_config: Configuration dictionary
        - df_node_feature: DataFrame containing node features
        - edges_grouped: DataFrame containing grouped edge data with weights and connection types
        - use_init_communities: If True, load initial communities instead of latest version

        Returns:
        - final_communities: List of sets, where each set contains node IDs belonging to a community
        """
        logger = log_utility(model_id = scoring_config.get("model_id", "unknown_model"), component = 'community_detection')
        logger.info("Starting graph construction and community detection")

        local_write = scoring_config.get('local_write', False)
        data_path = scoring_config['data_path'] 
        input_env =  scoring_config['input_env']
        # model_path = scoring_config['model_path']
        
        # Bucket and path setup
        principle = scoring_config.get('principle', 'user')
        s_number = scoring_config.get('s_number', 's745998')
        project_id = scoring_config.get('project_id', 'ria-vul-bbcc')
        if principle == 'user':
            bucket_name = f"{project_id}-aap-{input_env}-{s_number}-bucket"
        elif principle == "service":
            bucket_name = f"{project_id}-aap-{input_env}-model-bucket"
        
        # load node data if not provided
        if df_node_feature is None:
             df_node_feature = load_data(data_path, 'processed_node', bucket_name, data_extension='pkl')
        if edges_grouped is None:
            edges_grouped = load_data(data_path, 'processed_edges', bucket_name, data_extension='pkl')
        
        # Build the CTP network graph
        try:
          graph_data, G, all_nodes_df = build_network (df_node_feature, edges_grouped)
          logger.info("Graph construction completed")   
        except Exception as e:
          logger.error(f"Graph construction failed: {e}")
          raise

        # Load communities with versioning support
        init_communities = load_latest_communities(data_path, bucket_name, logger, use_init=use_init_communities)

        try:
            # Convert initial communities to partition dict if loaded
            if init_communities:
                init_communities = communities_to_partition(init_communities)
                # Build warm start partition for all nodes in G
                init_communities = build_warm_start_partition(G, init_communities, "edge_weight")
                logger.info(f"Using warm start with {len(set(init_communities.values()))} initial communities")
            else:
                logger.info("Starting community detection without warm start")

            final_communities = []
            
            # Perform recursive community detection
            recursive_community_detection(
                G,
                final_communities,
                resolution=1.2,      # try a different resolution if you want more/smaller splits
                threshold=50,        # your stopping size
                seed_value=42,
                init_partition=init_communities
            )
            logger.info(f"Community detection completed: {len(final_communities)} communities detected")   
        except Exception as e:
            logger.error(f"Community detection failed: {e}")
            raise
        
        # Save communities with versioning
        save_versioned_communities(final_communities, data_path, bucket_name, logger)
        
        # Save Pyg and networkx graph if local_write is True
        if local_write:
             save_data(graph_data, data_path, 'graph_data', bucket_name, data_extension='pt')
             save_data(G, data_path, 'networkx_graph', bucket_name, data_extension='pkl')
             save_data(all_nodes_df, data_path, 'all_nodes_df', bucket_name, data_extension='csv')
             logger.info("Graph-related data saved locally" )

        return graph_data, G, all_nodes_df, final_communities

if __name__ == "__main__":
    # Example usage
    score_config = SYNDICATE_CONFIG
    # Set use_init_communities=True to load initial baseline communities
    # Set use_init_communities=False (default) to load latest versioned communities
    graph_data, G, all_nodes_df, final_communities = main(score_config, use_init_communities=False)

          
