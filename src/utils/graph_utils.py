# Functions for use in other scripts
import os
import torch
from torch_geometric.data import Data
import pandas as pd
from torch_geometric.utils import to_networkx
from src.utils.utils import save_data
import networkx as nx
import matplotlib.pyplot as plt

# Function to build the network from edge and node data
def build_network(df_edges, df_encoded, conf):
    # Unpack different types of edges
    cust_df, doc_lawyer_df, doc_psych_df, doc_repair_df, vehicle_df = df_edges
    
    # Concatenate all different types of edges
    edges_all = pd.concat([cust_df, doc_lawyer_df, doc_psych_df, doc_repair_df, vehicle_df], ignore_index=True)
    
    # Create undirected edges by sorting the node IDs
    edges_all['edge'] = edges_all.apply(lambda row: (row['claim_number_1'], row['claim_number_2']), axis=1)
    
    # Group by 'edge' and sum the weights
    edges_grouped = edges_all.groupby('edge').agg({'weight': 'sum'}).reset_index()
    
    # Split 'edge' back into 'source' and 'target'
    edges_grouped[['source', 'target']] = pd.DataFrame(edges_grouped['edge'].tolist(), index=edges_grouped.index)
    edges_grouped.drop(columns='edge', inplace=True)
    
    # Create a mapping from node IDs to indices
    edge_node_ids = set(edges_grouped['source']).union(edges_grouped['target'])
    feature_node_ids = set(df_encoded['claim_number'])
    all_node_ids = edge_node_ids.union(feature_node_ids)
    node_id_to_idx = {node_id: idx for idx, node_id in enumerate(sorted(all_node_ids))}
    
    # Map claim numbers to indices in edges
    edges_grouped['source_idx'] = edges_grouped['source'].map(node_id_to_idx)
    edges_grouped['target_idx'] = edges_grouped['target'].map(node_id_to_idx)
    
    # Prepare node features, including nodes without edges
    df_encoded['node_idx'] = df_encoded['claim_number'].map(node_id_to_idx)
    all_nodes_df = df_encoded.sort_values('node_idx').reset_index(drop=True).set_index('node_idx')
    feature_cols = df_encoded.columns.drop(['claim_number', 'node_idx', 'investigation_flag', 'triage_flag'])
    
    # Extract node features, edge index, edge weights, and labels as tensors
    x = torch.tensor(all_nodes_df[feature_cols].values, dtype=torch.float)
    edge_index = torch.tensor([edges_grouped['source_idx'].values, edges_grouped['target_idx'].values], dtype=torch.long)
    edge_weight = torch.tensor(edges_grouped['weight'].values, dtype=torch.float)
    y = torch.tensor(all_nodes_df['investigation_flag'].values, dtype=torch.long)
    
    # Create PyG data object
    data = Data(x=x, edge_index=edge_index, edge_weight=edge_weight, y=y)
    
    # Save the data object
    graph_path = os.path.join(conf.data_path, 'ctp_pyg_data.pt')
    torch.save(data, graph_path)
    
    # Convert PyG object to networkx graph
    G = to_networkx(data, node_attrs=['x', 'y'], edge_attrs=['edge_weight'], to_undirected=True)
    
    # Add claim_number as node attribute in networkx graph
    for node_idx, claim_number in all_nodes_df['claim_number'].items():
        G.nodes[node_idx]['claim_number'] = claim_number
    
    # Print the number of nodes and edges
    print(f"Number of nodes: {G.number_of_nodes()}")
    print(f"Number of edges: {G.number_of_edges()}")
    
    # Save the networkx graph and node data
    save_data(G, conf.data_path, 'ctp_network', data_extension='pkl')
    save_data(all_nodes_df, conf.data_path, 'node_data', data_extension='csv')
    
    return data, G, all_nodes_df


# Function to visualize a community with claim numbers as node labels
def visualize_community(G, communities, community_idx):
    # Create a subgraph containing only the nodes in the cluster
    selected_community = communities[community_idx]
    community_subgraph = G.subgraph(selected_community)

    # Create a color map for nodes based on an attribute 'y'
    color_map = []
    for node in community_subgraph.nodes(data=True):
        if node[1].get('y') == 1:
            color_map.append('red')
        else:
            color_map.append('skyblue')

    # Create edge labels based on an attribute 'label'
    edge_labels = nx.get_edge_attributes(community_subgraph, 'edge_weight')

    # Plot the subgraph for the selected community
    pos = nx.spring_layout(community_subgraph)  # You can change the layout as needed
    plt.figure(figsize=(6, 4))
    nx.draw(community_subgraph, pos, with_labels=False, node_color=color_map, edge_color='grey', node_size=800, font_size=10)
    # nx.draw_networkx_edge_labels(community_subgraph, pos, edge_labels=edge_labels, font_size=6, label_pos=0.5)
    nx.draw_networkx_labels(community_subgraph, pos, labels={node: data['claim_number'] for node, data in community_subgraph.nodes(data=True)}, font_size=8)
    plt.title(f'Subgraph for Community {community_idx}')
    plt.show()