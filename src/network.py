import torch
import torch.nn as nn
import torch.nn.functional as F
from torch_geometric.nn import GCNConv
from torch_geometric.utils import negative_sampling
from torch_geometric.data import Data

# Model Definition
class Encoder(nn.Module):
    def __init__(self, nfeat, nhid, dropout):
        super(Encoder, self).__init__()
        self.gc1 = GCNConv(nfeat, nhid)
        self.gc2 = GCNConv(nhid, nhid)
        self.dropout = dropout

    def forward(self, x, edge_index, edge_weight=None):
        x = F.relu(self.gc1(x, edge_index, edge_weight))
        x = F.dropout(x, p=self.dropout, training=self.training)
        x = F.relu(self.gc2(x, edge_index, edge_weight))
        return x

class Attribute_Decoder(nn.Module):
    def __init__(self, nfeat, nhid, dropout):
        super(Attribute_Decoder, self).__init__()
        self.gc1 = GCNConv(nhid, nhid)
        self.gc2 = GCNConv(nhid, nfeat)
        self.dropout = dropout

    def forward(self, x, edge_index, edge_weight=None):
        x = F.relu(self.gc1(x, edge_index, edge_weight))
        x = F.dropout(x, p=self.dropout, training=self.training)
        x = self.gc2(x, edge_index, edge_weight)
        return x

class Structure_Decoder(nn.Module):
    def __init__(self, nhid, dropout):
        super(Structure_Decoder, self).__init__()
        self.gc1 = GCNConv(nhid, nhid)
        self.dropout = dropout

    def forward(self, x, edge_index, edge_weight=None):
        x = F.relu(self.gc1(x, edge_index, edge_weight))
        x = F.dropout(x, p=self.dropout, training=self.training)
        x = torch.matmul(x, x.t())
        return x
    


class Dominant(nn.Module):
    def __init__(self, feat_size, hidden_size, dropout):
        super(Dominant, self).__init__()
        self.shared_encoder = Encoder(feat_size, hidden_size, dropout)
        self.attr_decoder = Attribute_Decoder(feat_size, hidden_size, dropout)
        self.struct_decoder = Structure_Decoder(hidden_size, dropout)
    
    def forward(self, data):
        x, edge_index, edge_weight = data.x, data.edge_index, data.edge_weight
        # Encode
        x_encoded = self.shared_encoder(x, edge_index, edge_weight)
        # Decode feature matrix
        x_hat = self.attr_decoder(x_encoded, edge_index, edge_weight)
        # Decode adjacency matrix
        struct_reconstructed = self.struct_decoder(x_encoded, edge_index, edge_weight)
        # Return reconstructed matrices
        return struct_reconstructed, x_hat, x_encoded

    def predict(self, data, get_emb=False):
        self.eval()
        with torch.no_grad():
            struct_reconstructed, x_hat, x_encoded = self(data)

            # Attribute Reconstruction Error
            attr_errors = torch.sqrt(torch.mean((x_hat - data.x) ** 2, dim=1))

            # Structure Reconstruction Error
            adj_true = torch.zeros(data.num_nodes, data.num_nodes, device='cpu')
            adj_true[data.edge_index[0], data.edge_index[1]] = 1
            struct_errors = torch.sqrt(torch.mean((struct_reconstructed - adj_true) ** 2, dim=1))

            # Total Anomaly Score
            anomaly_scores = attr_errors + struct_errors
            if get_emb == True:
                return anomaly_scores, x_encoded
            else:
                return anomaly_scores

    def train_model(self, data, num_epochs=200, learning_rate=0.01, weight_decay=5e-4):
        device = torch.device('cuda' if torch.cuda.is_available() else 'cpu')
        data = data.to(device)
        self.to(device)

        # Define optimizer
        optimizer = torch.optim.Adam(self.parameters(), lr=learning_rate, weight_decay=weight_decay)

        # Define loss functions
        # Attribute reconstruction loss (MSE Loss)
        attr_criterion = nn.MSELoss()
        struct_criterion = nn.MSELoss()

        # Training loop
        for epoch in range(num_epochs):
            self.train()
            optimizer.zero_grad()
            
            # Forward pass
            struct_reconstructed, x_hat, x_encoded = self(data)
            
            # Attribute reconstruction loss
            attr_loss = attr_criterion(x_hat, data.x)
            
            # Structure reconstruction loss
            # Use negative sampling for efficiency
            pos_edge_index = data.edge_index
            neg_edge_index = negative_sampling(
                edge_index=pos_edge_index, num_nodes=data.num_nodes,
                num_neg_samples=pos_edge_index.size(1)
            )

            # Create adjacency matrix labels
            adj_matrix = torch.zeros((data.num_nodes, data.num_nodes), device=device)
            adj_matrix[pos_edge_index[0], pos_edge_index[1]] = 1
            adj_matrix[neg_edge_index[0], neg_edge_index[1]] = 0

            # Structure reconstruction loss (MSE Loss)
            struct_loss = struct_criterion(struct_reconstructed, adj_matrix)

            # Total loss
            loss = 1000 * attr_loss + struct_loss

            # Backward pass and optimization
            loss.backward()
            optimizer.step()

            print(f'Epoch {epoch+1}/{num_epochs}, Loss: {loss.item():.2f}, Struct loss: {struct_loss.item():.2f}, Attr loss: {attr_loss.item():.2f}')

        return self