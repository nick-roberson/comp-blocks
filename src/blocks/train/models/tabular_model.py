import logging
from typing import List, Tuple

import torch
import torch.nn as nn

logger = logging.getLogger(__name__)


class TabularModel(nn.Module):
    """
    A neural network for tabular data that incorporates embeddings for categorical features,
    batch normalization for continuous features, and a series of linear layers.

    Attributes:
        embeds (nn.ModuleList): List of embedding layers for categorical features.
        emb_drop (nn.Dropout): Dropout layer for embeddings.
        bn_cont (nn.BatchNorm1d): Batch normalization for continuous features.
        layers (nn.Sequential): Sequential container of all layers including linear, ReLU, batch norm, and dropout.
    """

    def __init__(
        self,
        emb_szs: List[Tuple[int, int]],
        n_cont: int,
        out_sz: int,
        layers: List[int],
        p: float = 0.5,
    ):
        """
        Initializes the TabularModel with specified architecture settings.

        Args:
            emb_szs (List[Tuple[int, int]]): A list of tuples where each tuple contains the number of unique values
                and the embedding size for a categorical feature.
            n_cont (int): Number of continuous features.
            out_sz (int): Size of the output layer.
            layers (List[int]): List of integers where each integer specifies the number of neurons in a hidden layer.
            p (float, optional): Dropout probability used in the embedding dropout and each hidden layer. Defaults to 0.5.
        """
        logger.debug(
            f"Initializing TabularModel with {emb_szs}, {n_cont}, {out_sz}, {layers}, {p}"
        )
        super().__init__()
        self.embeds = nn.ModuleList([nn.Embedding(ni, nf) for ni, nf in emb_szs])
        self.emb_drop = nn.Dropout(p)
        self.bn_cont = nn.BatchNorm1d(n_cont)
        layerlist = [
            self._add_layer(n_in, n_out, p)
            for n_in, n_out in zip(
                [sum(nf for _, nf in emb_szs) + n_cont] + layers[:-1], layers
            )
        ]
        layerlist.append(nn.Linear(layers[-1], out_sz))
        self.layers = nn.Sequential(*layerlist)
        logger.debug(
            f"TabularModel initialized with {self.embeds}, {self.emb_drop}, {self.bn_cont}, {self.layers}"
        )

    def _add_layer(self, n_in: int, n_out: int, p: float) -> nn.Sequential:
        """
        Helper function to create a single layer block for the network.

        Args:
            n_in (int): Number of input features to the layer.
            n_out (int): Number of output features from the layer.
            p (float): Dropout probability.

        Returns:
            nn.Sequential: A sequential layer consisting of Linear, ReLU, BatchNorm, and Dropout.
        """
        layer = [
            nn.Linear(n_in, n_out),
            nn.ReLU(inplace=True),
            nn.BatchNorm1d(n_out),
            nn.Dropout(p),
        ]
        return nn.Sequential(*layer)

    def forward(self, x_cat: torch.Tensor, x_cont: torch.Tensor) -> torch.Tensor:
        """
        Forward pass of the model which processes both categorical and continuous data.

        Args:
            x_cat (torch.Tensor): Tensor containing encoded categorical data.
            x_cont (torch.Tensor): Tensor containing normalized continuous data.

        Returns:
            torch.Tensor: The output of the model after processing input through all layers.
        """
        x = torch.cat([e(x_cat[:, i]) for i, e in enumerate(self.embeds)], 1)
        x = self.emb_drop(x)
        x_cont = self.bn_cont(x_cont)
        x = torch.cat([x, x_cont], 1)
        return self.layers(x)
