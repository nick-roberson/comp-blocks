import logging
import os
from typing import Tuple

import numpy as np
import pandas as pd
import torch
from torch import nn

from src.block_base import BlockBase
from src.blocks.train.models.tabular_model import TabularModel
from src.params_base import BlockParamBase

logger = logging.getLogger(__name__)


class PredictModelParams(BlockParamBase):

    # Categorical columns to use for the model
    cat_cols: list = ["hour", "am_or_pm", "weekday"]
    cont_cols: list = [
        "pickup_latitude",
        "pickup_longitude",
        "dropoff_latitude",
        "dropoff_longitude",
        "passenger_count",
        "dist_km",
    ]

    # Model architecture parameters
    model_file: str = "TaxiFareRegrModel.pt"
    model_layers: list = [200, 100]
    model_dropout: float = 0.4

    # Target column and prediction columns
    target_col: str = "fare_amount"
    prediction_col: str = "predictions"
    difference_col: str = "difference"


class PredictBlock(BlockBase):
    """
    Block to load a trained model and make predictions on provided data.
    """

    params: PredictModelParams

    def load_model(self, input_df: pd.DataFrame) -> nn.Module:
        """
        Load the trained model from the specified path.

        Returns:
            nn.Module: The loaded PyTorch model.
        """
        # Get the path to the model file
        model_path = os.path.join(os.getcwd(), self.params.model_file)

        # Get the number of unique categories for each categorical feature, and set the embedding sizes
        cat_szs = [
            len(pd.Categorical(input_df[col]).categories)
            for col in self.params.cat_cols
        ]
        emb_szs = [(size, min(50, (size + 1) // 2)) for size in cat_szs]

        # Load the model and set it to evaluation mode
        model = TabularModel(
            emb_szs,
            len(self.params.cont_cols),
            1,
            self.params.model_layers,
            p=self.params.model_dropout,
        )
        model.load_state_dict(torch.load(model_path))

        # Set the model to evaluation mode and return
        model.eval()
        return model

    def prepare_tensors(
        self, input_df: pd.DataFrame
    ) -> Tuple[torch.Tensor, torch.Tensor]:
        """
        Prepare tensors from DataFrame columns.

        Args:
            input_df (pd.DataFrame): Input data frame to process.

        Returns:
            Tuple[torch.Tensor, torch.Tensor]: Tensors for categorical and continuous features.
        """
        for col in self.params.cat_cols:
            input_df[col] = pd.Categorical(
                input_df[col], categories=pd.Categorical(input_df[col]).categories
            )

        cats = np.stack(
            [input_df[col].cat.codes.values for col in self.params.cat_cols], 1
        )
        conts = np.stack([input_df[col].values for col in self.params.cont_cols], 1)
        return torch.tensor(cats, dtype=torch.int64), torch.tensor(
            conts, dtype=torch.float
        )

    def predict(
        self, model: nn.Module, cats: torch.Tensor, conts: torch.Tensor
    ) -> np.ndarray:
        """
        Make predictions using the trained model.

        Args:
            model (nn.Module): The trained model.
            cats (torch.Tensor): Categorical feature's tensor.
            conts (torch.Tensor): Continuous feature's tensor.

        Returns:
            np.ndarray: Predicted values.
        """
        with torch.no_grad():
            predictions = model(cats, conts).numpy()
        return predictions

    def run(self, input_df: pd.DataFrame) -> pd.DataFrame:
        """
        Main method to load model, prepare data, and make predictions.

        Args:
            input_df (pd.DataFrame): Input DataFrame to process.

        Returns:
            pd.DataFrame: DataFrame with appended predictions.
        """
        # Load model, prepare tensors, and make predictions
        model = self.load_model(input_df=input_df)
        cats, conts = self.prepare_tensors(input_df)
        predictions = self.predict(model, cats, conts)

        # Add predictions to the input DataFrame
        input_df[self.params.prediction_col] = predictions

        # Calculate the difference between the prediction and the target
        input_df[self.params.difference_col] = (
            input_df[self.params.target_col] - input_df[self.params.prediction_col]
        )

        # Move the target, prediction, and difference columns to the end
        priority_cols = [
            self.params.target_col,
            self.params.prediction_col,
            self.params.difference_col,
        ]
        input_df = input_df[
            [col for col in input_df.columns if col not in priority_cols]
            + priority_cols
        ]

        # Return the DataFrame with predictions
        return input_df
