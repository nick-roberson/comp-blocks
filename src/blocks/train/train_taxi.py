import os.path
from typing import List, Tuple

import numpy as np
import pandas as pd
import torch
import torch.nn as nn
from rich import print
from typing_extensions import override

from src.block_base import BlockBase
from src.blocks.train.models.tabular_model import TabularModel
from src.params_base import BlockParamBase


class TrainModelParams(BlockParamBase):
    """Parameters for the TrainTaxiModel."""

    model_name: str = "TaxiFareRegrModel.pt"
    id_col: str = "id"
    cat_cols: list = ["hour", "am_or_pm", "weekday"]
    cont_cols: list = [
        "pickup_latitude",
        "pickup_longitude",
        "dropoff_latitude",
        "dropoff_longitude",
        "passenger_count",
        "dist_km",
    ]
    y_col: str = "fare_amount"
    batch_size: int = 60000
    test_size: int = int(batch_size * 0.2)
    epochs: int = 300

    # Model Params
    model_layers: List[int] = [200, 100]
    model_dropout: float = 0.4


class TrainModelBlock(BlockBase):
    """Prepare block to clean, prepare the input data, and train a model."""

    params: TrainModelParams = TrainModelParams()

    @override
    def validate(self, input_df: pd.DataFrame) -> None:
        """Validate the input dataframe to ensure it is not empty.

        Args:
            input_df (pd.DataFrame): The input DataFrame to validate.

        Raises:
            ValueError: If the input DataFrame is empty.
        """
        # Ensure the input DataFrame is not empty
        if input_df.empty:
            raise ValueError("Input dataframe must not be empty")

        # Ensure that all required columns are present
        for col in self.params.cat_cols + self.params.cont_cols + [self.params.y_col]:
            if col not in input_df.columns:
                raise ValueError(f"Column '{col}' not found in input DataFrame")

        # Ensure that the target column is not empty
        if input_df[self.params.y_col].isnull().any():
            raise ValueError(
                f"Target column '{self.params.y_col}' contains missing values"
            )

        # Ensure that the target column is not negative
        if (input_df[self.params.y_col] < 0).any():
            raise ValueError(
                f"Target column '{self.params.y_col}' contains negative values"
            )

    @override
    def run(self, input_df: pd.DataFrame) -> pd.DataFrame:
        """Main method to run data preparation and model training processes.

        Args:
            input_df (pd.DataFrame): The input DataFrame to process.

        Returns:
            pd.DataFrame: The processed DataFrame, potentially with modifications or additional columns.
        """
        print("******************************************** VALIDATE")
        # Validate and convert columns to categories
        self.validate(input_df=input_df)
        self.convert_columns_to_categories(input_df=input_df)

        # Prepare tensors and setup model
        print(
            "******************************************** PREPARE TENSORS AND SETUP MODEL"
        )
        cats, conts, y = self.prepare_tensors(input_df=input_df)
        model, criterion, optimizer = self.setup_model(input_df=input_df, conts=conts)

        # Train
        print("******************************************** TRAIN THE MODEL")
        losses = self.train_model(
            model=model,
            criterion=criterion,
            optimizer=optimizer,
            cats=cats,
            conts=conts,
            y=y,
        )

        # Evaluate
        print("******************************************** EVALUATE THE MODEL")
        self.evaluate_model(
            model=model, criterion=criterion, cats=cats, conts=conts, y=y, losses=losses
        )
        return input_df

    def convert_columns_to_categories(self, input_df: pd.DataFrame) -> None:
        """Converts specified columns in the DataFrame to categorical data types.

        Args:
            input_df (pd.DataFrame): The DataFrame whose columns are to be converted.
        """
        for cat in self.params.cat_cols:
            input_df[cat] = input_df[cat].astype("category")

    def prepare_tensors(
        self, input_df: pd.DataFrame
    ) -> Tuple[torch.Tensor, torch.Tensor, torch.Tensor]:
        """Converts DataFrame columns into PyTorch tensors for model input.

        Args:
            input_df (pd.DataFrame): The DataFrame from which to create tensors.

        Returns:
            Tuple[torch.Tensor, torch.Tensor, torch.Tensor]: Tensors for categorical features, continuous features, and target values.
        """
        cats = np.stack(
            [input_df[col].cat.codes.values for col in self.params.cat_cols], 1
        )
        conts = np.stack([input_df[col].values for col in self.params.cont_cols], 1)
        y = input_df[self.params.y_col].values
        return (
            torch.tensor(cats, dtype=torch.int64),
            torch.tensor(conts, dtype=torch.float),
            torch.tensor(y, dtype=torch.float).reshape(-1, 1),
        )

    def setup_model(
        self, input_df: pd.DataFrame, conts: torch.Tensor
    ) -> Tuple[nn.Module, nn.Module, torch.optim.Optimizer]:
        """Initializes the neural network model, loss function, and optimizer.

        Args:
            conts (torch.Tensor): Tensor containing continuous feature data used to determine input size for the model.

        Returns:
            Tuple[nn.Module, nn.Module, torch.optim.Optimizer]: The initialized model, criterion, and optimizer.
        """
        cat_szs = [
            len(pd.Categorical(input_df[col]).categories)
            for col in self.params.cat_cols
        ]
        emb_szs = [(size, min(50, (size + 1) // 2)) for size in cat_szs]
        model = TabularModel(
            emb_szs,
            conts.shape[1],
            1,
            self.params.model_layers,
            p=self.params.model_dropout,
        )
        criterion = nn.MSELoss()
        optimizer = torch.optim.Adam(model.parameters(), lr=0.001)
        return model, criterion, optimizer

    def train_model(
        self,
        model: nn.Module,
        criterion: nn.Module,
        optimizer: torch.optim.Optimizer,
        cats: torch.Tensor,
        conts: torch.Tensor,
        y: torch.Tensor,
    ) -> List:
        """Trains the neural network model using the provided data.

        Args:
            model (nn.Module): The neural network model to train.
            criterion (nn.Module): The loss function.
            optimizer (torch.optim.Optimizer): The optimizer.
            cats (torch.Tensor): Categorical feature data.
            conts (torch.Tensor): Continuous feature data.
            y (torch.Tensor): Target data.
        Returns:
            List: The list of losses for each epoch.
        """
        losses = []

        # Split the data into training and testing sets
        cat_train, cat_test = (
            cats[: self.params.batch_size - self.params.test_size],
            cats[self.params.batch_size - self.params.test_size :],
        )
        con_train, con_test = (
            conts[: self.params.batch_size - self.params.test_size],
            conts[self.params.batch_size - self.params.test_size :],
        )
        y_train, y_test = (
            y[: self.params.batch_size - self.params.test_size],
            y[self.params.batch_size - self.params.test_size :],
        )

        # Train the model
        for i in range(self.params.epochs):
            y_pred = model(cat_train, con_train)

            # RMSE
            loss = torch.sqrt(criterion(y_pred, y_train))

            optimizer.zero_grad()
            loss.backward()
            optimizer.step()
            losses.append(loss.item())
            if i % 25 == 0:
                print(f"Epoch {i}: Loss = {loss.item():.8f}")

        return losses

    def evaluate_model(
        self,
        model: nn.Module,
        criterion: nn.Module,
        cats: torch.Tensor,
        conts: torch.Tensor,
        y: torch.Tensor,
        losses: List = [],
    ) -> None:
        """Evaluates the model on the test dataset and prints performance metrics.

        Args:
            model (nn.Module): The neural network model.
            criterion (nn.Module): The loss function.
            cats (torch.Tensor): Categorical feature data for testing.
            conts (torch.Tensor): Continuous feature data for testing.
            y (torch.Tensor): Target data for testing.
        """
        # Evaluate the model
        cat_test, con_test, y_test = (
            cats[self.params.batch_size - self.params.test_size :],
            conts[self.params.batch_size - self.params.test_size :],
            y[self.params.batch_size - self.params.test_size :],
        )
        with torch.no_grad():
            y_val = model(cat_test, con_test)
            loss = torch.sqrt(criterion(y_val, y_test))
            print(f"Final RMSE: {loss:.8f}")

        # Print some predictions
        predictions = []
        for i in range(min(50, len(y_test))):
            pred, actual = y_val[i].item(), y_test[i].item()
            diff = np.abs(pred - actual)
            predictions.append((pred, actual, diff))

        # Sort the predictions by the difference between the predicted and actual values
        predictions.sort(key=lambda x: x[2])
        print("Predictions:")
        for i, (pred, actual, diff) in enumerate(predictions):
            print(
                f"{i + 1:2}. Predicted: {pred:.4f}, Actual: {actual:.4f}, Diff: {diff:.4f}"
            )

        # Save the model if training is complete
        if len(losses) == self.params.epochs:
            model_fp = os.path.abspath(self.params.model_name)
            torch.save(model.state_dict(), model_fp)
            print(f"Model saved successfully to path '{model_fp}'")
        else:
            print("Model training incomplete.")
            raise ValueError("Model training incomplete.")
