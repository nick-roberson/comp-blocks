import logging
import time

import pandas as pd
import typer
from rich import print

from src.blocks.predict.predict_tabular import PredictBlock, PredictModelParams
from src.blocks.prepare.prepare_taxi import (PrepareTaxiBlock,
                                             PrepareTaxiBlockParams)
from src.blocks.train.train_tabular import TrainModelBlock, TrainModelParams
from src.runners.parallel_runner import ParallelRunner
from src.runners.sequential_runner import SequentialRunner
from src.utils.logging import init_logging

app = typer.Typer()

# Params specifically for the taxi fare example
TAXI_DATA = "data/NYCTaxiFares.csv"
MODEL_FILE = "TaxiFareRegrModel.pt"
CATEGORICAL_COLUMNS = ["hour", "am_or_pm", "weekday", "time_of_day"]
CONTINUOUS_COLUMNS = [
    "pickup_latitude",
    "pickup_longitude",
    "dropoff_latitude",
    "dropoff_longitude",
    "passenger_count",
    "dist_km",
]
TARGET_COLUMN = "fare_amount"
BATCH_SIZE = 60000
TEST_SIZE = 12000
EPOCHS = 500
MODEL_LAYERS = [200, 100]
MODEL_DROPOUT = 0.4


@app.command()
def train_taxi(
    verbose: bool = False,
):
    """Run a more complex model training example of a computation workflow."""
    # Initialize logging and load the data
    init_logging(level="DEBUG" if verbose else "INFO")

    # Read in the data
    test_data = pd.read_csv(TAXI_DATA)
    print(f"Loaded data with {len(test_data)} records.")

    # Create params for the preparation block
    prepare_params = PrepareTaxiBlockParams(id_col="id", log_level="DEBUG")

    # Create params for the training block
    train_params = TrainModelParams(
        model_file=MODEL_FILE,
        cat_cols=CATEGORICAL_COLUMNS,
        cont_cols=CONTINUOUS_COLUMNS,
        y_col=TARGET_COLUMN,
        batch_size=BATCH_SIZE,
        test_size=TEST_SIZE,
        epochs=EPOCHS,
        model_layers=MODEL_LAYERS,
        model_dropout=MODEL_DROPOUT,
        # Base Params
        num_retries=3,
        log_level="INFO",
    )

    # Create params for the prediction block
    predict_params = PredictModelParams(
        model_file=MODEL_FILE,
        cat_cols=CATEGORICAL_COLUMNS,
        cont_cols=CONTINUOUS_COLUMNS,
        model_layers=MODEL_LAYERS[:2],
        model_dropout=MODEL_DROPOUT,
        # Base Params
        num_retries=3,
        log_level="INFO",
    )

    # Create a sequential runner with a map of blocks to run
    sequential_runner = SequentialRunner(
        block_map={
            # (Parallel) Pre-process data
            1: ParallelRunner(
                block=PrepareTaxiBlock(params=prepare_params),
                chunk_size=10000,
                use_thread_pool=True,
            ),
            # (Sequential) Train the model
            2: TrainModelBlock(params=train_params),
            # (Parallel) Predict using the model
            3: ParallelRunner(
                block=PredictBlock(params=predict_params),
                chunk_size=10000,
                use_thread_pool=True,
            ),
        }
    )

    # Run the sequential runner and time the execution
    start_time = time.time()
    result = sequential_runner(test_data)
    duration = round(time.time() - start_time, 6)

    # Print the result and duration
    result = result.reset_index(drop=True)
    print(f"\nDataframe Preview:\n{result.head(10)}")
    print(f"\nCompleted all work in {duration} seconds.")


if __name__ == "__main__":
    app()
