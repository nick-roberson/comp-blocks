import logging
import time

import pandas as pd
import typer
from rich import print

from src.blocks.predict.predict_taxi import PredictBlock, PredictModelParams
from src.blocks.prepare.prepare_taxi import (PrepareTaxiBlock,
                                             PrepareTaxiBlockParams)
from src.blocks.train.train_taxi import TrainModelBlock, TrainModelParams
from src.runners.parallel_runner import ParallelRunner
from src.runners.sequential_runner import SequentialRunner

app = typer.Typer()

# Params specifically for the taxi fare example
TAXI_DATA = "data/NYCTaxiFares.csv"
MODEL_NAME = "TaxiFareRegrModel.pt"
CATEGORICAL_COLUMNS = ["hour", "am_or_pm", "weekday"]
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
EPOCHS = 300
MODEL_LAYERS = [200, 100]
MODEL_DROPOUT = 0.4


def init_logging():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )


@app.command()
def train_taxi():
    """Run a simple example of a computation workflow."""
    # Initialize logging and load the data
    init_logging()
    test_data = pd.read_csv(TAXI_DATA)
    print(f"Loaded data with {len(test_data)} records.")

    # Create params for the preparation block
    prepare_params = PrepareTaxiBlockParams(id_col="id")

    # Create params for the training block
    train_params = TrainModelParams(
        model_name=MODEL_NAME,
        cat_cols=CATEGORICAL_COLUMNS,
        cont_cols=CONTINUOUS_COLUMNS,
        y_col=TARGET_COLUMN,
        batch_size=BATCH_SIZE,
        test_size=TEST_SIZE,
        epochs=EPOCHS,
        model_layers=MODEL_LAYERS,
        model_dropout=MODEL_DROPOUT,
    )

    # Create params for the prediction block
    predict_params = PredictModelParams(
        model_name=MODEL_NAME,
        cat_cols=CATEGORICAL_COLUMNS,
        cont_cols=CONTINUOUS_COLUMNS,
        model_layers=MODEL_LAYERS[:2],
        model_dropout=MODEL_DROPOUT,
    )

    # Create a sequential runner with a map of blocks to run
    sequential_runner = SequentialRunner(
        block_map={
            # Prepare the data in parallel using a chunk size of 10,000
            1: ParallelRunner(
                block=PrepareTaxiBlock(params=prepare_params),
                chunk_size=10000,
                use_thread_pool=True,
            ),
            # Train the model in series
            2: TrainModelBlock(params=train_params),
            # Predict in parallel using a chunk size of 10,000
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
