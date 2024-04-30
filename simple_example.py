import logging
import time

import pandas as pd
import typer
from pydantic import BaseModel
from rich import print
from typing_extensions import override

from src.block_base import BlockBase
from src.blocks.prepare.prepare_block import PrepareBlock
from src.blocks.simple.average.average_block import (AverageBlock,
                                                     AverageBlockParams)
from src.params_base import BlockParamBase
from src.runners.parallel_runner import ParallelRunner
from src.runners.sequential_runner import SequentialRunner
from src.utils.logging import init_logging

app = typer.Typer()


############################################################################################
# AddNBlock
############################################################################################


class AddNBlockParams(BlockParamBase):
    n: int
    target_column: str


class AddNBlock(BlockBase):
    """Simple block to add a number to a column."""

    params: AddNBlockParams

    @override
    def validate(self, input_df: pd.DataFrame) -> None:
        """Validate that the input dataframe is not empty and that the target column exists and is numeric."""
        if input_df.empty:
            raise ValueError("Input dataframe must not be empty")
        # check that col exists
        if self.params.target_column not in input_df.columns:
            raise ValueError(
                f"Column {self.params.target_column} not found in input_df"
            )
        # check that col is numeric
        if not pd.api.types.is_numeric_dtype(input_df[self.params.target_column]):
            raise ValueError(f"Column {self.params.target_column} is not numeric")

    @override
    def run(self, input_df: pd.DataFrame):
        """Run the block and return the result"""
        # Validate the input data
        self.validate(input_df=input_df)
        # Run the block
        result_df = input_df.copy()
        result_df[self.params.target_column] += self.params.n
        return result_df


############################################################################################
# MultiplyByNBlock
############################################################################################


class MultiplyBYNBlockParams(BlockParamBase):
    n: int
    target_column: str


class MultiplyByNBlock(BlockBase):
    """Simple block to multiply a column by a number."""

    params: MultiplyBYNBlockParams

    @override
    def validate(self, input_df: pd.DataFrame) -> None:
        """Validate that the input dataframe is not empty and that the target column exists and is numeric."""
        if input_df.empty:
            raise ValueError("Input dataframe must not be empty")
        # check that col exists
        if self.params.target_column not in input_df.columns:
            raise ValueError(
                f"Column {self.params.target_column} not found in input_df"
            )
        # check that col is numeric
        if not pd.api.types.is_numeric_dtype(input_df[self.params.target_column]):
            raise ValueError(f"Column {self.params.target_column} is not numeric")

    @override
    def run(self, input_df: pd.DataFrame):
        """Run the block and return the result"""
        # Validate the input data
        self.validate(input_df=input_df)
        # Run the block
        result_df = input_df.copy()
        result_df[self.params.target_column] *= self.params.n
        return result_df


@app.command()
def example(
    verbose: bool = False,
):
    """Run a simple example of a computation workflow."""
    # Initialize logging and load the data
    init_logging(level="DEBUG" if verbose else "INFO")
    print("Starting the computation sequence.")

    test_data = pd.DataFrame(
        {"ColumnA": list(range(10)), "ColumnB": list(range(10, 20))}
    )
    print(f"Running computation on data:")
    print(f"{test_data.head(10)}")

    sequential_runner = SequentialRunner(
        block_map={
            # Prepare the data
            1: PrepareBlock(),
            # Run the AddNBlock in parallel
            2: ParallelRunner(
                block=AddNBlock(
                    params=AddNBlockParams(n=5, target_column="column_a", num_retries=3)
                ),
                num_chunks=5,
                use_thread_pool=True,
            ),
            # Run the MultiplyByNBlock in parallel
            3: ParallelRunner(
                block=MultiplyByNBlock(
                    params=MultiplyBYNBlockParams(
                        n=2, target_column="column_a", num_retries=3
                    )
                ),
                num_chunks=5,
                use_thread_pool=True,
            ),
            # Run the AverageBlock sequentially by itself (just to show how it works)
            4: SequentialRunner(
                block_map={
                    1: AverageBlock(
                        params=AverageBlockParams(
                            column_mapping={"column_a": "column_a_avg"}
                        )
                    )
                }
            ),
        }
    )

    # Run the computation
    start_time = time.time()
    result = sequential_runner(test_data)
    duration = time.time() - start_time
    duration = round(duration, 6)

    # Cleanup before viewing
    result = result.sort_values(by="column_a")
    result = result.reset_index(drop=True)

    print(f"\nCompleted all computations in {duration} seconds.")
    print(f"Cols: {list(result.columns)}")
    print(f"Head:\n{result.head(10)}")


if __name__ == "__main__":
    app()
