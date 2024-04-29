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
from src.utils.wrapper import log_run_info

app = typer.Typer()


def init_logging():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )


class AddNBlockParams(BlockParamBase):
    n: int


class AddNBlock(BlockBase):
    params: AddNBlockParams

    @override
    def run(self, input_df: pd.DataFrame):
        result_df = input_df.copy()
        result_df["column_b"] += self.params.n
        return result_df


class MultiplyBYNBlockParams(BlockParamBase):
    n: int


class MultiplyByNBlock(BlockBase):
    params: MultiplyBYNBlockParams

    @override
    def run(self, input_df: pd.DataFrame):
        result_df = input_df.copy()
        result_df["column_b"] *= self.params.n
        return result_df


@app.command()
def example():
    """Run a simple example of a computation workflow."""
    init_logging()
    print("Starting the computation sequence.")

    test_data = pd.DataFrame(
        {"ColumnA": list(range(10)), "ColumnB": list(range(10, 20))}
    )
    print(f"Running computation on data:")
    print(f"{test_data.head(10)}")

    sequential_runner = SequentialRunner(
        block_map={
            1: PrepareBlock(),
            2: ParallelRunner(
                block=AddNBlock(params=AddNBlockParams(n=5)),
                num_chunks=5,
                use_thread_pool=True,
            ),
            3: ParallelRunner(
                block=MultiplyByNBlock(params=MultiplyBYNBlockParams(n=2)),
                num_chunks=5,
                use_thread_pool=True,
            ),
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
