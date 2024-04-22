import logging

import pandas as pd
import typer
from pydantic import BaseModel

from src.blocks.average.average import AverageBlock, AverageBlockParams
from src.blocks.block_base import BlockBase
from src.blocks.prepare.prepare import PrepareBlock
from src.runners.parallel_runner import ParallelRunner
from src.runners.sequential_runner import SequentialRunner

app = typer.Typer()


def init_logging():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )


class AddNBlockParams(BaseModel):
    n: int


class AddNBlock(BlockBase):
    params: AddNBlockParams

    def __call__(self, input_df: pd.DataFrame):
        logging.info(f"Adding {self.params.n} to all values in 'column_a'")
        result_df = input_df.copy()
        result_df["column_a"] += self.params.n
        return result_df


class MultiplyBYNBlockParams(BaseModel):
    n: int


class MultiplyByNBlock(BlockBase):
    params: MultiplyBYNBlockParams

    def __call__(self, input_df: pd.DataFrame):
        logging.info(f"Multiplying all values in 'column_b' by {self.params.n}")
        result_df = input_df.copy()
        result_df["column_b"] *= self.params.n
        return result_df


@app.command()
def example():
    """Run a simple example of a computation workflow."""
    init_logging()
    logging.info("Starting the computation sequence.")

    test_data = pd.DataFrame(
        {"ColumnA": list(range(10)), "ColumnB": list(range(10, 20))}
    )
    logging.info(f"Running computation on data:")
    logging.info(f"{test_data.head(10)}")

    sequential_runner = SequentialRunner(
        block_map={
            0: PrepareBlock(),
            2: ParallelRunner(
                block=AddNBlock(params=AddNBlockParams(n=5)),
                chunk_size=5,
                use_thread_pool=True,
            ),
            3: ParallelRunner(
                block=MultiplyByNBlock(params=MultiplyBYNBlockParams(n=2)),
                chunk_size=5,
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
    result = sequential_runner(test_data)

    # Cleanup before viewing
    result = result.sort_values(by="column_a")
    result = result.reset_index(drop=True)

    logging.info("Completed all computations.")
    print(f"Cols: {result.columns}")
    print(f"Head: {result.head(10)}")


if __name__ == "__main__":
    app()
