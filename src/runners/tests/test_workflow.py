# Third Party Imports
import pandas as pd
import pytest

from src.block_base import BlockBase
from src.blocks.average.average import AverageBlock, AverageBlockParams
from src.blocks.prepare.prepare import PrepareBlock
from src.runners.parallel_runner import ParallelRunner
from src.runners.sequential_runner import SequentialRunner

# Define test data
TEST_DATA = pd.DataFrame({"ColumnA": list(range(10)), "ColumnB": list(range(10, 20))})


# Setup for test blocks
class AddFiveBlock(BlockBase):
    def __call__(self, input_df: pd.DataFrame):
        # Adds 5 to all values in ColumnA
        result_df = input_df.copy()
        result_df["column_a"] += 5
        return result_df


class MultiplyTwoBlock(BlockBase):
    def __call__(self, input_df: pd.DataFrame):
        # Multiplies all values in ColumnB by 2
        result_df = input_df.copy()
        result_df["column_b"] *= 2
        return result_df


# Test combining Parallel and Sequential Runners
def test_combined_runners():
    # Parallel: Create parallel runner to add 5 to ColumnA
    add_five_to_col_a_parallel = ParallelRunner(
        block=AddFiveBlock(),
        chunk_size=5,  # Splitting the data into two chunks
        use_thread_pool=True,
    )

    # Parallel: Multiply ColumnB by 2
    multiply_col_b_by_two_parallel = ParallelRunner(
        block=MultiplyTwoBlock(),
        chunk_size=5,  # Splitting the data into two chunks
        use_thread_pool=True,
    )

    # Sequential: Create parallel runner to average ColumnA
    average_col = AverageBlock(
        params=AverageBlockParams(column_mapping={"column_a": "column_a_avg"})
    )
    average_col_a_sequential = SequentialRunner(block_map={1: average_col})

    # Now create a sequential runner to run the above parallel runners with a prepare block at the beginning
    sequential_runner = SequentialRunner(
        block_map={
            # Create id column, convert ColumnA to column_a, ColumnB to column_b
            0: PrepareBlock(),
            # Add 5 to ColumnA in parallel
            2: add_five_to_col_a_parallel,
            # Multiply ColumnB by 2 in parallel
            3: multiply_col_b_by_two_parallel,
            # Average ColumnA in parallel
            4: average_col_a_sequential,
        }
    )
    final_result = sequential_runner(TEST_DATA)

    # Expected results after both operations
    expected_result = TEST_DATA.copy()
    expected_result["ColumnA"] += 5
    expected_result["ColumnB"] *= 2
    expected_result.columns = ["column_a", "column_b"]
    expected_result["column_a_avg"] = expected_result["column_a"].mean()

    # Sort and compare
    final_result = final_result.sort_values(by="column_a").reset_index(drop=True)
    expected_result = expected_result.sort_values(by="column_a").reset_index(drop=True)

    # Drop id col from both since it will be unique across runs
    final_result = final_result.drop(columns="id")

    # Assert that the final result matches the expected result
    assert set(final_result.columns) == set(
        expected_result.columns
    ), "The final result should have the same columns as the expected result"

    # Check each col in the final result
    for col in expected_result.columns:
        assert final_result[col].equals(
            expected_result[col]
        ), f"The column {col} should match the expected result"


if __name__ == "__main__":
    pytest.main([__file__])
