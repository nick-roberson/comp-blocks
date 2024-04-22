# Third Party Imports
import pandas as pd
import pytest

# My Imports
from src.block_base import BlockBase
from src.runners.sequential_runner import SequentialRunner

# Define test data
TEST_DATA = pd.DataFrame({"ColumnA": list(range(10)), "ColumnB": list(range(10, 20))})


# Setup for test
class IncrementBlock(BlockBase):
    def __call__(self, input_df: pd.DataFrame):
        # A simple transformation: incrementing all values in ColumnA
        result_df = input_df.copy()
        result_df["ColumnA"] += 1
        return result_df


class DoubleBlock(BlockBase):
    def __call__(self, input_df: pd.DataFrame):
        # Another simple transformation: doubling all values in ColumnB
        result_df = input_df.copy()
        result_df["ColumnB"] *= 2
        return result_df


# Initialize blocks for use in tests
INCREMENT_BLOCK = IncrementBlock()
DOUBLE_BLOCK = DoubleBlock()

####################################################################################################
# The following tests are for the SequentialRunner class                                           #
####################################################################################################


def test_sequential_runner_basic():
    # Setup sequential runner with two blocks
    sequential_runner = SequentialRunner(
        block_map={1: INCREMENT_BLOCK, 2: DOUBLE_BLOCK}
    )

    result = sequential_runner.run(TEST_DATA)
    expected_result = TEST_DATA.copy()
    expected_result["ColumnA"] += 1
    expected_result["ColumnB"] *= 2

    # Check that results are as expected
    assert result.equals(
        expected_result
    ), "The result should match the expected modified DataFrame after sequential transformations"


def test_sequential_runner_order():
    # Setup sequential runner with blocks in reverse order
    sequential_runner = SequentialRunner(
        block_map={1: DOUBLE_BLOCK, 2: INCREMENT_BLOCK}
    )

    result = sequential_runner.run(TEST_DATA)
    expected_result = TEST_DATA.copy()
    expected_result["ColumnB"] *= 2
    expected_result["ColumnA"] += 1

    # Ensure the order of operations was followed
    assert result.equals(
        expected_result
    ), "The result should reflect the order of block execution"


def test_sequential_runner_single_block():
    # Test with only one block in the map
    sequential_runner = SequentialRunner(block_map={1: INCREMENT_BLOCK})

    result = sequential_runner.run(TEST_DATA)
    expected_result = TEST_DATA.copy()
    expected_result["ColumnA"] += 1

    # Check if only one transformation was applied
    assert result.equals(
        expected_result
    ), "The result should only have the increment transformation applied"


if __name__ == "__main__":
    pytest.main([__file__])
