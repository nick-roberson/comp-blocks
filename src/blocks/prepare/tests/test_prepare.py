# Third Party Imports
import hashlib

import pandas as pd
import pytest

# My Imports
from src.blocks.prepare.prepare import PrepareBlock
from src.runners.block_runner import BlockRunner
from src.runners.parallel_runner import ParallelRunner
from src.runners.sequential_runner import SequentialRunner

# Define test data
TEST_DATA: pd.DataFrame = pd.DataFrame(
    {
        "ColumnA": [1, 2, 3],
        "ColumnB": [4, 5, 6],
        "ColumnC": [7, 8, 9],
    }
)

# Expected output
TEST_RESULT: pd.DataFrame = TEST_DATA.copy()
TEST_RESULT.columns = ["column_a", "column_b", "column_c"]
TEST_RESULT["id"] = TEST_RESULT.apply(
    lambda row: hashlib.sha256("".join(str(row.values)).encode()).hexdigest(), axis=1
)

####################################################################################################
# The following tests are for the PrepareBlock class                                                 #
####################################################################################################


def test_run_alone():
    # create block
    block = PrepareBlock()
    # run block
    result = block(TEST_DATA)
    # check result
    # Note: Test for snake_case conversion and presence of 'id' column
    assert all(
        col in result.columns for col in ["column_a", "column_b", "column_c", "id"]
    )
    assert result["id"].is_unique


def test_using_runner():
    # create block and runner
    block = PrepareBlock()
    runner = BlockRunner(block=block)
    # run block
    result = runner.run(TEST_DATA)
    # check result
    assert all(
        col in result.columns for col in ["column_a", "column_b", "column_c", "id"]
    )
    assert result["id"].is_unique


####################################################################################################
# The following tests are for the ParallelRunner and SequentialRunner classes                      #
####################################################################################################


def test_run_sequential():
    # create blocks
    block = PrepareBlock()

    # create sequential runner
    sequential_runner = SequentialRunner(block_map={1: block})

    # run blocks
    result = sequential_runner(TEST_DATA)

    # check result
    assert all(
        col in result.columns for col in ["column_a", "column_b", "column_c", "id"]
    )
    assert result["id"].is_unique


@pytest.mark.parametrize(
    "chunk_size, use_process_pool, use_thread_pool",
    [
        (1, False, True),
        (1, True, False),
        (2, False, True),
        (2, True, False),
        (3, False, True),
        (3, True, False),
    ],
)
def test_run_parallel_chunk_size(chunk_size, use_process_pool, use_thread_pool):
    # create blocks
    block = PrepareBlock()

    # create parallel runner
    parallel_runner = ParallelRunner(
        block=block,
        chunk_size=chunk_size,
        use_process_pool=use_process_pool,
        use_thread_pool=use_thread_pool,
    )

    # run blocks
    result = parallel_runner(TEST_DATA)

    # check result len and general structure
    assert not result.empty
    assert len(result) == 3
    assert all(
        col in result.columns for col in ["column_a", "column_b", "column_c", "id"]
    )
    assert result["id"].is_unique


if __name__ == "__main__":
    pytest.main([__file__])
