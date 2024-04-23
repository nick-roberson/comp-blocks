from typing import Dict

import pandas as pd
import pytest

from src.blocks.simple.average.average_block import (AverageBlock,
                                                     AverageBlockParams)
from src.runners.block_runner import BlockRunner
from src.runners.parallel_runner import ParallelRunner
from src.runners.sequential_runner import SequentialRunner

# Define the test parameters and data
TEST_COLUMNS: Dict[str, str] = {
    "a": "a_avg",
    "b": "b_avg",
    "c": "c_avg",
}
TEST_DATA: Dict[str, list[int]] = {
    "a": [1, 2, 3],
    "b": [4, 5, 6],
    "c": [7, 8, 9],
}

# Create the parameters and data
TEST_PARAMS: AverageBlockParams = AverageBlockParams(column_mapping=TEST_COLUMNS)
TEST_DATA: pd.DataFrame = pd.DataFrame(TEST_DATA)

# Create the test result data
TEST_RESULT: pd.DataFrame = pd.DataFrame(
    {
        "a": [1, 2, 3],
        "b": [4, 5, 6],
        "c": [7, 8, 9],
        "a_avg": [2.0, 2.0, 2.0],
        "b_avg": [5.0, 5.0, 5.0],
        "c_avg": [8.0, 8.0, 8.0],
    }
)

####################################################################################################
# The following tests are for the AverageBlock class                                               #
####################################################################################################


def test_run_alone():
    # create block
    block = AverageBlock(params=TEST_PARAMS)
    # run block
    result = block(TEST_DATA)
    # check result
    assert result.equals(TEST_RESULT)


def test_using_runner():
    # create block and runner
    block = AverageBlock(params=TEST_PARAMS)
    runner = BlockRunner(block=block)
    # run block
    result = runner.run(TEST_DATA)
    # check result
    assert result.equals(TEST_RESULT)


####################################################################################################
# The following tests are for the ParallelRunner and SequentialRunner classes                      #
####################################################################################################


def test_run_sequential():
    # create sequential params
    params_block_1 = AverageBlockParams(column_mapping={"a": "a_avg"})
    params_block_2 = AverageBlockParams(column_mapping={"b": "b_avg"})
    params_block_3 = AverageBlockParams(column_mapping={"c": "c_avg"})

    # create sequential blocks
    block_1 = AverageBlock(params=params_block_1)
    block_2 = AverageBlock(params=params_block_2)
    block_3 = AverageBlock(params=params_block_3)

    # create sequential runner
    sequential_blocks = {1: block_1, 2: block_2, 3: block_3}
    sequential_runner = SequentialRunner(block_map=sequential_blocks)

    # run blocks
    result = sequential_runner(TEST_DATA)

    # check result
    assert result.equals(TEST_RESULT)


@pytest.mark.parametrize(
    "chunk_size, expected_result, use_process_pool, use_thread_pool",
    [
        (1, TEST_RESULT, False, True),
        (1, TEST_RESULT, True, False),
        (2, TEST_RESULT, False, True),
        (2, TEST_RESULT, True, False),
        (3, TEST_RESULT, False, True),
        (3, TEST_RESULT, True, False),
    ],
)
def test_run_parallel_chunk_size(
    chunk_size, expected_result, use_process_pool, use_thread_pool
):
    # create sequential params
    params_block_1 = AverageBlockParams(column_mapping={"a": "a_avg"})

    # create sequential blocks
    block_1 = AverageBlock(params=params_block_1)

    # create sequential runner
    parallel_runner = ParallelRunner(
        block=block_1,
        chunk_size=chunk_size,
        use_process_pool=use_process_pool,
        use_thread_pool=use_thread_pool,
    )

    # run blocks
    result = parallel_runner(TEST_DATA)

    # check result len and general structure
    assert result is not None
    assert not result.empty
    assert len(result) == 3

    # check cols
    original_cols = list(TEST_DATA.columns)
    original_cols.append("a_avg")
    assert original_cols == list(result.columns)


@pytest.mark.parametrize(
    "num_chunks, expected_result, use_process_pool, use_thread_pool",
    [
        (1, TEST_RESULT, False, True),
        (1, TEST_RESULT, True, False),
        (2, TEST_RESULT, False, True),
        (2, TEST_RESULT, True, False),
        (3, TEST_RESULT, False, True),
        (3, TEST_RESULT, True, False),
    ],
)
def test_run_parallel_num_chunks(
    num_chunks, expected_result, use_process_pool, use_thread_pool
):
    # create sequential params
    params_block_1 = AverageBlockParams(column_mapping={"a": "a_avg"})

    # create sequential blocks
    block_1 = AverageBlock(params=params_block_1)

    # create sequential runner
    parallel_runner = ParallelRunner(
        block=block_1,
        num_chunks=num_chunks,
        use_process_pool=use_process_pool,
        use_thread_pool=use_thread_pool,
    )

    # run blocks
    result = parallel_runner(TEST_DATA)

    # check result len and general structure
    assert result is not None
    assert not result.empty
    assert len(result) == 3

    # check cols
    original_cols = list(TEST_DATA.columns)
    original_cols.append("a_avg")
    assert original_cols == list(result.columns)


if __name__ == "__main__":
    pytest.main([__file__])
