# Third Party Imports

import pandas as pd
import pytest

# My Imports
from src.block_base import BlockBase
from src.runners.parallel_runner import ParallelRunner

# Define test data
TEST_DATA = pd.DataFrame({"ColumnA": list(range(10)), "ColumnB": list(range(10, 20))})


# Setup for test
class DummyBlock(BlockBase):
    def __call__(self, input_df: pd.DataFrame):
        # A simple transformation, for example, adding a constant to a column
        result_df = input_df.copy()
        result_df["ColumnA"] += 1
        return result_df


# Initialize a dummy block for use in tests
DUMMY_BLOCK = DummyBlock()


####################################################################################################
# The following tests are for the ParallelRunner class                                             #
####################################################################################################


@pytest.mark.parametrize(
    "use_process_pool, use_thread_pool", [(True, False), (False, True)]
)
def test_parallel_runner_basic(use_process_pool, use_thread_pool):
    # Test basic parallel execution with different pool types
    block_runner = ParallelRunner(
        block=DUMMY_BLOCK,
        chunk_size=2,
        use_process_pool=use_process_pool,
        use_thread_pool=use_thread_pool,
    )

    # Run the block, get the result, order by ColumnA and reset the index
    result = block_runner(TEST_DATA)
    result = result.sort_values(by="ColumnA").reset_index(drop=True)

    # Create the expected result
    expected_result = TEST_DATA.copy()
    expected_result["ColumnA"] += 1
    expected_result = expected_result.sort_values(by="ColumnA").reset_index(drop=True)

    # Check that results are as expected
    print(result)
    print(expected_result)
    assert result.equals(
        expected_result
    ), "The result should match the expected modified DataFrame"


def test_split_function():
    # Test the split function with a specific chunk size
    block_runner = ParallelRunner(block=DUMMY_BLOCK, chunk_size=3, use_thread_pool=True)

    chunks = block_runner.split(TEST_DATA)
    assert len(chunks) == 4, "There should be 4 chunks for 10 items with chunk size 3"


def test_merge_function():
    # Test the merge function with independent dataframes
    block_runner = ParallelRunner(block=DUMMY_BLOCK, chunk_size=2, use_thread_pool=True)
    df1 = pd.DataFrame({"ColumnA": [1, 2], "ColumnB": [11, 12]})
    df2 = pd.DataFrame({"ColumnA": [3, 4], "ColumnB": [13, 14]})

    result = block_runner.merge([df1, df2])
    expected_result = pd.concat([df1, df2])

    assert result.equals(
        expected_result
    ), "The merged result should be equal to the concatenation of the two dataframes"


@pytest.mark.parametrize("num_chunks, expected_num_chunks", [(2, 2), (3, 3)])
def test_split_with_num_chunks(num_chunks, expected_num_chunks):
    # Test the split function with num_chunks parameter
    block_runner = ParallelRunner(
        block=DUMMY_BLOCK, num_chunks=num_chunks, use_thread_pool=True
    )

    chunks = block_runner.split(TEST_DATA)
    assert (
        len(chunks) == expected_num_chunks
    ), f"There should be {expected_num_chunks} chunks"


def test_runner_validation():
    # Test that the validation raises errors appropriately
    block_runner = ParallelRunner(
        block=DUMMY_BLOCK, use_process_pool=False, use_thread_pool=False
    )
    with pytest.raises(ValueError):
        block_runner.validate_runner()

    block_runner.use_process_pool = True
    block_runner.use_thread_pool = True
    with pytest.raises(ValueError):
        block_runner.validate_runner()

    block_runner.use_thread_pool = False
    block_runner.num_chunks = 0
    with pytest.raises(ValueError):
        block_runner.validate_runner()


if __name__ == "__main__":
    pytest.main([__file__])
