# Comp Blocks

## Description

Framework for organizing and running computation blocks on an input dataframe. 
The two main components here are the SequentialRunner and ParallelRunner classes.
These two classes perform the same function, but the ParallelRunner class is able to run the computation blocks in parallel, while the SequentialRunner class runs the computation blocks in sequence.

## Installation

Poetry is used to manage dependencies. To install the dependencies, run the following commands:
```bash
% poetry install 
% poetry update
```

## Usage

The point of this example is to show how the framework can be used to organize and run computation blocks on an input dataframe. The example below shows how to create a sequential runner that performs the following operations, opting to do some in parallel where possible:
```python
import pandas as pd
from src.blocks.average.average import AverageBlock, AverageBlockParams
from src.blocks.block_base import BlockBase
from src.blocks.prepare.prepare import PrepareBlock
from src.runners.parallel_runner import ParallelRunner
from src.runners.sequential_runner import SequentialRunner

# Define test data
TEST_DATA = pd.DataFrame(
    {
        "ColumnA": list(range(10)), 
        "ColumnB": list(range(10, 20))
    }
)


# Setup for test blocks
class AddFiveBlock(BlockBase):
    """ Dummy block that adds 5 to all values in ColumnA """
    def __call__(self, input_df: pd.DataFrame):
        # Adds 5 to all values in ColumnA
        result_df = input_df.copy()
        result_df["column_a"] += 5
        return result_df


class MultiplyTwoBlock(BlockBase):
    """ Dummy block that multiplies all values in ColumnB by 2 """
    def __call__(self, input_df: pd.DataFrame):
        # Multiplies all values in ColumnB by 2
        result_df = input_df.copy()
        result_df["column_b"] *= 2
        return result_df

def sample():
    """ Sample function to display the usage of the framework. """
    # Create a sequential runner to perform some operations
    sequential_runner = SequentialRunner(
        block_map={
            # Create id column, convert ColumnA to column_a, ColumnB to column_b
            0: PrepareBlock(),
            # Add 5 to ColumnA in parallel
            2: ParallelRunner(
                block=AddFiveBlock(),
                chunk_size=5,  # Splitting the data into two chunks
                use_thread_pool=True,
            ),
            # Multiply ColumnB by 2 in parallel
            3: ParallelRunner(
                block=MultiplyTwoBlock(),
                chunk_size=5,  # Splitting the data into two chunks
                use_thread_pool=True,
            ),
            4: SequentialRunner(block_map={
                1: AverageBlock(
                    params=AverageBlockParams(column_mapping={"column_a": "column_a_avg"})
                )
            }),
        }
    )
    return sequential_runner(TEST_DATA)

# Run sample and preview the result
result = sample()
print(f"Cols: {result.columns}")
""" Output:
Cols: Index(['id', 'column_a', 'column_b', 'column_a_avg'], dtype='object')
"""
print(f"Head: {result.head(10)}")
""" Output:
   column_a  column_b                                                 id
0         5        20  73d9d92cd0c229d2df3fb4db4c0eed7ab0977b05a7d102...
1         6        22  9bc4cd485fe277f9c2c16a9bc1cfea81db2a3ed294cc4b...
2         7        24  d362ce1155cb0aedb2a9fa3203cb614326ce2cec98bb82...
3         8        26  6f322ab92247df40b028834dd641302b08134a4afad791...
4         9        28  8fb0ab3bf31047c1d3fe24eb7fd15e13a7d1043939f60e...
5        10        30  89512ae1404ffa1a8ad68993f8a1fb2f2d243814c6ff23...
6        11        32  fb88400d2b8ff1c86694ab22d884309ae39f3da8ef26a3...
7        12        34  c1cf06a78a87c1ab962a7c8df3c13385c8205c3f751179...
8        13        36  554e301e92851d827f2705991e7e377687223ec1c9f653...
9        14        38  bc24c3a5ce2dbf73d9c548ca238b54efd2e8ecea655fcd...
"""
```
