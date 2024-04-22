from concurrent.futures import (ProcessPoolExecutor, ThreadPoolExecutor,
                                as_completed)
from typing import List

import pandas as pd

# My Imports
from src.block_base import BlockBase


class ParallelRunner(BlockBase):

    # The blocks to run in parallel
    block: BlockBase

    # Two different ways of running in parallel
    num_chunks: int = None
    chunk_size: int = None

    # Two different ways to parallelize
    use_process_pool: bool = False
    use_thread_pool: bool = False

    def __call__(self, *args, **kwargs):
        """Call the block and return the result"""
        return self.run(*args, **kwargs)

    @property
    def runner_name(self) -> str:
        """Return the name of the runner"""
        return self.__class__.__name__

    @property
    def block_name(self) -> str:
        """Return the name of the block"""
        return self.block.__class__.__name__

    def validate_runner(self):
        """Simple validation on params."""
        if not self.use_process_pool and not self.use_thread_pool:
            raise ValueError("Either use_process_pool or use_thread_pool must be True")
        if self.use_process_pool and self.use_thread_pool:
            raise ValueError(
                "Only one of use_process_pool or use_thread_pool must be True"
            )
        if self.num_chunks is not None and self.chunk_size is not None:
            raise ValueError("Only one of num_chunks or chunk_size must be specified")
        if self.num_chunks is None and self.chunk_size is None:
            raise ValueError("Either num_chunks or chunk_size must be specified")
        if self.num_chunks is not None and self.num_chunks <= 0:
            raise ValueError("num_chunks must be greater than 0")
        if self.chunk_size is not None and self.chunk_size <= 0:
            raise ValueError("chunk_size must be greater than 0")

    def split(self, input_df: pd.DataFrame) -> List[pd.DataFrame]:
        """Split the dataframe into chunks"""
        # Split the dataframe into num_chunks chunks
        if self.num_chunks is not None:
            chunk_size = (
                len(input_df) // self.num_chunks
            )  # Determine size of each chunk
            remainder = (
                len(input_df) % self.num_chunks
            )  # Determine if there's a remainder
            chunks = [
                input_df.iloc[i * chunk_size : (i + 1) * chunk_size]
                for i in range(self.num_chunks)
            ]
            # If there's a remainder, add the last few rows to the last chunk
            if remainder:
                chunks[-1] = pd.concat([chunks[-1], input_df.iloc[-remainder:]])

            return chunks

        # Split the dataframe into chunks of size chunk_size
        elif self.chunk_size is not None:
            return [
                input_df.iloc[i : i + self.chunk_size]
                for i in range(0, input_df.shape[0], self.chunk_size)
            ]
        else:
            raise ValueError("Either num_chunks or chunk_size must be specified")

    def merge(self, input_dfs: List[pd.DataFrame]) -> pd.DataFrame:
        """Merge the dataframes into one dataframe"""
        return pd.concat(input_dfs)

    def run_process_pool(self, chunks: List[pd.DataFrame]) -> List[pd.DataFrame]:
        # Run in parallel using ProcessPoolExecutor
        results = []
        with ProcessPoolExecutor() as executor:

            # Submit the tasks
            futures = {}
            for chunk in chunks:
                print(
                    f"Running block {self.block.__class__} with chunk\n{chunk.head(10)}"
                )
                future = executor.submit(self.block, chunk)
                futures[future] = chunk

            # Wait for the tasks to complete and aggregate the results
            for future in as_completed(futures):
                block = futures[future]
                try:
                    results.append(future.result())
                except Exception as e:
                    print(f"Block {block.__class__} failed with error: {e}")
                    raise e
        return results

    def run_thread_pool(self, chunks: List[pd.DataFrame]) -> List[pd.DataFrame]:
        # Run in parallel using ProcessPoolExecutor
        results = []
        with ThreadPoolExecutor() as executor:

            # Submit the tasks
            futures = {}
            for chunk in chunks:
                print(
                    f"Running block {self.block.__class__} with chunk\n{chunk.head(10)}"
                )
                future = executor.submit(self.block, chunk)
                futures[future] = chunk

            # Wait for the tasks to complete and aggregate the results
            for future in as_completed(futures):
                block = futures[future]
                try:
                    results.append(future.result())
                except Exception as e:
                    print(f"Block {block.__class__} failed with error: {e}")
                    raise e
        return results

    def run(self, input_df: pd.DataFrame) -> pd.DataFrame:
        """Run the blocks that the runner was initialized with in order
        from the first block to the last block. Passing the result of the
        previous block to the next block."""
        self.validate_runner()

        # Generate the chunks
        chunks = self.split(input_df)
        print(f"Split the dataframe into {len(chunks)} chunks")

        # Run in parallel
        results = []
        if self.use_process_pool:
            results = self.run_process_pool(chunks=chunks)
            print(
                f"Completed running ProcessPool block {self.runner_name} for block {self.block_name}"
                f"with {len(results)}"
            )
        elif self.use_thread_pool:
            results = self.run_thread_pool(chunks=chunks)
            print(
                f"Completed running ThreadPool block {self.runner_name} for block {self.block_name}"
                f"with {len(results)}"
            )

        # Merge the results
        for result in results:
            print(result.head(10))

        result = self.merge(results)
        print(f"Merged the results into a dataframe with shape {result.shape}")
        print(f"Head of the merged dataframe\n{result.head(10)}")

        # Return the result
        return result
