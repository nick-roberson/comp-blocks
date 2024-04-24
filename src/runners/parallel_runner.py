import logging
from concurrent.futures import (ProcessPoolExecutor, ThreadPoolExecutor,
                                as_completed)
from typing import List

import pandas as pd

from src.block_base import BlockBase
from src.utils.wrapper import log_run_info


class ParallelRunner(BlockBase):

    # The blocks to run in parallel
    block: BlockBase

    # Two different ways of running in parallel
    num_chunks: int = None
    chunk_size: int = None

    # Two different ways to parallelize
    use_process_pool: bool = False
    use_thread_pool: bool = False

    @property
    def runner_name(self) -> str:
        """Return the name of the runner"""
        return self.__class__.__name__

    @property
    def block_name(self) -> str:
        """Return the name of the block"""
        return self.block.__class__.__name__

    def validate(self, input_df: pd.DataFrame) -> None:
        """Override the validate method to add additional validation."""
        pass

    def validate_runner(self) -> None:
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
        """Split the input dataframe into chunks based on the specified parameters."""
        # If we are using num_chunks, split the dataframe into num_chunks
        if self.num_chunks is not None:
            chunk_size = len(input_df) // self.num_chunks
            return [
                input_df.iloc[i : i + chunk_size]
                for i in range(0, len(input_df), chunk_size)
            ]
        elif self.chunk_size is not None:
            return [
                input_df.iloc[i : i + self.chunk_size]
                for i in range(0, len(input_df), self.chunk_size)
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
                future = executor.submit(self.block, chunk)
                futures[future] = chunk

            # Wait for the tasks to complete and aggregate the results
            for future in as_completed(futures):
                block = futures[future]
                try:
                    results.append(future.result())
                except Exception as e:
                    logging.debug(f"Block {block.__class__} failed with error: {e}")
                    raise e
        return results

    def run_thread_pool(self, chunks: List[pd.DataFrame]) -> List[pd.DataFrame]:
        # Run in parallel using ProcessPoolExecutor
        results = []
        with ThreadPoolExecutor() as executor:

            # Submit the tasks
            futures = {}
            for chunk in chunks:
                future = executor.submit(self.block, chunk)
                futures[future] = chunk

            # Wait for the tasks to complete and aggregate the results
            for future in as_completed(futures):
                block = futures[future]
                try:
                    results.append(future.result())
                except Exception as e:
                    logging.debug(f"Block {block.__class__} failed with error: {e}")
                    raise e
        return results

    def run(self, input_df: pd.DataFrame) -> pd.DataFrame:
        """Run the blocks that the runner was initialized with in order
        from the first block to the last block. Passing the result of the
        previous block to the next block."""
        self.validate_runner()

        # Generate the chunks
        chunks = self.split(input_df)

        # Run in parallel
        results = []
        if self.use_process_pool:
            results = self.run_process_pool(chunks=chunks)
        elif self.use_thread_pool:
            results = self.run_thread_pool(chunks=chunks)

        # Merge the results
        result = self.merge(results)

        # Return the result
        return result
