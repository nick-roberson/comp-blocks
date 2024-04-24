import logging
from typing import Dict

import pandas as pd

from src.block_base import BlockBase
from src.utils.wrapper import log_run_info


class SequentialRunner(BlockBase):

    block_map: Dict[int, BlockBase]

    def validate(self, input_df: pd.DataFrame) -> None:
        """Override the validate method to add additional validation."""
        pass

    @log_run_info
    def run(self, input_df: pd.DataFrame) -> pd.DataFrame:
        """Run the blocks that the runner was initialized with in order
        from the first block to the last block. Passing the result of the
        previous block to the next block."""
        result = input_df

        # Sort the blocks by order
        ordered_blocks = sorted(self.block_map.items(), key=lambda x: x[0])

        # Run in order
        for order, block in ordered_blocks:
            logging.debug(f"Running block {block.__class__} with order {order}")
            result = block(result)
            logging.debug(f"Completed block {block.__class__} with order {order}")

        # Return the result
        return result
