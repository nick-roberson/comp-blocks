from typing import Dict

import pandas as pd
from pydantic import BaseModel

# My Imports
from src.blocks.block_base import BlockBase


class SequentialRunner(BaseModel):

    block: Dict[int, BlockBase]

    def run(self, input_df: pd.DataFrame) -> pd.DataFrame:
        """Run the blocks that the runner was initialized with in order
        from the first block to the last block. Passing the result of the
        previous block to the next block."""
        result = input_df

        # Sort the blocks by order
        ordered_blocks = sorted(self.block.items(), key=lambda x: x[0])

        # Run in order
        for order, block in ordered_blocks:
            print(f"Running block {block.__class__} with order {order}")
            result = block(result)
            print(f"Completed block {block.__class__} with order {order}")

        # Return the result
        return result
