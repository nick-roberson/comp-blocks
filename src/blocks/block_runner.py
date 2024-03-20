import pandas as pd
from pydantic import BaseModel

# My Imports
from src.blocks.block_base import BlockBase


class BlockRunner(BaseModel):

    block: BlockBase

    def run(self, input_df: pd.DataFrame) -> pd.DataFrame:
        """Call the block and return the result"""
        print(f"Running block {self.block.__class__}")
        result = self.block(input_df)
        print(f"Completed block {self.block.__class__}")
        return result
