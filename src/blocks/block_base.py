import pandas as pd
from pydantic import BaseModel


class BlockBase(BaseModel):
    """Base class for all blocks, which are the building blocks of the pipeline."""

    def __call__(self, input_df: pd.DataFrame) -> pd.DataFrame:
        """Call the block and return the result"""
        raise NotImplementedError

    def validate(self, input_df: pd.DataFrame) -> None:
        """Validate that all the required parameters are present."""
        raise NotImplementedError

    def run(self, input_df: pd.DataFrame) -> pd.DataFrame:
        """Run the block and return the result"""
        raise NotImplementedError
