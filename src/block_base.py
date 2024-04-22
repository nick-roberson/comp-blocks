import time

import pandas as pd
from pydantic import BaseModel


class BlockBase(BaseModel):
    """Base class for all blocks, which are the building blocks of the pipeline."""

    def __call__(self, input_df: pd.DataFrame) -> pd.DataFrame:
        """Call the block and return the result"""
        self.validate(input_df=input_df)

        # Run the block with any input data
        num_attempts = self.params.attempts
        retry_delay = self.params.retry_delay
        while num_attempts > 0:
            try:
                return self.run(input_df=input_df)
            except Exception as e:
                num_attempts -= 1
                if num_attempts == 0:
                    raise e
                time.sleep(retry_delay)

    def validate(self, input_df: pd.DataFrame) -> None:
        """Validate that all the required parameters are present."""
        raise NotImplementedError

    def run(self, input_df: pd.DataFrame) -> pd.DataFrame:
        """Run the block and return the result"""
        raise NotImplementedError
