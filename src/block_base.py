import logging
import time
import uuid

import pandas as pd
from pydantic import BaseModel

from src.params_base import BlockParamBase
from src.utils.logging import init_logging
from src.utils.wrapper import log_run_info


class BlockBase(BaseModel):
    """Base class for all blocks, which are the building blocks of the pipeline."""

    # Unique identifier for the block
    id: str = str(uuid.uuid4())
    # Parameters for the block
    params: BlockParamBase = BlockParamBase()

    def __init__(self, **data):
        """Initialize the block with the given parameters."""
        # Assign new id to the block and call the super constructor
        data["id"] = str(uuid.uuid4())
        # Call the super constructor
        super().__init__(**data)

    @log_run_info
    def __call__(self, input_df: pd.DataFrame) -> pd.DataFrame:
        """Call the block and return the result"""
        # Initialize the logging
        init_logging(level=self.params.log_level)

        # Validate the input data
        self.validate(input_df=input_df)

        # Run the block with any input data
        num_attempts = self.params.attempts
        retry_delay = self.params.retry_delay
        while num_attempts > 0:
            try:
                result = self.run(input_df=input_df)
                return result
            except Exception as e:
                num_attempts -= 1
                if num_attempts == 0:
                    logging.info(
                        f"Failed to run block {self.__class__.__name__} with error: {e}"
                    )
                    raise e
                logging.info(
                    f"Failed to run block {self.__class__.__name__} with error: {e}. Retrying in {retry_delay} seconds."
                )
                time.sleep(retry_delay)

    def validate(self, input_df: pd.DataFrame) -> None:
        """Validate that all the required parameters are present."""
        # Simple assertion that the input_df is not None
        assert input_df is not None, "Input DataFrame is None"

    def run(self, input_df: pd.DataFrame) -> pd.DataFrame:
        """Run the block and return the result"""
        raise NotImplementedError(
            "The run method must be implemented in the derived class"
        )
