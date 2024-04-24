import datetime
import logging

import pandas as pd

logger = logging.getLogger(__name__)


def log_run_info(func):
    """Decorator to log information about the run function."""

    def wrapper(self, input_df: pd.DataFrame) -> pd.DataFrame:
        """Wrapper function to log information about the run function.

        Args:
            input_df: The input DataFrame.
        Returns:
            result_df: The resulting DataFrame.
        """
        # Log the start of the run
        block_name = self.__class__.__name__
        start_time = datetime.datetime.now()
        logger.info(
            f"Starting {block_name} at {start_time} with input shape {input_df.shape}"
        )

        # Call the original function
        result_df = func(self, input_df)

        # Log the run information
        end_time = datetime.datetime.now()
        logger.info(
            f"Finished {block_name} at {end_time} in {end_time - start_time} with output shape {result_df.shape}"
        )
        return result_df

    return wrapper
