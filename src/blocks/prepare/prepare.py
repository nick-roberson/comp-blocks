# Third Party Imports
import hashlib
import time

import pandas as pd
from typing_extensions import override

# My Imports
from src.block_base import BlockBase
from src.params_base import BlockParamBase

HASH_LENGTH: int = 16


def to_snake_case(s: str) -> str:
    """Converts a string to snake_case."""
    return "".join(["_" + c.lower() if c.isupper() else c for c in s]).lstrip("_")


def hash_row_values(row: pd.Series) -> str:
    """Hashes the values of a DataFrame row using SHA-256 and returns the hash as a hex string."""
    hash_val = hashlib.sha256("".join(str(row.values)).encode()).hexdigest()
    return hash_val[:HASH_LENGTH]


def convert_columns_to_snake_case(df: pd.DataFrame) -> pd.DataFrame:
    """Converts all column names of a DataFrame to snake_case."""
    df.columns = [to_snake_case(col) for col in df.columns]
    return df


class PrepareBlockParams(BlockParamBase):
    id_col: str = "id"


class PrepareBlock(BlockBase):
    """Prepare block to clean and prepare the input data."""

    params: PrepareBlockParams = PrepareBlockParams()

    def __call__(self, input_df: pd.DataFrame) -> pd.DataFrame:
        """Call the block and return the result"""
        self.validate(input_df=input_df)

        # Run the block with any input data
        num_retries = self.params.attempts
        retry_delay = self.params.retry_delay
        while num_retries > 0:
            try:
                return self.run(input_df=input_df)
            except Exception as e:
                num_retries -= 1
                if num_retries == 0:
                    raise e
                time.sleep(retry_delay)

    @override
    def validate(self, input_df: pd.DataFrame) -> None:
        """Validate that the input dataframe is not empty"""
        if input_df.empty:
            raise ValueError("Input dataframe must not be empty")

    @override
    def run(self, input_df: pd.DataFrame) -> pd.DataFrame:
        """Run the block and return the result"""
        # Convert column names to snake_case and hash each row
        output_df = convert_columns_to_snake_case(input_df.copy())

        # If ID_COL does not exist, add it
        if self.params.id_col not in output_df.columns:
            output_df[self.params.id_col] = output_df.apply(hash_row_values, axis=1)

        # Delete duplicate rows
        output_df.drop_duplicates(inplace=True)

        # Reorder cols to put ID_COL first
        cols = output_df.columns.tolist()
        cols.remove(self.params.id_col)
        output_df = output_df[[self.params.id_col] + cols]

        return output_df
