# Third Party Imports
import hashlib

import pandas as pd
from pydantic import BaseModel
from typing_extensions import override

# My Imports
from src.blocks.block_base import BlockBase


def to_snake_case(s: str) -> str:
    """Converts a string to snake_case."""
    return "".join(["_" + c.lower() if c.isupper() else c for c in s]).lstrip("_")


def hash_row_values(row: pd.Series) -> str:
    """Hashes the values of a DataFrame row using SHA-256 and returns the hash as a hex string."""
    return hashlib.sha256("".join(str(row.values)).encode()).hexdigest()


def convert_columns_to_snake_case(df: pd.DataFrame) -> pd.DataFrame:
    """Converts all column names of a DataFrame to snake_case."""
    df.columns = [to_snake_case(col) for col in df.columns]
    return df


class PrepareBlock(BlockBase):

    ID_COL: str = "id"

    def __call__(self, input_df: pd.DataFrame) -> pd.DataFrame:
        """Call the block and return the result"""
        self.validate(input_df=input_df)
        return self.run(input_df=input_df)

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
        if self.ID_COL not in output_df.columns:
            output_df[self.ID_COL] = output_df.apply(hash_row_values, axis=1)

        # Delete duplicate rows
        output_df.drop_duplicates(inplace=True)

        return output_df
