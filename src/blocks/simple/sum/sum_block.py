from typing import Dict

import pandas as pd
from pydantic import field_validator
from typing_extensions import override

from src.block_base import BlockBase
from src.params_base import BlockParamBase


class SumBlockParams(BlockParamBase):
    column_mapping: Dict[str, str]

    @field_validator("column_mapping")
    def validate_columns(cls, value: Dict[str, str]) -> Dict[str, str]:
        """Validate that the columns are valid"""
        if not isinstance(value, dict):
            raise ValueError("column_mapping must be a dict")
        if not all(
            isinstance(col, str) and isinstance(new_col, str)
            for col, new_col in value.items()
        ):
            raise ValueError("column_mapping must be a dict of strings to strings")
        if len(value) == 0:
            raise ValueError("column_mapping must be non-empty")
        return value


class SumBlock(BlockBase):
    params: SumBlockParams

    @override
    def validate(self, input_df: pd.DataFrame) -> None:
        """Validate that all required parameters are present and columns specified are in the dataframe"""
        col_keys = list(self.params.column_mapping.keys())
        col_types = [input_df[col].dtype for col in col_keys]

        if not isinstance(self.params, SumBlockParams):
            raise ValueError("params must be of type SumBlockParams")
        if len(col_keys) == 0:
            raise ValueError("column_mapping must be non-empty")

        if not all(col in input_df.columns for col in col_keys):
            raise ValueError("All columns must be present in the dataframe")
        if not all(col_type in [int, float] for col_type in col_types):
            raise ValueError(
                f"All columns must be of type int or float, were {col_types}"
            )

    @override
    def run(self, input_df: pd.DataFrame) -> pd.DataFrame:
        """Run the block and return the result by summing up specified columns"""
        return input_df.assign(
            **{
                new_col: input_df[original_col].sum()
                for original_col, new_col in self.params.column_mapping.items()
            }
        )
