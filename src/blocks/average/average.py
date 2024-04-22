# Third Party Imports
import time
from typing import Dict

import pandas as pd
from pydantic import field_validator
from typing_extensions import override

# My Imports
from src.block_base import BlockBase
from src.params_base import BlockParamBase


class AverageBlockParams(BlockParamBase):

    column_mapping: Dict[str, str]

    @field_validator("column_mapping")
    def validate_columns(cls, value: Dict[str, str]) -> Dict[str, str]:
        """Validate that the columns are valid"""
        if not isinstance(value, dict):
            raise ValueError("columns must be a list")
        if not all([isinstance(col, str) for col in value]):
            raise ValueError("columns must be a list of strings")
        if not all([isinstance(avg_col, str) for avg_col in value.values()]):
            raise ValueError("columns must be a list of strings")
        if len(value.keys()) == 0:
            raise ValueError("columns must be non-empty")
        return value


class AverageBlock(BlockBase):

    params: AverageBlockParams

    @override
    def validate(self, input_df: pd.DataFrame) -> None:
        """Validate that all the required parameters are present.
        and that the columns specified are present in the dataframe
        """
        col_keys = list(self.params.column_mapping.keys())
        col_types = [input_df[col].dtype for col in col_keys]

        # General Validation
        if not isinstance(self.params, AverageBlockParams):
            raise ValueError("params must be of type AverageBlockParams")
        if not isinstance(self.params.column_mapping, Dict):
            raise ValueError("columns must be a Dict")
        if len(col_keys) == 0:
            raise ValueError("columns must be non-empty")

        # Input Validation
        if not all([col in input_df.columns for col in col_keys]):
            raise ValueError("columns must be present in the dataframe")
        if not all([col_type in [int, float] for col_type in col_types]):
            raise ValueError(f"columns must be of type int / float, were {col_types}")

    @override
    def run(self, input_df: pd.DataFrame) -> pd.DataFrame:
        """Run the block and return the result"""
        # Average each column and output a new dataframe with N new columns
        # where N is the number of columns in the input dataframe
        return input_df.assign(
            **{
                avg_col: input_df[original_col].mean()
                for original_col, avg_col in self.params.column_mapping.items()
            }
        )
