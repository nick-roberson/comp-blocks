import hashlib

import numpy as np
import pandas as pd
from rich import print
from typing_extensions import override

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


def haversine_distance(df, lat1, long1, lat2, long2):
    """
    Calculates the haversine distance between 2 sets of GPS coordinates in df
    """
    r = 6371  # average radius of Earth in kilometers

    phi1 = np.radians(df[lat1])
    phi2 = np.radians(df[lat2])

    delta_phi = np.radians(df[lat2] - df[lat1])
    delta_lambda = np.radians(df[long2] - df[long1])

    a = (
        np.sin(delta_phi / 2) ** 2
        + np.cos(phi1) * np.cos(phi2) * np.sin(delta_lambda / 2) ** 2
    )
    c = 2 * np.arctan2(np.sqrt(a), np.sqrt(1 - a))
    d = r * c  # in kilometers

    return d


class PrepareTaxiBlockParams(BlockParamBase):
    """Parameters for the PrepareBlock."""

    id_col: str = "id"


class PrepareTaxiBlock(BlockBase):
    """Prepare block to clean and prepare the input data."""

    params: PrepareTaxiBlockParams = PrepareTaxiBlockParams()

    @override
    def validate(self, input_df: pd.DataFrame) -> None:
        """Validate that the input dataframe is not empty"""
        if input_df.empty:
            raise ValueError("Input dataframe must not be empty")

    def _prepare_taxi_data(self, input_df: pd.DataFrame) -> pd.DataFrame:
        """Prepare the taxi data for analysis"""
        # Convert the pickup_datetime column to a datetime object
        input_df["edt_date"] = pd.to_datetime(
            input_df["pickup_datetime"].str[:19]
        ) - pd.Timedelta(hours=4)

        # Extract useful features from the datetime object
        input_df["hour"] = input_df["edt_date"].dt.hour

        # Granular break down of "morning" "midday" "afternoon" "evening" "night"
        input_df["time_of_day"] = pd.cut(
            input_df["hour"],
            bins=[0, 6, 12, 18, 24],
            labels=["night", "morning", "afternoon", "evening"],
            right=False,
        )
        input_df["am_or_pm"] = np.where(input_df["hour"] < 12, "am", "pm")
        input_df.loc[:, "am_or_pm"] = np.where(input_df["hour"] < 12, "am", "pm")
        input_df["weekday"] = input_df["edt_date"].dt.strftime("%a")

        # Calculate the haversine distance between the pickup and dropoff locations
        input_df["dist_km"] = haversine_distance(
            input_df,
            "pickup_latitude",
            "pickup_longitude",
            "dropoff_latitude",
            "dropoff_longitude",
        )
        return input_df

    def standard_prepare(self, input_df: pd.DataFrame) -> pd.DataFrame:
        # If ID_COL does not exist, add it
        if self.params.id_col not in input_df.columns:
            input_df[self.params.id_col] = input_df.apply(hash_row_values, axis=1)

        # Delete duplicate rows
        input_df.drop_duplicates(inplace=True)

        # Reorder cols to put ID_COL first
        cols = input_df.columns.tolist()
        cols.remove(self.params.id_col)
        input_df = input_df[[self.params.id_col] + cols]
        return input_df

    @override
    def run(self, input_df: pd.DataFrame) -> pd.DataFrame:
        """Run the block and return the result"""
        # Validate the input dataframe
        input_df = self.standard_prepare(input_df)

        # Prepare the taxi data
        input_df = self._prepare_taxi_data(input_df)

        # Convert column names to snake_case and hash each row
        input_df = convert_columns_to_snake_case(input_df.copy())

        return input_df
