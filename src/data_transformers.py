import io
import pandas as pd
from pyspark.sql import SparkSession, DataFrame as SparkDataFrame
from typing import Optional

class DataTransformers:
    def __init__(
                  self, response: bytes, 
                  spark: SparkSession, 
                  df_spark: Optional[SparkDataFrame] = None,
                  select_list: Optional[list] = None) -> None:
        self.response = response
        self.spark = spark
        self._df: Optional[pd.DataFrame] = None
        self.df_spark = df_spark

    def _bytes_buffer(self) -> io.BytesIO:
        return io.BytesIO(self.response)

    def _create_dataframe_pandas(self) -> None:
        buffer = self._bytes_buffer()
        self._df = pd.read_parquet(buffer)

    def create_dataframe_spark(self) -> SparkDataFrame:
        self._create_dataframe_pandas()
        self.df_spark = self.spark.createDataFrame(self._df)
        return self.df_spark