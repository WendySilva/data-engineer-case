import io
import pandas as pd
from pyspark.sql import SparkSession, DataFrame as SparkDataFrame
from typing import Optional

class DataRequests:
    def __init__(
                  self, response: bytes, 
                  spark: SparkSession, 
                  df_spark: Optional[SparkDataFrame] = None,
                  select_list: Optional[list] = None) -> None:
        self.response = response
        self.spark = spark
        self._df: Optional[pd.DataFrame] = None
        self.df_spark = df_spark
        self.select_list = select_list

    def _bytes_buffer(self) -> io.BytesIO:
        return io.BytesIO(self.response)

    def _create_dataframe_pandas(self) -> None:
        buffer = self._bytes_buffer()
        self._df = pd.read_parquet(buffer)

    def create_dataframe_spark(self) -> SparkDataFrame:
        self._create_dataframe_pandas()
        self.df_spark = self.spark.createDataFrame(self._df)
        return self.df_spark

    def select_dataframe_spark(self) -> SparkDataFrame:
        if self.df_spark is None:
            raise ValueError("O DataFrame Spark ainda não foi criado.")
        if self.select_list is None:
            raise ValueError("Nenhuma lista de colunas foi definida para seleção.")
        self.df_spark = self.df_spark.select(self.select_list)
        return self.df_spark