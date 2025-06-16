from pyspark.sql import SparkSession, DataFrame as SparkDataFrame
import pyspark.sql.functions as F
from typing import Optional, List
import logging


class DataTransformers:
    """
    Classe responsável por transformar dados brutos em DataFrames do Spark.

    Atributos
    ---------
    logger : logging.Logger
        Logger utilizado para registrar mensagens e erros.

    spark : SparkSession, opcional
        Sessão Spark ativa, necessária para leitura de dados do Parquet.

    path : str, opcional
        Caminho do arquivo Parquet a ser lido.

    partition_column : str, opcional
        Nome da coluna de origem para criar a coluna de partição no formato 'yyyy-MM'.
        Deve ser usada em conjunto com `partition_name`.

    partition_name : str, opcional
        Nome da nova coluna de partição a ser criada.
        Deve ser usada em conjunto com `partition_column`.

    df_spark : SparkDataFrame, opcional
        DataFrame já existente que será transformado.

    column_select : list[str], opcional
        Lista de colunas que devem ser mantidas no DataFrame final.

    filter_column : str, opcional
        Nome da coluna que será usada para aplicar filtro.

    filter_list : list[str], opcional
        Lista de valores permitidos no filtro aplicado à `filter_column`.

    Notas
    -----
    - `partition_column` e `partition_name` devem ser informados **juntos**, caso contrário será levantado um erro.
    - `filter_column` e `filter_list` também devem ser usados **em conjunto** para o filtro funcionar corretamente.
    """

    def __init__(
        self,
        logger: logging.Logger,
        spark: Optional[SparkSession] = None,
        path: Optional[str] = None,
        partition_column: Optional[str] = None,
        partition_name: Optional[str] = None,
        df_spark: Optional[SparkDataFrame] = None,
        column_select: Optional[List[str]] = None,
        filter_column: Optional[str] = None,
        filter_list: Optional[list[str]] = None,
    ) -> None:
        self.logger = logger
        self.spark = spark
        self.path = path
        self.partition_column = partition_column
        self.partition_name = partition_name
        self.df_spark = df_spark
        self.column_select = column_select
        self.filter_column = filter_column
        self.filter_list = filter_list

    @staticmethod
    def format_columns(df: SparkDataFrame) -> SparkDataFrame:
        rename_map = {
            "Airport_fee": "airport_fee",
            "VendorID": "vendor_id",
            "RatecodeID": "ratecode_id",
            "PULocationID": "pu_location_id",
            "DOLocationID": "do_location_id",
        }

        cast_map = {
            "vendor_id": "bigint",
            "tpep_pickup_datetime": "timestamp",
            "tpep_dropoff_datetime": "timestamp",
            "passenger_count": "double",
            "trip_distance": "double",
            "ratecode_id": "double",
            "store_and_fwd_flag": "string",
            "pu_location_id": "bigint",
            "do_location_id": "bigint",
            "payment_type": "bigint",
            "fare_amount": "double",
            "extra": "double",
            "mta_tax": "double",
            "tip_amount": "double",
            "tolls_amount": "double",
            "improvement_surcharge": "double",
            "total_amount": "double",
            "congestion_surcharge": "double",
            "airport_fee": "double",
        }

        for wrong_name, correct_name in rename_map.items():
            if wrong_name in df.columns:
                df = df.withColumnRenamed(wrong_name, correct_name)

        for col, dtype in cast_map.items():
            if col in df.columns:
                df = df.withColumn(col, F.col(col).cast(dtype))
        return df

    def _create_column_partition(self):
        if self.partition_column and self.partition_name:
            if self.partition_name not in self.df_spark.columns:
                self.logger.info(f"Criando coluna de partição: {self.partition_name}")
                self.df_spark = self.df_spark.withColumn(
                    self.partition_name,
                    F.date_format(F.col(self.partition_column), "yyyy-MM"),
                )

    def create_dataframe_spark(self) -> SparkDataFrame:
        if not self.partition_column or not self.partition_name:
            self.logger.error("A coluna de partição não foi informada.")
            raise ValueError("Informe a coluna de partição para continuar.")

        try:
            self.df_spark = self.spark.read.parquet(self.path)
            self._create_column_partition()
            return self.df_spark
        except Exception as e:
            self.logger.error(f"Erro ao criar o DataFrame Spark: {e}")
            raise

    def filter_dataframe_spark(self) -> SparkDataFrame:
        if not self.filter_column or not self.filter_list:
            self.logger.error("A lista de colunas para filtro não foi informada.")
            raise ValueError("Informe as colunas que deseja selecionar.")

        try:
            if self.partition_column and self.partition_name:
                self._create_column_partition()
            self.df_spark = self.df_spark.filter(
                F.col(self.filter_column).isin(self.filter_list)
            )

            return self.df_spark
        except Exception as e:
            self.logger.error(f"Erro ao selecionar colunas do DataFrame: {e}")
            raise

    def select_expr_dataframe_spark(self) -> SparkDataFrame:
        if not self.column_select:
            self.logger.error("A lista de colunas para seleção não foi informada.")
            raise ValueError("Informe as colunas que deseja selecionar.")

        try:
            if self.partition_column and self.partition_name:
                self._create_column_partition()
            self.df_spark = self.df_spark.selectExpr(*self.column_select)
            return self.df_spark
        except Exception as e:
            self.logger.error(f"Erro ao selecionar colunas do DataFrame: {e}")
            raise