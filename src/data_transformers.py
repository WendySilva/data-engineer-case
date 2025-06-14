import io
import pandas as pd
from pyspark.sql import SparkSession, DataFrame as SparkDataFrame
import pyspark.sql.functions as F
from typing import Optional, List
import logging


class DataTransformers:
    """
    Classe responsável por transformar dados brutos (arquivos Parquet em bytes) em DataFrames do Spark.

    Atributos
    ---------
    logger : logging.Logger
        Logger utilizado para registrar mensagens e erros durante o processamento.

    spark : SparkSession, opcional
        Sessão Spark ativa utilizada para criação de DataFrames.

    responses : list[bytes], opcional
        Lista de arquivos Parquet em formato binário que serão lidos e combinados.

    partition : str, opcional
        Nome da coluna que será usada para gerar uma coluna de partição no formato 'yyyy-MM'.

    df_spark : SparkDataFrame, opcional
        DataFrame Spark já existente (pode ser usado para reutilização).

    column_select : list[str], opcional
        Lista de colunas que devem ser selecionadas do DataFrame final.

    Métodos
    -------
    create_dataframe_spark() -> SparkDataFrame
        Lê todos os arquivos Parquet fornecidos, os concatena e transforma em um DataFrame Spark com a coluna de partição.

    select_expr_dataframe_spark() -> SparkDataFrame
        Aplica a seleção de colunas no DataFrame Spark e recria a coluna de partição, se aplicável.

    Uso típico
    ----------
    transformador = DataTransformers(
        logger=logger,
        spark=spark,
        responses=[arquivo1, arquivo2],
        partition='tpep_pickup_datetime',
        column_select=["VendorID", "passenger_count"]
    )
    df = transformador.create_dataframe_spark()
    df_selecionado = transformador.select_expr_dataframe_spark()
    """

    def __init__(
        self,
        logger: logging.Logger,
        spark: Optional[SparkSession] = None,
        responses: Optional[List[bytes]] = None,
        partition: Optional[str] = None,
        df_spark: Optional[SparkDataFrame] = None,
        column_select: Optional[List[str]] = None,
    ) -> None:
        self.logger = logger
        self.spark = spark
        self.responses = responses or []
        self.partition = partition
        self._df: Optional[pd.DataFrame] = None
        self.df_spark = df_spark
        self.column_select = column_select

    def _read_all_parquets(self) -> pd.DataFrame:
        if not self.responses:
            self.logger.error(
                "Nenhum conteúdo foi fornecido para leitura dos arquivos."
            )
            raise ValueError("A lista de arquivos está vazia.")

        dfs = []
        for i, content in enumerate(self.responses):
            try:
                buffer = io.BytesIO(content)
                df = pd.read_parquet(buffer)
                dfs.append(df)
            except Exception as e:
                self.logger.error(f"Falha ao ler o arquivo {i}: {e}")
                raise

        return pd.concat(dfs, ignore_index=True)

    def _create_column_partition(self) -> SparkDataFrame:
        return self.df_spark.withColumn(
            "partition", F.date_format(F.col(self.partition), "yyyy-MM")
        )

    def create_dataframe_spark(self) -> SparkDataFrame:
        if not self.partition:
            self.logger.error("A coluna de partição não foi informada.")
            raise ValueError("Informe a coluna de partição para continuar.")

        try:
            self._df = self._read_all_parquets()
            self.df_spark = self.spark.createDataFrame(self._df)
            self.df_spark = self._create_column_partition()
            return self.df_spark
        except Exception as e:
            self.logger.error(f"Erro ao criar o DataFrame Spark: {e}")
            raise

    def select_expr_dataframe_spark(self) -> SparkDataFrame:
        if not self.column_select:
            self.logger.error("A lista de colunas para seleção não foi informada.")
            raise ValueError("Informe as colunas que deseja selecionar.")

        try:
            self.df_spark = self.df_spark.selectExpr(*self.column_select)

            if self.partition:
                self.df_spark = self._create_column_partition()

            return self.df_spark
        except Exception as e:
            self.logger.error(f"Erro ao selecionar colunas do DataFrame: {e}")
            raise