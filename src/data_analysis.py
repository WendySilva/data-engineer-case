from pyspark.sql import DataFrame as SparkDataFrame
import pyspark.sql.functions as F
from typing import Optional
from logging import Logger


class DataAnalysis:
    """
    DataAnalysis

    Classe responsável por realizar análises estatísticas em DataFrames Spark relacionados a dados de corridas de táxi.

    Atributos:
    -----------
    logger : Logger
        Instância de logger utilizada para registrar mensagens e erros.

    df : SparkDataFrame
        DataFrame com os dados de entrada para análise.

    column_group_by : Optional[str]
        Nome da coluna usada para agrupar os dados na análise mensal de valores.

    column_year_month : Optional[str]
        Nome da coluna com a informação de ano e mês no formato "yyyy-MM".

    value_year_month : Optional[str]
        Valor específico da coluna de ano/mês para filtrar os dados na análise por hora.

    column_date_hour : Optional[str]
        Nome da coluna de timestamp usada para extrair hora e data.

    Métodos:
    --------

    mediaValorMensal() -> Optional[SparkDataFrame]
        Calcula a média do valor total das corridas ("total_amount") por grupo (ex: por mês).
        Retorna um DataFrame com a média arredondada na coluna "media_total_amount".
        Em caso de erro, retorna None.

    mediaPassageirosHora() -> Optional[SparkDataFrame]
        Calcula a média de passageiros por hora em dias de um mês específico (`value_year_month`).
        Retorna um DataFrame com colunas "date", "hours" e "media_passageiros".
        Em caso de erro, retorna None.

    Exemplo de uso:
    ---------------
    analisador = DataAnalysis(
        logger,
        df_spark,
        column_group_by="year_month",
        column_year_month="year_month",
        value_year_month="2023-05",
        column_date_hour="tpep_pickup_datetime"
    )

    df_valor_mensal = analisador.mediaValorMensal()
    df_passageiros_hora = analisador.mediaPassageirosHora()
    """

    def __init__(
        self,
        logger: Logger,
        df: SparkDataFrame,
        column_group_by: Optional[str] = None,
        column_year_month: Optional[str] = None,
        value_year_month: Optional[str] = None,
        column_date_hour: Optional[str] = None,
    ) -> None:
        self.logger = logger
        self.df = df
        self.column_group_by = column_group_by
        self.column_year_month = column_year_month
        self.value_year_month = value_year_month
        self.column_date_hour = column_date_hour

    def mediaValorMensal(self) -> SparkDataFrame:
        try:
            self.logger.info("Calculando a média do valor total das corridas por mês")
            df = self.df.groupBy(self.column_group_by).agg(
                F.round(F.avg("total_amount"), 2).alias("media_total_amount")
            )
            self.logger.info("Média do valor total das corridas por mês calculada com sucesso")
            return df
        except Exception as e:
            self.logger.error(e)

    def mediaPassageirosHora(self) -> SparkDataFrame:
        try:
            self.logger.info("Calculando a média de passageiros por hora em um mês específico")
            df = (
                self.df.filter(F.col(self.column_year_month) == self.value_year_month)
                .withColumn("hours", F.hour(F.col(self.column_date_hour)))
                .withColumn("date", F.date_format(self.column_date_hour, "yyyy-MM-dd"))
                .groupBy(F.col("date"), F.col("hours"))
                .agg(F.round(F.avg("passenger_count"), 2).alias("media_passageiros"))
                .orderBy(F.col("date"), F.col("hours"))
            )
            self.logger.info("Média de passageiros por hora em um mês específico calculada com sucesso")
            return df
        except Exception as e:
            self.logger.error(e)