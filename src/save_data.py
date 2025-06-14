from pyspark.sql import SparkSession, DataFrame as SparkDataFrame
from typing import Optional
from logging import Logger


class SaveData:
    """
    Classe responsável por salvar um DataFrame Spark em um bucket S3 como tabela Delta
    e registrar a tabela no catálogo do Databricks, caso ainda não exista.

    Atributos
    ----------
    logger : Logger
        Instância de logger para registrar mensagens de erro e informação.

    spark : SparkSession
        Sessão ativa do Spark.

    df : SparkDataFrame
        DataFrame Spark que será salvo.

    layer : str
        Nome da camada (schema) no catálogo (por exemplo: bronze, silver, gold).

    table : str
        Nome da tabela que será criada ou atualizada.

    partition : str, opcional
        Nome da coluna usada como partição do Delta Table.

    bucket : str, opcional
        Caminho do bucket S3 onde os dados serão armazenados. Padrão: 's3://wendy-ifood-case/'.

    catalogo : str, opcional
        Nome do catálogo Databricks onde a tabela será registrada. Padrão: 'ifood_case'.

    mode_save : str, opcional
        Modo de escrita do Spark. Pode ser 'append', 'overwrite', entre outros. Padrão: 'append'.

    format_save : str, opcional
        Formato de salvamento dos dados. Padrão: 'delta'.

    Métodos
    -------
    saveData():
        Salva os dados no bucket e registra a tabela no catálogo se ela não existir.
    """

    def __init__(
        self,
        logger: Logger,
        spark: SparkSession,
        df: SparkDataFrame,
        layer: str,
        table: str,
        partition: str = None,
        bucket: Optional[str] = "s3://wendy-ifood-case/",
        catalogo: Optional[str] = "ifood_case",
        mode_save: Optional[str] = "append",
        format_save: Optional[str] = "delta",
    ) -> None:
        self.logger = logger
        self.spark = spark
        self.df = df
        self.bucket = bucket
        self.catalogo = catalogo
        self.layer = layer
        self.table = table
        self.mode_save = mode_save
        self.format_save = format_save
        self.partition = partition

    def _createCatalogo(self):
        self.spark.sql(f"CREATE CATALOG IF NOT EXISTS {self.catalogo}")

    def _createSchema(self):
        self.spark.sql(f"CREATE SCHEMA IF NOT EXISTS {self.catalogo}.{self.layer}")

    def _createTable(self):
        try:
            self._createCatalogo()
            self._createSchema()
            (
                self.df.write.mode(self.mode_save)
                .partitionBy(self.partition)
                .format(self.format_save)
                .option("path", f"{self.bucket}/{self.layer}/{self.table}")
                .saveAsTable(f"{self.catalogo}.{self.layer}.{self.table}")
            )
            self.logger.info(f"Tabela {self.table} criada com sucesso")
        except Exception as e:
            self.logger.error("Não foi possivel criar a tabela")
            self.logger.error(e)

    def _saveBucket(self):
        try:
            (
                self.df.write.format(self.format_save)
                .mode(self.mode_save)
                .partitionBy(self.partition)
                .save(f"{self.bucket}/{self.layer}/{self.table}")
            )
        except Exception as e:
            self.logger.error("Não foi possivel salvar no bucket")
            self.logger.error(e)

    def saveData(self):
        try:
            if not self.spark.catalog.tableExists(
                f"{self.catalogo}.{self.layer}.{self.table}"
            ):
                self.logger.info("Tabela não existe, criando...")
                self._createTable()
            else:
                self.logger.info("Tabela existe, salvando no bucket...")
                self._saveBucket()
        except Exception as e:
            self.logger.error("Não foi possivel salvar a tabela")
            self.logger.error(e)