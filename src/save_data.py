from pyspark.sql import SparkSession, DataFrame as SparkDataFrame
from typing import Optional
from logging import Logger
import os
from dotenv import load_dotenv
import boto3
from io import BytesIO
from src.data_transformers import DataTransformers



class SaveData:
    """
    Classe responsável por salvar um DataFrame do Spark em uma tabela Delta,
    registrar no catálogo Databricks, e realizar upload para o S3 na camada bronze.

    Atributos
    ----------
    logger : Logger
        Logger para registrar mensagens de informação e erro.

    spark : SparkSession
        Sessão Spark ativa.

    df : SparkDataFrame, opcional
        DataFrame a ser salvo (usado nas camadas Silver e Gold).

    layer : str
        Nome da camada de dados (bronze, silver, gold).

    table : str
        Nome da tabela que será criada ou atualizada.

    partition : str, opcional
        Nome da coluna de partição Delta (ex: 'year_month').

    bucket : str, opcional
        Nome do bucket S3. Padrão: 'wendy-ifood-case'.

    response : bytes, opcional
        Conteúdo binário do arquivo para upload no S3 (usado na camada bronze).

    catalogo : str, opcional
        Nome do catálogo Databricks. Padrão: 'ifood_case'.

    mode_save : str, opcional
        Modo de escrita do Spark: 'append', 'overwrite', etc. Padrão: 'append'.

    format_save : str, opcional
        Formato de salvamento. Padrão: 'delta'.

    ano : str, opcional
        Ano da partição do arquivo a ser salvo no S3. Usado na camada bronze.

    mes : str, opcional
        Mês da partição do arquivo a ser salvo no S3. Usado na camada bronze.

    Métodos
    -------
    saveData():
        Gerencia o fluxo completo de salvamento. Se for camada bronze, faz upload do arquivo
        e leitura para o Spark. Em seguida, salva como tabela Delta no Databricks.

    _saveBucket():
        Realiza o upload de um arquivo Parquet diretamente no bucket S3.

    _saveTable():
        Salva o DataFrame como uma tabela Delta particionada no Databricks.

    _createCatalogo():
        Cria o catálogo no Databricks se ele ainda não existir.

    _createSchema():
        Cria o schema (camada) no catálogo se ele ainda não existir.
    """


    def __init__(
        self,
        logger: Logger,
        spark: SparkSession,
        layer: str,
        table: str,
        partition: str = None,
        df: Optional[SparkDataFrame] = None,
        bucket: Optional[str] = "wendy-ifood-case",
        response: Optional[bytes] = None,
        catalogo: Optional[str] = "ifood_case",
        mode_save: Optional[str] = "append",
        format_save: Optional[str] = "delta",
        ano: Optional[str] = None,
        mes: Optional[str] = None
    ) -> None:
        self.logger = logger
        self.spark = spark
        self.df = df
        self.catalogo = catalogo
        self.layer = layer
        self.table = table
        self.mode_save = mode_save
        self.format_save = format_save
        self.partition = partition
        self.response = response
        self.bucket=bucket
        self.ano = ano
        self.mes = mes

    def _createCatalogo(self):
        self.spark.sql(f"CREATE CATALOG IF NOT EXISTS {self.catalogo}")
        self.logger.info(f"O Catalogo {self.catalogo} criado com sucesso")

    def _createSchema(self):
        self.spark.sql(f"CREATE SCHEMA IF NOT EXISTS {self.catalogo}.{self.layer}")
        self.logger.info(f"O Schema {self.layer} criado com sucesso")

    def _saveTable(self):
        try:
            self._createCatalogo()
            self._createSchema()
            (
                self.df
                .write
                .mode(self.mode_save)
                .format(self.format_save)
                .option("path", f"s3://{self.bucket}/{self.layer}/{self.table}")
                .saveAsTable(f"{self.catalogo}.{self.layer}.{self.table}")
            )
            self.logger.info(f"Tabela {self.table} salva com sucesso")

        except Exception as e:
            self.logger.error("Não foi possivel criar a tabela")
            self.logger.error(e)

    def _saveBucket(self):
        try:
            load_dotenv()
            os.getenv("AWS_ACESS_KEY_ID")
            os.getenv("AWS_SECRET_ACCESS_KEY")

            aws_access_key_id = os.getenv("AWS_ACESS_KEY_ID")
            aws_secret_access_key = os.getenv("AWS_SECRET_ACCESS_KEY")

            session = boto3.Session(
                aws_access_key_id=aws_access_key_id,
                aws_secret_access_key=aws_secret_access_key,
            )
            s3 = session.client("s3")
            buffer = BytesIO(self.response)
            chave = f"{self.layer}/arquivos/{self.table}_{self.ano}_{self.mes}.parquet"
            s3.upload_fileobj(buffer, self.bucket, chave)
            self.logger.info(f"Upload concluído para: {chave}")

        except Exception as e:
            self.logger.error("Não foi possivel salvar no bucket")
            self.logger.error(e)

    def saveData(self):
        try:
            if self.layer == "bronze":
                self.logger.info("Camada bronze, salvando no bucket...")
                self._saveBucket()
                self.df = self.spark.read.parquet(f"s3://{self.bucket}/{self.layer}/arquivos/{self.table}_{self.ano}_{self.mes}.parquet")
                self.df = DataTransformers.format_columns(self.df)

            self.logger.info("salvando tabela...")
            self._saveTable()
        except Exception as e:
            self.logger.error("Não foi possivel salvar a tabela")
            self.logger.error(e)