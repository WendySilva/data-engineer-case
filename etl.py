from src.data_requests import DataRequests
from src.data_transformers import DataTransformers
import os
from dotenv import load_dotenv
import pandas as pd
from pyspark.sql import SparkSession
import pyspark.sql.functions as F

# def parametros():
#     load_dotenv()
#     url = os.getenv("URL_DADOS")
#     api_key = os.getenv("API_KEY")

#     return url, api_key

def job(spark: SparkSession):
    url = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2023-01.parquet"
    requisicao = DataRequests(url)
    buffer = requisicao.get_response()

    transformacao = DataTransformers(buffer, spark)
    df_spark = transformacao.create_dataframe_spark()

    df = (
        df_spark
        .withColumn(
            'tpep_pickup_date', F.to_timestamp("tpep_pickup_datetime")
        )
        .filter(
            (F.col('tpep_pickup_date') >= '2023-01-01') &
            (F.col('tpep_pickup_date') <= '2023-05-31')
        )
        .selectexpr(
            "VendorID",
            "passenger_count", 
            "total_amount",
            "tpep_pickup_datetime", 
            "tpep_dropoff_datetime"
        )
    )

    df.show()



if __name__ == "__main__":
    spark = SparkSession.builder.appName("ETL Job").getOrCreate()
    job(spark)