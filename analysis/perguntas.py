import pyspark.sql.functions as F

df_analysis = (
    df
    .withColumn(
        "year_month", 
        F.date_format(
            "tpep_pickup_datetime", "yyyy-MM"
        )
    )
)

# Qual a média de valor total (total\_amount) recebido em um mês
# considerando todos os yellow táxis da frota?

media_valor_mensal = (
    df_analysis
    .groupBy("ano_mes")
    .agg(
        F.avg("total_amount")
        .alias("media_total_amount")
    )
)

# Qual a média de passageiros (passenger\_count) por cada hora do dia
# que pegaram táxi no mês de maio considerando todos os táxis da
# frota?

media_passageiros_por_hora = (
    df_analysis
    .filter(
        F.col('year_month') == '2023-05'
    )
    .withColumn(
        "hora", hour("pickup_ts")
    )
    .groupBy("hora")
    .agg(
        avg("passenger_count")
        .alias("media_passageiros")
    )
)