import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.functions import col, date_format
from pyspark.sql.types import StringType, DecimalType, TimestampType, IntegerType

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# carregando dataframe silver
df_silver = spark.read.options(delimiter=";", header=True).parquet("s3://athosbucketimage/silver/")

# carregando dataframe produtos
df_produto = spark.read.option("delimiter", "|").option("header", "true").parquet("s3://athosbucketimage/produto_silver/")

# transformando tipos dos tipos de dados de produto_dataset
df_produto_normalized = df_produto.withColumn("id_produto", F.col("id_produto").cast(IntegerType())) \
                        .withColumn("nome_produto", F.col("nome_produto").cast(StringType())) \
                        .withColumn("saldo_produto", F.col("saldo_produto").cast(IntegerType())) \
                        .withColumn("custo_produto", F.col("custo_produto").cast(DecimalType())) \
                        .withColumn("preco_produto", F.col("preco_produto").cast(DecimalType())) \
                        .withColumn("tipo_produto", F.col("tipo_produto").cast(StringType())) \
                        .withColumn("marca_produto", F.col("marca_produto").cast(StringType())) \
                        .withColumn("frete_produto", F.col("frete_produto").cast(DecimalType()))

# # renomeando coluna do dataframe produtos
df_produto = df_produto_normalized.withColumnRenamed("id_produto", "prod_id")

# join estoque e produtos 
result_df = df_silver.join(df_produto, on='prod_id', how='inner')

# Selecionando as colunas para o dataframe final
result_df = result_df.select("prod_id", "qtd", "tipo_de_transacao", "saldo", "data_hora", "nome_produto", "preco_produto", "tipo_produto", "marca_produto")

# transformando tipos dos tipos de dados de result_df
result_df_normalized = result_df.withColumn("prod_id", F.col("prod_id").cast(IntegerType())) \
                        .withColumn("qtd", F.col("qtd").cast(IntegerType())) \
                        .withColumn("tipo_de_transacao", F.col("tipo_de_transacao").cast(DecimalType())) \
                        .withColumn("saldo", F.col("saldo").cast(IntegerType())) \
                        .withColumn("data_hora", F.col("data_hora").cast(TimestampType())) \
                        .withColumn("nome_produto", F.col("nome_produto").cast(StringType())) \
                        .withColumn("preco_produto", F.col("preco_produto").cast(DecimalType())) \
                        .withColumn("tipo_produto", F.col("tipo_produto").cast(StringType())) \
                        .withColumn("marca_produto", F.col("marca_produto").cast(StringType()))

database = "estoque"
tabela = "produto_estoque_gold"
result_df_normalized.write.insertInto(f"{database}.{tabela}",overwrite=True)