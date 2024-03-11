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
amazonS3 = spark.read.options(delimiter=";", header=True).parquet("s3://athosbucketimage/silver/")

# visualizando dataframe estoque
amazonS3.show()

# carregando dataframe produtos
produto_dataset = spark.read.option("delimiter", "|").option("header", "true").parquet("s3://athosbucketimage/produto_silver/")

# visualizando dataframe produtos
produto_dataset.show()

# # transformando tipos dos tipos de dados de produto_dataset
df_produto_normalized = produto_dataset.withColumn("id_produto", F.col("id_produto").cast(IntegerType())) \
                        .withColumn("nome_produto", F.col("nome_produto").cast(StringType())) \
                        .withColumn("saldo_produto", F.col("saldo_produto").cast(IntegerType())) \
                        .withColumn("custo_produto", F.col("custo_produto").cast(DecimalType())) \
                        .withColumn("preco_produto", F.col("preco_produto").cast(DecimalType())) \
                        .withColumn("tipo_produto", F.col("tipo_produto").cast(StringType())) \
                        .withColumn("marca_produto", F.col("marca_produto").cast(StringType())) \
                        .withColumn("frete_produto", F.col("frete_produto").cast(DecimalType()))

# visualizando dataframe produtos normalizado
df_produto_normalized.show()

# # renomeando colunas dataframe produtos
produto_dataset = df_produto_normalized.withColumnRenamed("id_produto", "prod_id") \
#                                         .withColumnRenamed("Saldo", "saldo_produto") \
#                                         .withColumnRenamed("NomeProduto", "nome_produto") \
#                                         .withColumnRenamed("TipoProduto", "tipo_produto")

# join estoque e produtos 
result_df = amazonS3.join(produto_dataset, on='prod_id', how='inner')

# visualizando resultado do join
result_df.show()

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

result_df_normalized.show()

# result_df_normalized = result_df_normalized.withColumn("data_hora", F.date_format("data_hora", "dd/MM/yyyy"))
#                 .withColumn("data_hora", date_format("data_hora", "dd/MM/yyyy HH:mm:ss")) \
# result_df_normalized.show()

database = "estoque"
tabela = "produto_estoque_gold"
result_df_normalized.write.insertInto(f"{database}.{tabela}",overwrite=True)

# job.commit()