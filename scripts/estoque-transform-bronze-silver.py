import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, DecimalType, TimestampType, IntegerType
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# carregando dados da bronze
df_bronze = spark.read.options(delimiter=";", header=True).json("s3://athosbucketimage/bronze/")

# transformando tipos dos tipos de dados
df_normalized = df_bronze.withColumn("prod_id", F.col("prod_id").cast(IntegerType())) \
                        .withColumn("id_transacao", F.col("id_transacao").cast(IntegerType())) \
                        .withColumn("qtd", F.col("qtd").cast(IntegerType())) \
                        .withColumn("tipo_de_transacao", F.col("tipo_de_transacao").cast(IntegerType())) \
                        .withColumn("saldo", F.col("saldo").cast(IntegerType())) \
                        .withColumn("data_hora", F.col("timestamp").cast(TimestampType())) \
                        .withColumn("nf_entrada", F.col("nf_entrada").cast(StringType()))

# dropando a coluna truncado                      
df_drop = df_normalized.drop(F.col("truncado"))

# Selecionando colunas sem a coluna timestamp
df_result = df_drop.select("prod_id","id_transacao", "qtd", "tipo_de_transacao", "saldo", "data_hora", "nf_entrada")

# retirando linhas duplicados
df_no_duplicates = df_result.dropDuplicates(["prod_id","id_transacao", "qtd", "tipo_de_transacao", "saldo", "data_hora", "nf_entrada"])

# escrevendo dataframe
database = "estoque"
tabela = "silver"
df_no_duplicates.write.insertInto(f"{database}.{tabela}",overwrite=True)