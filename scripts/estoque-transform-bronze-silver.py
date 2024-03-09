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

amazonS3 = spark.read.options(delimiter=";", header=True).json("s3://athosbucketimage/bronze/")

# transformando tipos dos tipos de dados
df_normalized = amazonS3.withColumn("prod_id", F.col("prod_id").cast(StringType())) \
                        .withColumn("id_transacao", F.col("id_transacao").cast(StringType())) \
                        .withColumn("qtd", F.col("qtd").cast(IntegerType())) \
                        .withColumn("tipo_transacao", F.col("tipo_de_transacao").cast(IntegerType())) \
                        .withColumn("saldo", F.col("saldo").cast(IntegerType())) \
                        .withColumn("data_hora", F.col("timestamp").cast(TimestampType())) \
                        .withColumn("nf_entrada", F.col("nf_entrada").cast(StringType()))

                        
df_drop = df_formatado.drop(F.col("truncado"))

df_normalized = df_drop.select("prod_id","id_transacao", "qtd", "tipo_transacao", "saldo", "data_hora", "nf_entrada")

df_no_duplicates = df_drop.dropDuplicates(["prod_id","id_transacao", "qtd", "tipo_transacao", "saldo", "data_hora", "nf_entrada"])
df_no_duplicates.show()

database = "estoque"
tabela = "silver"
output_path = "s3://athosbucketimage/silver/"
df_drop.write.insertInto(f"{database}.{tabela}",overwrite=True)