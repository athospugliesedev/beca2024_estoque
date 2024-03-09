import boto3
import sys
import os
import logging
from io import BytesIO
from azure.storage.blob import BlobServiceClient
from botocore.exceptions import ClientError
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import SparkSession

def download_blobs_to_s3(blob_list, container_name, s3_client, s3_bucket_name):
    for blob in blob_list:
        blob_client = service.get_blob_client(container_name, blob.name)
        blob_data = blob_client.download_blob().readall()
        blob_io = BytesIO(blob_data)
        blob_io.seek(0)

       # s3_object_key = os.path.basename(blob.name)

        s3_object_key = os.path.join("bronze", os.path.basename(blob.name))


        if not check_s3_object_exists(s3_client, s3_bucket_name, s3_object_key):
            try:
                response = s3_client.upload_fileobj(blob_io, s3_bucket_name, s3_object_key)
            except ClientError as e:
                logging.error(e)
                return False
    return True

def check_s3_object_exists(s3_client, bucket_name, object_key):
    try:
        s3_client.head_object(Bucket=bucket_name, Key=object_key)
        return True
    except ClientError as e:
        if e.response['Error']['Code'] == '404':
            return False
        else:
            logging.error(e)
            return False

args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
print("Job name is:".format(args["JOB_NAME"]))

credential = credential
service = BlobServiceClient(account_url="https://projetointegrado2024.blob.core.windows.net/", credential=credential)

blob_list_container1 = service.get_container_client("estoque").list_blobs()
s3 = boto3.client('s3')
success = download_blobs_to_s3(blob_list_container1, "estoque", s3, "athosbucketimage")
job.commit()
