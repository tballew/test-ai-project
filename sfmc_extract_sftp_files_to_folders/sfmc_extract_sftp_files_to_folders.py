import sys
import io
import os
import logging
import pyspark.sql.functions as F
from pyspark.sql import SparkSession
import zipfile
import datetime
import re
import boto3
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from botocore.exceptions import ClientError
import glob

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)
s3_client = boto3.client('s3')

def init_spark_glue(args):
    # Initialize params
    spark = SparkSession.builder.appName("S3_to_S3_Tracking").getOrCreate()
    glueContext = GlueContext(spark)
    job = Job(glueContext)
    job.init(args['JOB_NAME'], args)
    return spark, glueContext, job

def unzip_to_folder(zip_bytes, extract_dir):
    # Extract all the files in a zip to the specified directory
    try:
        os.makedirs(extract_dir, exist_ok=True)
        with zipfile.ZipFile(zip_bytes) as zip_ref:
            zip_ref.extractall(extract_dir)
        logger.info(f"Unzipped to {extract_dir}")
    except Exception as e:
        logger.error(f"Unzipping failed for {extract_dir}: {e}")

def upload_files_to_s3(local_folder, s3_dest_bucket, s3_prefix):
    # Upload the files to an S3 bucket.  Used to move extracted CSV files to a new directory
    for filename in glob.glob(os.path.join(local_folder, '*')):
        s3_key = f"{s3_prefix}/{os.path.basename(local_folder)}/{os.path.basename(filename)}"
        s3_client.upload_file(filename, s3_dest_bucket, s3_key)
        logger.info(f"Uploaded {filename} to s3://{s3_dest_bucket}/{s3_key}")

def list_zip_files_in_s3(s3_source_bucket, s3_prefix):
    #  Get a list of all zip files, excluding those in the 'processed' directory
    zip_files = []
    paginator = s3_client.get_paginator('list_objects_v2')
    for page in paginator.paginate(Bucket=s3_source_bucket):  #, Prefix=s3_prefix):
        for obj in page.get('Contents', []):
            key = obj['Key']
            if key.endswith('.zip') and '/processed' not in key:
                zip_files.append(key)
                
    logger.info(f"zip_files={zip_files}")
    return zip_files

def move_s3_file(s3_source_bucket, s3_dest_bucket, source_key, destination_key):
    # Move files from one s3 bucket location to another.  Used to move zip files to '/processed/ directory after extracted
    try:
        s3_client.copy_object(Bucket=s3_source_bucket, CopySource={'Bucket': s3_source_bucket, 'Key': source_key}, Key=destination_key)
        s3_client.delete_object(Bucket=s3_source_bucket, Key=source_key)
        logger.info(f"Moved s3://{s3_source_bucket}/{source_key} to s3://{s3_source_bucket}/{destination_key}")
    except Exception as e:
        logger.error(f"Failed to move s3://{s3_source_bucket}/{source_key} to s3://{s3_source_bucket}/{destination_key}: {e}")
        raise

def main():
    args = getResolvedOptions(sys.argv, [
        'JOB_NAME',
        'S3_OUTPUT_BASE',
        'S3_SFTP_SOURCE'
    ])

    local_output_base = "/tmp/Tracking/CSV/"  # temp directory on Glue worker
    s3_sftp_source = args['S3_SFTP_SOURCE']  # s3 bucket for SFTP source
    s3_output_base = args['S3_OUTPUT_BASE']  # s3 bucket for location to put CSV files

    s3_source_bucket = s3_sftp_source.replace("s3://", "").split('/')[0]
    s3_dest_bucket = s3_output_base.replace("s3://", "").split('/')[0]
    s3_prefix = '/'.join(s3_output_base.replace("s3://", "").split('/')[1:])
    logger.info(f"s3_source_bucket={s3_source_bucket}...s3_dest_bucket={s3_dest_bucket}...s3_prefix{s3_prefix}")
    
    processed_prefix = f"Tracking/processed"

    try:
        spark, glueContext, job = init_spark_glue(args)

        zip_files = list_zip_files_in_s3(s3_source_bucket, s3_prefix)  # Get a list of all the zip files
        for zip_key in zip_files:
            filename = os.path.basename(zip_key)
            logger.info(f"processing file {filename}")
            
            match = re.match(r"TrackingExtract(\d{4})(\d{2})(\d{2})\.zip", filename)
            if not match:
                logger.info(f"Filename {filename} does not match expected pattern, skipping.")
                continue
            year, month, day = match.groups()
            date_folder = year + month + day
            extract_dir = os.path.join(local_output_base, date_folder)
            logger.info(f"extract_dir={extract_dir}")
            
            # Extract files from each zip
            logger.info(f"Processing s3://{s3_source_bucket}/{zip_key} ...")
            zip_buffer = io.BytesIO()
            s3_client.download_fileobj(s3_source_bucket, zip_key, zip_buffer)
            zip_buffer.seek(0)
            unzip_to_folder(zip_buffer, extract_dir)
            
            # Move extracted files to a new directory
            logger.info(f"Move files to {s3_dest_bucket}/{s3_prefix}/csv_files")
            upload_files_to_s3(
                extract_dir,
                s3_dest_bucket,
                f"{s3_prefix}/csv_files"
            )

            # Move the processed zip file to the /processed directory 
            destination_key = f"{processed_prefix}/{filename}"
            move_s3_file(s3_source_bucket, s3_dest_bucket, zip_key, destination_key)

        logger.info("All zip files processed, uploaded, and moved to processed folder in S3.")

    except Exception as e:
        logger.error(f"Job failed: {e}")
        raise

if __name__ == "__main__":
    main()
