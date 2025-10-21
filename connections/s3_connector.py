# connections/s3_connector.py (Adding local download function)

from pyspark.sql import SparkSession, DataFrame
from .secrets_manager import get_secret 
from .aws_config import AWS_REGION,S3_SECRET_NAME
# from config import S3_SECRET_NAME # Used to fetch keys for Boto3
import boto3
import os

# Helper function to convert a standard S3 URI to the Spark s3a URI
def s3_to_s3a(s3_path: str) -> str:
    """Converts a standard s3:// URI to the s3a:// protocol required by Spark/Hadoop."""
    if s3_path.startswith("s3://"):
        return s3_path.replace("s3://", "s3a://")
    return s3_path

# --- Existing read_data_from_s3 function goes here (modified slightly for clarity) ---

def read_data_from_s3(
    spark: SparkSession, 
    s3_path: str, 
    file_format: str = "parquet", 
    **options
) -> DataFrame:
    # ... (body of this function remains as provided in previous turn)
    # This function is now used primarily for reading the downloaded local file.
    # We'll use a local path argument when calling it later.
    s3a_path = s3_to_s3a(s3_path)
    print(f"Reading data from: {s3a_path}")
    try:
        df = spark.read.format(file_format).options(**options).load(s3a_path)
        print(f"Successfully read data from {file_format} file format.")
        return df
    except Exception as e:
        print(f"ERROR reading data from {s3a_path}: {e}")
        raise e

# --- New Function for Secure Local Download ---
def download_file_from_s3(s3_uri: str, local_path: str) -> None:
    """
    Downloads a single file from S3 to a local path using Boto3.
    Requires S3_SECRET_NAME to be configured.
    """
    print(f"Attempting to download {s3_uri} to {local_path}...")
    
    try:
        # 1. Fetch credentials from Secrets Manager
        s3_creds = get_secret(S3_SECRET_NAME)
        access_key = s3_creds["access_key_s3"] # FIXED KEY NAMES
        secret_key = s3_creds["secret_key_s3"] # FIXED KEY NAMES
        
        # 2. Robustly parse S3 URI: s3://bucket-name/key/path/file.ext
        uri_parts = s3_uri.replace("s3://", "").split('/', 1)
        bucket_name = uri_parts[0]
        s3_key = uri_parts[1]
        
        # 3. Create Boto3 client (using AWS_REGION from aws_config.py via the import chain)
        session = boto3.Session(
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            region_name=AWS_REGION # Added region for client
        )
        s3_client = session.client('s3', region_name=AWS_REGION)
        
        # 4. Ensure local directory exists
        os.makedirs(os.path.dirname(local_path), exist_ok=True)
        
        # 5. Download file
        s3_client.download_file(bucket_name, s3_key, local_path)
        
        print(f"Download successful to {local_path}")
        
    except Exception as e:
        print(f"FATAL ERROR during S3 download: {e}")
        raise e
        

def write_data_to_s3(
    df: DataFrame, 
    s3_path: str, 
    file_format: str = "parquet", 
    mode: str = "overwrite", 
    **options
) -> None:

    print(1)