# src/ingestion/data_ingestor.py (Revised - Local-first strategy)

from pyspark.sql import SparkSession, DataFrame
from connections.spark_session import get_spark_session
# Import the new download function
from connections.s3_connector import read_data_from_s3, download_file_from_s3 
from ...config import (
    RAW_DATA_PATH, 
    RAW_FILE_FORMAT, 
    RAW_FILE_OPTIONS, 
    LOCAL_RAW_DATA_PATH # New local path import
) 

def ingest_raw_data(spark: SparkSession) -> DataFrame:
    """
    Downloads raw data from S3 to a local directory, then reads it into a DataFrame.
    
    Args:
        spark: The active SparkSession object.

    Returns:
        A Spark DataFrame containing the raw data.
    """
    print("--- Starting Data Ingestion (Local-First Strategy) ---")
    
    # 1. Download the raw file from S3
    try:
        download_file_from_s3(
            s3_uri=RAW_DATA_PATH, 
            local_path=LOCAL_RAW_DATA_PATH
        )
    except Exception as e:
        print("Download failed. Cannot proceed with ingestion.")
        raise e
        
    # 2. Read the local file using Spark
    # Spark understands the 'file:///' scheme (which it infers) for local files.
    print(f"Reading data from local path: {LOCAL_RAW_DATA_PATH}")
    
    # Note: We must explicitly use 'file:///' for local paths in some environments.
    spark_compatible_path = f"file:///{LOCAL_RAW_DATA_PATH}" 
    
    try:
        raw_df = read_data_from_s3(
            spark=spark, 
            s3_path=spark_compatible_path, 
            file_format=RAW_FILE_FORMAT, 
            **RAW_FILE_OPTIONS
        )
        
        print(f"Ingestion complete. Total records: {raw_df.count()}")
        return raw_df
    
    except Exception as e:
        print(f"FATAL ERROR during Spark read of local data: {e}")
        raise

if __name__ == "__main__":
    # Test execution
    spark = get_spark_session()
    try:
        sales_data = ingest_raw_data(spark)
        sales_data.limit(5).show(truncate=False)
    except Exception as e:
        print(f"Test failed: {e}")
    finally:
        spark.stop()