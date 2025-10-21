# src/cleaning/data_cleaner.py

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, TimestampType

def clean_data(df: DataFrame) -> DataFrame:
    """
    Performs initial data cleaning, type casting, and validation 
    on the denormalized dataset before it is broken down into tables.

    Args:
        df: The raw Spark DataFrame ingested from the source.

    Returns:
        A pre-cleaned Spark DataFrame ready for normalization.
    """
    print("--- Starting Data Cleaning and Transformation (Pre-Normalization) ---")

    # 1. Standardize/Rename Key Columns
    df = df.withColumnRenamed("order_purchase_timestamp", "order_date")\
           .withColumnRenamed("price", "sales_price")\
           .withColumnRenamed("order_status", "status")

    # 2. Type Casting and Format Conversion
    # Standard date format for the Brazilian E-Commerce dataset
    DATE_FORMAT = "yyyy-MM-dd HH:mm:ss"

    df = df.withColumn("order_date", 
                       F.to_timestamp(F.col("order_date"), DATE_FORMAT)) \
           .withColumn("shipping_limit_date", 
                       F.to_timestamp(F.col("shipping_limit_date"), DATE_FORMAT)) \
           .withColumn("sales_price", F.col("sales_price").cast(DoubleType())) \
           .withColumn("freight_value", F.col("freight_value").cast(DoubleType()))

    # 3. Handle Critical Missing Values and Filter
    initial_count = df.count()
    
    # Drop records where the primary key attributes for the transaction are missing
    df_cleaned = df.na.drop(subset=["order_id", "customer_id", "order_date", "sales_price"])
    
    dropped_count = initial_count - df_cleaned.count()
    if dropped_count > 0:
        print(f"Dropped {dropped_count} rows due to missing critical transaction IDs/Metrics.")

    # 4. Filter for Valid/Completed Transactions (Needed for accurate sales metrics)
    # We only keep transactions that resulted in a delivered or completed status.
    valid_statuses = ["delivered", "shipped"]
    df_cleaned = df_cleaned.filter(F.col("status").isin(valid_statuses))
    
    print(f"Filtered out {df.count() - df_cleaned.count()} rows with invalid status.")
    print(f"Cleaning complete. Final record count: {df_cleaned.count()}")
    
    return df_cleaned


if __name__ == "__main__":
    # --- Local Test Block ---
    # This block requires the raw data to be downloaded first by running data_ingestor.py
    from connections.spark_session import get_spark_session
    from connections.s3_connector import read_data_from_s3
    # Use parent-level config for paths
    from ..src_config import LOCAL_RAW_DATA_PATH, RAW_FILE_FORMAT, RAW_FILE_OPTIONS 
    
    spark = get_spark_session()
    
    try:
        print("Attempting to read local data for cleaning test...")
        local_input_path = f"file:///{LOCAL_RAW_DATA_PATH}"
        
        raw_test_df = read_data_from_s3(
            spark=spark, 
            s3_path=local_input_path, 
            file_format=RAW_FILE_FORMAT, 
            **RAW_FILE_OPTIONS
        )
        
        cleaned_test_df = clean_data(raw_test_df)
        
        print("\nCleaned Data Sample:")
        cleaned_test_df.select("order_date", "sales_price", "status").limit(5).show(truncate=False)

    except Exception as e:
        print(f"Test Failed. Ensure data is downloaded. Error: {e}")
        
    finally:
        spark.stop()