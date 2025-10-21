# src/cleaning/data_breakdown.py

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T
import os
import shutil

# Import configurations and connector utilities
from connections.s3_connector import write_data_to_s3
from ..src_config import LOCAL_PROCESSED_DATA_DIR, OUTPUT_FILE_FORMAT, OUTPUT_FILE_OPTIONS

# The 9 core tables derived from the column names:
# Primary Key columns are listed first.
TABLE_SCHEMAS = {
    # DIMENSIONS
    "customers": ["customer_unique_id", "customer_zip_code_prefix", "customer_city", "customer_state"],
    "geolocation": ["customer_zip_code_prefix"], # NOTE: This only extracts the prefix, as geolocation data is usually separate
    "sellers": ["seller_id", "seller_zip_code_prefix", "seller_city", "seller_state"],
    "products": [
        "product_id", "product_category_name", "product_category_name_english",
        "product_name_lenght", "product_description_lenght", "product_photos_qty",
        "product_weight_g", "product_length_cm", "product_height_cm", "product_width_cm"
    ],
    "order_reviews": [
        "review_id", "order_id", "review_score", "review_comment_title", 
        "review_comment_message", "review_creation_date", "review_answer_timestamp"
    ],
    "order_payments": [
        "order_id", "payment_sequential", "payment_type", 
        "payment_installments", "payment_value"
    ],

    # FACTS (Contain Foreign Keys and Metrics)
    "orders": [
        "order_id", "customer_id", "order_status", "order_purchase_timestamp", 
        "order_approved_at", "order_delivered_carrier_date", 
        "order_delivered_customer_date", "order_estimated_delivery_date"
    ],
    "order_items": [
        "order_id", "order_item_id", "product_id", "seller_id", 
        "shipping_limit_date", "price", "freight_value"
    ]
}
# We skip the 'order_customer' link table as 'customer_id' is already in 'orders'

def break_data_into_tables(df: DataFrame) -> dict:
    """
    Splits the denormalized DataFrame into dimensional and fact tables,
    removes duplicates, and saves them locally as Parquet files.

    Args:
        df: The input denormalized DataFrame.

    Returns:
        A dictionary where keys are table names and values are the resulting DataFrames.
    """
    print("--- Starting Data Breakdown (Normalization) ---")
    
    # Clean up the previous local output directory
    if os.path.exists(LOCAL_PROCESSED_DATA_DIR):
        shutil.rmtree(LOCAL_PROCESSED_DATA_DIR)
        print(f"Cleaned previous output directory: {LOCAL_PROCESSED_DATA_DIR}")
        
    extracted_tables = {}
    
    for table_name, columns in TABLE_SCHEMAS.items():
        # 1. Select only the relevant columns for the table
        extracted_df = df.select(*columns)
        
        # 2. Drop duplicates based on the primary key(s)
        pk_columns = columns[:1] if len(columns) > 0 else columns
        
        if table_name == "order_items":
            # order_items is a composite key (order_id, order_item_id)
            pk_columns = ["order_id", "order_item_id"]
        elif table_name == "order_payments":
            # order_payments is a composite key (order_id, payment_sequential)
            pk_columns = ["order_id", "payment_sequential"]
            
        print(f"Extracting {table_name}. PKs: {pk_columns}")
        
        # Remove any rows where the primary key is null (critical for integrity)
        extracted_df = extracted_df.na.drop(subset=pk_columns)
        
        # Drop duplicates to create true dimension/fact tables
        extracted_df = extracted_df.dropDuplicates(subset=pk_columns)
        
        extracted_tables[table_name] = extracted_df
        
        # 3. Write to local storage (Parquet)
        output_path = f"file:///{LOCAL_PROCESSED_DATA_DIR}/{table_name}"
        
        extracted_df.write.format(OUTPUT_FILE_FORMAT) \
            .mode(OUTPUT_FILE_OPTIONS['mode']) \
            .option("compression", OUTPUT_FILE_OPTIONS['compression']) \
            .save(output_path)
            
        print(f"-> {table_name}: {extracted_df.count()} unique records written to {output_path}")

    print("--- Data Breakdown Complete. Tables stored locally. ---")
    return extracted_tables


if __name__ == "__main__":
    from connections.spark_session import get_spark_session
    from connections.s3_connector import read_data_from_s3
    from src.ingestion.data_ingestor import ingest_raw_data
    from src.cleaning.data_cleaner import clean_data
    
    spark = get_spark_session()
    try:
        # NOTE: Assumes data_ingestor has run successfully to put raw_sales.csv locally
        raw_df = ingest_raw_data(spark)
        
        # Perform initial cleaning for consistent schema before breaking down
        # This prevents breaking the schema during the breakdown step
        pre_cleaned_df = clean_data(raw_df) 

        tables = break_data_into_tables(pre_cleaned_df)
        
        print("\nVerification of Table Counts:")
        for name, df in tables.items():
            print(f"- {name}: {df.count()} rows")

    except Exception as e:
        print(f"Data breakdown test failed: {e}")
        
    finally:
        spark.stop()