# src/src_config.py (NEW - Processing Parameters)

# --- Data Processing Settings ---
S3_BUCKET_NAME = "capstone-new-bucket-2"
RAW_DATA_PATH = f"s3://{S3_BUCKET_NAME}/capstone-raw-data/sales_data/denormalized_brazilian_dataset.csv"
# --- Local Processing Configuration ---
# Local directory on your machine for temporary data storage
LOCAL_PROCESSED_DATA_DIR = "data/processed_tables"
LOCAL_DATA_DIR = "data/raw" 
# Full path to the local raw data file
LOCAL_RAW_DATA_PATH = f"{LOCAL_DATA_DIR}/raw_sales.csv"

# Input file format and options
RAW_FILE_FORMAT = "csv"
RAW_FILE_OPTIONS = {
    "header": "true",
    "inferSchema": "true", 
    "sep": ","
}

# Output file format and options (Parquet is recommended for Spark)
OUTPUT_FILE_FORMAT = "parquet"
OUTPUT_FILE_OPTIONS = {
    "mode": "overwrite",
    "compression": "snappy"
}

# --- Data Breakdown Schemas (Used by data_breakdown.py) ---
# Primary Key columns are listed first.
TABLE_SCHEMAS = {
    # DIMENSIONS
    "customers": ["customer_unique_id", "customer_zip_code_prefix", "customer_city", "customer_state"],
    "geolocation": ["customer_zip_code_prefix"],
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

# --- Forecasting Parameters ---
# Number of periods (months) to forecast ahead
FORECAST_PERIODS = 1