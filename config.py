# config.py

# --- AWS Secrets Manager Configuration ---
# The name of the secret storing your S3 access_key and secret_key
S3_SECRET_NAME = "capstone/spark/s3-credentials" 
# The AWS region where your secrets and data buckets reside
AWS_REGION = "us-east-1" 

# --- S3 Data Paths and Names ---
# Name of the primary S3 bucket for the project
S3_BUCKET_NAME = "capstone-raw-data/"

# Path to the raw, unprocessed input data
RAW_DATA_PATH = f"s3 ://{S3_BUCKET_NAME}/sales_data/denormalized_brazilian_dataset.csv" 

# Path for intermediate/cleaned data (e.g., Parquet for fast reading)
CLEANED_DATA_PATH = f"s3://{S3_BUCKET_NAME}/processed/cleaned_sales_data/"

# Path for final analytical tables (e.g., aggregations or metrics)
ANALYSIS_DATA_PATH = f"s3://{S3_BUCKET_NAME}/analysis/final_metrics/"

# Path to store the final forecast results
FORECAST_OUTPUT_PATH = f"s3://{S3_BUCKET_NAME}/forecasts/next_month_sales/"
 

# --- Data Processing Settings ---
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

# --- Forecasting Parameters ---
# Number of periods (months) to forecast ahead
FORECAST_PERIODS = 1

# --- Local Processing Configuration ---
# Local directory on your machine for temporary data storage
LOCAL_DATA_DIR = "local_cache/data" 
# Full path to the local raw data file
LOCAL_RAW_DATA_PATH = f"{LOCAL_DATA_DIR}/raw_sales.csv"