# config.py (Updated - Global Paths Only)

# --- AWS Secrets Manager Configuration (Key names are in aws_config.py) ---
# The AWS region where your secrets and data buckets reside
AWS_REGION = "us-east-1" 

# --- S3 Data Paths and Names ---
# Name of the primary S3 bucket for the project
S3_BUCKET_NAME = "capstone-raw-data" # Corrected: Removed trailing slash

# Path to the raw, unprocessed input data
RAW_DATA_PATH = f"s3://{S3_BUCKET_NAME}/sales_data/denormalized_brazilian_dataset.csv" 

# Path for intermediate/cleaned data (Not strictly needed since we use local cache, but kept for future S3 deployment)
CLEANED_DATA_PATH = f"s3://{S3_BUCKET_NAME}/processed/cleaned_sales_data/"

# Path for final analytical tables 
ANALYSIS_DATA_PATH = f"s3://{S3_BUCKET_NAME}/analysis/final_metrics/"

# Path to store the final forecast results
FORECAST_OUTPUT_PATH = f"s3://{S3_BUCKET_NAME}/forecasts/next_month_sales/"

# --- Local Processing Configuration ---
# Local directory on your machine for temporary data storage
LOCAL_DATA_DIR = "local_cache/data" 
# Full path to the local raw data file
LOCAL_RAW_DATA_PATH = f"{LOCAL_DATA_DIR}/raw_sales.csv"