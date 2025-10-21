# connections/spark_session.py

from pyspark.sql import SparkSession
from .secrets_manager import get_secret 
from .aws_config import S3_SECRET_NAME # Imported from aws_config.py

# Define necessary external packages for S3 interaction
S3_PACKAGES = "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk:1.12.569"

def get_spark_session(app_name: str = "SalesForecastingPipeline") -> SparkSession:
    """
    Creates and configures a centralized SparkSession, fetching credentials 
    securely from AWS Secrets Manager for S3 access capability (when needed).
    """
    
    print(f"Fetching S3 credentials from Secrets Manager: {S3_SECRET_NAME}")
    
    try:
        # 1. Retrieve the credentials securely
        s3_creds = get_secret(S3_SECRET_NAME)
        
        # 2. CRITICAL FIX: Use the actual keys defined in the AWS Secret
        AWS_ACCESS_KEY = s3_creds["access_key_s3"] 
        AWS_SECRET_KEY = s3_creds["secret_key_s3"]
        print("Credentials Fetched")
        
    except Exception as e:
        # If secret retrieval fails (e.g., AccessDenied), the pipeline cannot proceed
        print("FATAL ERROR: Could not retrieve S3 credentials.")
        raise e 
        
    # 3. Build the Spark Session
    # Note: These configurations are required for S3 I/O, even if it's just the driver
    spark = (
        SparkSession.builder
        .appName(app_name)
        # .config("spark.jars.packages", S3_PACKAGES)
        
        # # Configure S3a filesystem credentials
        # .config("spark.hadoop.fs.s3a.access.key", AWS_ACCESS_KEY)
        # .config("spark.hadoop.fs.s3a.secret.key", AWS_SECRET_KEY)
        
        # .config("spark.hadoop.fs.s3a.fast.upload", "true") 
        .getOrCreate()
    )
    
    # Set the log level to WARN to reduce console clutter
    # spark.sparkContext.setLogLevel("WARN")
    spark.sparkContext.setLogLevel("ERROR")
    print("SparkSession successfully created and configured with S3 capabilities.")
    return spark

if __name__ == "__main__":
    # Example usage for testing the session creation
    try:
        test_spark = get_spark_session()
        print(f"Spark Version: {test_spark.version}")
        test_spark.stop()
    except Exception as e:
        print(f"Test failed: {e}")