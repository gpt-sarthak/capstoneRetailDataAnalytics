# connections/spark_session.py (Revised - Cleaner Executor Environment)

from pyspark.sql import SparkSession
# Removed dependency on Secrets Manager and S3_SECRET_NAME here

# Define necessary external packages (keep this, as it may be needed for other tasks, e.g., Hadoop utilities)
S3_PACKAGES = "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk:1.12.569"

def get_spark_session(app_name: str = "SalesForecastingPipeline") -> SparkSession:
    """
    Creates a basic, unconfigured SparkSession, minimizing Executor overhead.
    Credentials for S3 are now managed by the Driver process.
    """
    
    # 1. Build the Spark Session
    spark = (
        SparkSession.builder
        .appName(app_name)
        .config("spark.jars.packages", S3_PACKAGES)
        # REMOVED: spark.hadoop.fs.s3a.access.key configuration
        # REMOVED: spark.hadoop.fs.s3a.secret.key configuration
        .getOrCreate()
    )
    
    spark.sparkContext.setLogLevel("WARN")
    print("SparkSession created")
    return spark

# Note: The main block for testing the session creation remains the same.