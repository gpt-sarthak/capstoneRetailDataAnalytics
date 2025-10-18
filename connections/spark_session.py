from pyspark.sql import SparkSession

def get_spark_session(app_name="CapstoneRetailDataAnalytics"):
    spark = (
        SparkSession.builder.appName(app_name)
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4") 
        # Add configurations for S3 access, e.g.,
        # .config("spark.hadoop.fs.s3a.access.key", "YOUR_ACCESS_KEY")
        # .config("spark.hadoop.fs.s3a.secret.key", "YOUR_SECRET_KEY")
        .getOrCreate()
    )
    return spark