# src/main_pipeline.py

from connections.spark_session import get_spark_session
from src.ingestion.data_ingestor import ingest_raw_data

def run_pipeline():
    """
    Main function to orchestrate the Spark data pipeline.
    
    The steps are:
    1. Initialize SparkSession securely.
    2. Ingest data (Download from S3 to local disk, then read with Spark).
    3. (Placeholder for Cleaning, Analysis, and Forecasting).
    4. Stop SparkSession.
    """
    print("==============================================")
    print("🚀 Starting Capstone Sales Forecasting Pipeline")
    print("==============================================")
    
    # 1. Initialize Spark Session
    spark = None
    try:
        spark = get_spark_session()
        print("\n✅ Step 1: Spark Session Initialized.")
        
        # 2. Data Ingestion (Downloads file to local_cache/data and reads it)
        print("\n⏳ Step 2: Starting Data Ingestion...")
        raw_df = ingest_raw_data(spark)
        print(f"✅ Ingestion successful. DataFrame has {raw_df.count()} records.")
        raw_df.printSchema()
        
        # 3. Placeholder for future steps
        
        # --- PHASE 2: Cleaning ---
        # from src.cleaning.data_cleaner import clean_data
        # cleaned_df = clean_data(raw_df) 
        # print("✅ Step 3: Data Cleaning complete.")
        
        # --- PHASE 3: Analysis ---
        # from src.analysis.sales_analyzer import analyze_data
        # metrics_df = analyze_data(cleaned_df)
        # print("✅ Step 4: Data Analysis complete.")
        
        # --- PHASE 4: Forecasting and Storage ---
        # from src.forecasting.sales_forecaster import generate_forecast
        # forecast_df = generate_forecast(metrics_df)
        # print("✅ Step 5: Forecasting complete.")
        
        # --- Final Storage (using s3_connector.py) ---
        # from connections.s3_connector import write_data_to_s3
        # from config import FORECAST_OUTPUT_PATH, OUTPUT_FILE_FORMAT, OUTPUT_FILE_OPTIONS
        # write_data_to_s3(forecast_df, FORECAST_OUTPUT_PATH, OUTPUT_FILE_FORMAT, **OUTPUT_FILE_OPTIONS)
        # print(f"✅ Step 6: Final Forecast stored to S3 at {FORECAST_OUTPUT_PATH}")

    except Exception as e:
        print(f"\n❌ PIPELINE FAILED: Encountered a critical error.")
        print(f"Error details: {e}")
        
    finally:
        # 4. Stop Spark Session
        if spark:
            spark.stop()
            print("\n==============================================")
            print("🛑 Spark Session Stopped. Pipeline Concluded.")
            print("==============================================")

if __name__ == "__main__":
    run_pipeline()