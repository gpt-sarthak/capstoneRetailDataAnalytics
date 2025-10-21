# src/main_pipeline.py (CORRECTED Error Handling and Finalization)
import logging
# Suppress noisy logging from the Boto3/Urllib3 libraries
logging.getLogger('boto3').setLevel(logging.ERROR)
logging.getLogger('botocore').setLevel(logging.ERROR)
logging.getLogger('urllib3').setLevel(logging.ERROR)


from connections.spark_session import get_spark_session
from src.ingestion.data_ingestor import ingest_raw_data
from src.cleaning.data_cleaner import clean_data
from src.cleaning.data_breakdown import break_data_into_tables 

def run_pipeline():
    """
    Main function to orchestrate the Spark data pipeline.
    Ensures graceful error handling and resource cleanup.
    """
    print("==============================================")
    print("🚀 Starting Capstone Sales Forecasting Pipeline")
    print("==============================================")
    
    spark = None
    try:
        a = 1
        # 1. Initialize Spark Session
        spark = get_spark_session()
        print("\n✅ Step 1: Spark Session Initialized.")
        a+=1
        # 2. Data Ingestion (Download and Local Read)
        print("\n⏳ Step 2: Starting Data Ingestion...")
        raw_df = ingest_raw_data(spark)
        a+=1
        print(f"✅ Ingestion successful. DataFrame has {raw_df.count()} records.")
        
        # 3. Data Cleaning
        print("\n⏳ Step 3: Starting Data Cleaning...")
        pre_cleaned_df = clean_data(raw_df) 
        a+=1
        print(f"✅ Data Cleaning (Pre-breakdown) complete. Final count: {pre_cleaned_df.count()} records.")
        
        # 4. Data Breakdown (Normalization and Local Write)
        print("\n⏳ Step 4: Starting Data Normalization and Breakdown...")
        extracted_tables = break_data_into_tables(pre_cleaned_df)
        a+=1
        print(f"✅ Data Breakdown complete. {len(extracted_tables)} tables created locally.")
        
        # --- PHASE 5: Cleaning of Individual Tables (Next Step) ---
        
    except Exception as e:
        # 🛑 CRITICAL: Log the error and fail gracefully
        print(f"\n❌ PIPELINE FAILED: Encountered a critical error.")
        print(f"Error details: {e}")
        # Re-raise the exception to ensure the failure is visible
        raise 
        
    finally:
        # 🛑 CRITICAL: Ensure the Spark session is always stopped to release resources
        if spark:
            spark.stop()
            print("\n==============================================")
            print("🛑 Spark Session Stopped. Pipeline Concluded.")
            print("==============================================")
        print(str(a)+" error dekho")

if __name__ == "__main__":
    run_pipeline()