# src/main_pipeline.py (Integration of Cleaning Step)

from connections.spark_session import get_spark_session
from src.ingestion.data_ingestor import ingest_raw_data
from src.cleaning.data_cleaner import clean_data # <--- NEW IMPORT

def run_pipeline():
    """Main function to orchestrate the Spark data pipeline."""
    # ... (header printing) ...
    
    spark = None
    try:
        spark = get_spark_session()
        print("\n✅ Step 1: Spark Session Initialized.")
        
        # 2. Data Ingestion
        print("\n⏳ Step 2: Starting Data Ingestion...")
        raw_df = ingest_raw_data(spark)
        print(f"✅ Ingestion successful. DataFrame has {raw_df.count()} records.")
        
        # 3. Data Cleaning <--- NEW STEP
        print("\n⏳ Step 3: Starting Data Cleaning...")
        cleaned_df = clean_data(raw_df) 
        print(f"✅ Data Cleaning complete. Final count: {cleaned_df.count()} records.")
        
        # --- PHASE 3: Analysis ---
        # ... (future steps will use cleaned_df) ...

    except Exception as e:
        # ... (error handling) ...
        
    finally:
        # ... (spark stop) ...

if __name__ == "__main__":
    run_pipeline()