# src/main_pipeline.py (Integration of Breakdown Step)

from connections.spark_session import get_spark_session
from src.ingestion.data_ingestor import ingest_raw_data
from src.cleaning.data_cleaner import clean_data
from src.cleaning.data_breakdown import break_data_into_tables # <--- NEW IMPORT

def run_pipeline():
    # ...
    try:
        spark = get_spark_session()
        print("\n✅ Step 1: Spark Session Initialized.")
        
        # 2. Data Ingestion
        print("\n⏳ Step 2: Starting Data Ingestion...")
        raw_df = ingest_raw_data(spark)
        print(f"✅ Ingestion successful. DataFrame has {raw_df.count()} records.")
        
        # 3. Data Cleaning (Pre-breakdown cleaning for consistent schema)
        print("\n⏳ Step 3: Starting Data Cleaning...")
        pre_cleaned_df = clean_data(raw_df) 
        print(f"✅ Data Cleaning (Pre-breakdown) complete. Final count: {pre_cleaned_df.count()} records.")
        
        # 4. Data Breakdown (Normalization) <--- NEW STEP
        print("\n⏳ Step 4: Starting Data Normalization and Breakdown...")
        # Note: This function saves tables locally as a side effect
        extracted_tables = break_data_into_tables(pre_cleaned_df)
        print(f"✅ Data Breakdown complete. {len(extracted_tables)} tables created locally.")
        
        # --- PHASE 5: Cleaning of Individual Tables (Next Step) ---
        
        # ... (future steps will load from local processed tables) ...

    except Exception as e:
        # ...
    finally:
        # ...

if __name__ == "__main__":
    run_pipeline()