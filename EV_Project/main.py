# main.py
from src.utils import get_spark_session, validate_schema
from src.data_tfm import flatten_json, clean_data
from src.dq_checks import run_quality_checks
from src.curated_tfm import curate_data  # Import curated transformation
from src.constants import *
import logging
import os

# Configure logging to display in the console
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s", handlers=[logging.StreamHandler()])
logger = logging.getLogger(__name__)

# Set SPARK_VERSION environment variable
os.environ["SPARK_VERSION"] = "3.5"

def run_silver_layer_pipeline(spark):
    """Orchestrate the Silver layer pipeline."""
    try:
        # Read raw JSON from ADLS
        logger.info(f"Reading JSON from {RAW_PATH}")
        print(f"Reading JSON from {RAW_PATH}")
        raw_df = spark.read.option("multiline", "true").json(RAW_PATH)
        
        # Flatten JSON
        flattened_df = flatten_json(raw_df)
        
        # Validate schema
        validate_schema(flattened_df, EXPECTED_COLUMNS)
        
        # Clean data
        cleaned_df = clean_data(flattened_df)
        
        # Perform custom quality checks
        critical_columns = ["VIN (1-10)", "Make", "Model"]
        numeric_columns = {
            "Electric Range": {"min": 0, "max": 1000},
            "Model Year": {"min": 2000, "max": 2025},
            "Base MSRP": {"min": 0, "max": 200000}
        }
        is_valid = run_quality_checks(spark, cleaned_df, critical_columns, numeric_columns)
        if not is_valid:
            logger.warning("Data quality issues detected, proceeding with caution.")
            print("Warning: Data quality issues detected, proceeding with caution.")
        
        # Write to processed layer
        logger.info(f"Writing cleaned data to {PROCESSED_PATH}")
        print(f"Writing cleaned data to {PROCESSED_PATH}")
        cleaned_df.write.mode("overwrite").parquet(PROCESSED_PATH)
        
        logger.info("Silver layer pipeline completed successfully.")
        print("Silver layer pipeline completed successfully.")
    except Exception as e:
        logger.error(f"Pipeline failed: {str(e)}")
        print(f"Error: Pipeline failed: {str(e)}")
        raise

def run_curated_layer_pipeline(spark):
    """Orchestrate the Curated layer pipeline."""
    try:
        curate_data(spark, PROCESSED_PATH, CURATED_PATH)
    except Exception as e:
        logger.error(f"Curated layer pipeline failed: {str(e)}")
        print(f"Error: Curated layer pipeline failed: {str(e)}")
        raise

def main():
    """Main function to run both silver and curated layers."""
    # Initialize Spark session
    try:
        logger.info("Attempting to initialize Spark session...")
        spark = get_spark_session()
        logger.info("Spark session initialized successfully with cluster ADLS configuration.")
        print("Spark session initialized successfully with cluster ADLS configuration.")
    except Exception as e:
        logger.error(f"Failed to initialize Spark session: {str(e)}")
        print(f"Error: Failed to initialize Spark session: {str(e)}")
        raise
        
    # Run silver layer pipeline
    run_silver_layer_pipeline(spark)
    
    # Run curated layer pipeline
    run_curated_layer_pipeline(spark)

if __name__ == "__main__":
    main()