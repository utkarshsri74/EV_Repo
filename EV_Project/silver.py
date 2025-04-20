from pyspark.sql import DataFrame
from src.silver_layer.data_tfm import flatten_json, load_allowed_columns
from src.utils import get_spark_session, validate_schema
from src.silver_layer.dq_checks import run_quality_checks
from src.silver_layer.scd_tfm import apply_scd2_logic
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def run_silver_layer_pipeline(raw_path: str, processed_path: str, metadata_path: str, approvals_path: str):
    """
    Run the silver layer pipeline: read raw JSON, flatten, validate, run DQ checks, and write to processed layer.

    Args:
        raw_path: Path to raw JSON file
        processed_path: Path for processed vehicle data (Parquet)
        metadata_path: Path for metadata (Parquet)
        approvals_path: Path for approvals data (Parquet)
    """
    try:
        # Initialize Spark session
        spark = get_spark_session()
        logger.info("Spark session initialized")

        # Read JSON
        logger.info(f"Reading JSON from {raw_path}")
        df = spark.read.option("multiline", "true").json(raw_path)

        # Load allowed columns from config
        allowed_columns = load_allowed_columns(logger=logger)

        # Flatten JSON (vehicle data, metadata, approvals)
        logger.info("Flattening JSON data")
        vehicle_df, metadata_df, approvals_df = flatten_json(df, allowed_columns, spark, logger)

        # Validate schema
        logger.info("Validating schema")
        is_valid = validate_schema(vehicle_df, allowed_columns)
        if not is_valid:
            logger.warning("Schema validation failed, but proceeding with pipeline")

        # Run data quality checks
        logger.info("Running data quality checks")
        critical_columns = ["VIN (1-10)", "Make", "Model"]
        numeric_columns = {
            "Electric Range": {"min": 0, "max": 1000},
            "Model Year": {"min": 2000, "max": 2025},
            "Base MSRP": {"min": 0, "max": 200000}
        }
        is_valid = run_quality_checks(
            spark=spark,
            df=vehicle_df,
            critical_columns=critical_columns,
            numeric_columns=numeric_columns,
            logger=logger
        )
        if not is_valid:
            logger.warning("Data quality checks failed, but proceeding with pipeline")

        # Write vehicle data to processed layer
        # logger.info(f"Writing vehicle data to {processed_path}")
        # vehicle_df.write.mode("overwrite").parquet(processed_path)
        # print(f"processed path is {processed_path}")
        # vehicle_df.show(10)
        # Commented out lines 62 - 63 to replace with below write block

        # Apply SCD Type 2 and get the final dataframe
        logger.info("Applying SCD Type 2 logic to processed layer")
        final_vehicle_df = apply_scd2_logic(spark, vehicle_df, path=processed_path)

        # Write final data to parquet
        logger.info(f"Writing final vehicle data to {processed_path}")
        final_vehicle_df.write.mode("overwrite").parquet(processed_path)
        # SCD Operation ends here

        # Write metadata to processed layer
        logger.info(f"Writing metadata to {metadata_path}")
        metadata_df.write.mode("overwrite").parquet(metadata_path)

        # Write approvals to processed layer
        logger.info(f"Writing approvals to {approvals_path}")
        approvals_df.write.mode("overwrite").parquet(approvals_path)

        logger.info("Silver layer pipeline completed successfully")

    except Exception as e:
        logger.error(f"Silver layer pipeline failed: {e}")
        raise
    # finally:
    #     if 'spark' in locals():
    #         spark.stop()
    #         logger.info("Spark session stopped")

if __name__ == "__main__":
    from src.constants import RAW_PATH, PROCESSED_PATH

    METADATA_PATH = PROCESSED_PATH.replace("ev_data2.parquet", "metadata.parquet")
    APPROVALS_PATH = PROCESSED_PATH.replace("ev_data2.parquet", "approvals.parquet")

    run_silver_layer_pipeline(RAW_PATH, PROCESSED_PATH, METADATA_PATH, APPROVALS_PATH)
