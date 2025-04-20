from pyspark.sql import SparkSession, DataFrame
from typing import List
import logging

logger = logging.getLogger(__name__)

def get_spark_session(app_name: str = "EVDataPipeline") -> SparkSession:
    """Initialize and return a Spark session.
    
    Args:
        app_name: Name of the Spark application (default: "EVDataPipeline")
    
    Returns:
        SparkSession: The created or existing Spark session
    """
    try:
        spark = SparkSession.builder.appName(app_name).getOrCreate()
        logger.info("Spark session created successfully")
        return spark
    except Exception as e:
        logger.error(f"Failed to create Spark session: {e}")
        raise

def validate_schema(df: DataFrame, expected_columns: List[str]) -> bool:
    """
    Validate if DataFrame contains all expected columns.
    
    Args:
        df: Input DataFrame
        expected_columns: List of expected column names
    
    Returns:
        bool: True if schema is valid, False otherwise
    """
    if df is None or df.rdd.isEmpty():
        logger.error("Input DataFrame is None or empty")
        return False
    
    actual_columns = set(df.columns)
    expected_columns = set(expected_columns)
    
    missing_columns = expected_columns - actual_columns
    new_columns = actual_columns - expected_columns
    
    if missing_columns:
        logger.warning(f"Missing columns: {missing_columns}")
    if new_columns:
        logger.info(f"New columns detected: {new_columns}")
    
    return not missing_columns