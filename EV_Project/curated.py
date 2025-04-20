# curated.py
from src.utils import get_spark_session
from src.curated_layer.curated_tfm import curate_data
from src.constants import PROCESSED_PATH, CURATED_PATH, CURATED_AGG_PATH
import logging
import os

# Configure logging to display in the console
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s", handlers=[logging.StreamHandler()])
logger = logging.getLogger(__name__)

# Set SPARK_VERSION environment variable
os.environ["SPARK_VERSION"] = "3.5"

def run_curated_layer_pipeline():
    """Orchestrate the Curated layer pipeline."""
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

    try:
        curate_data(spark, PROCESSED_PATH, CURATED_PATH, CURATED_AGG_PATH)
    except Exception as e:
        logger.error(f"Curated layer pipeline failed: {str(e)}")
        print(f"Error: Curated layer pipeline failed: {str(e)}")
        raise

if __name__ == "__main__":
    run_curated_layer_pipeline()