from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, explode, from_unixtime, to_date  # changed here for epoch
import json
import logging
from typing import List, Tuple, Dict


def setup_logger(name: str = __name__, level: int = logging.INFO) -> logging.Logger:
    """Set up and return a logger instance."""
    logger = logging.getLogger(name)
    if not logger.hasHandlers():
        logging.basicConfig(level=level)
    return logger


def load_allowed_columns(config_path: str = "config.json", logger: logging.Logger = None) -> List[str]:
    """Load allowed columns from configuration file."""
    logger = logger or setup_logger()
    try:
        with open(config_path, 'r') as f:
            config = json.load(f)
        return config.get("allowed_columns", [])
    except Exception as e:
        logger.error(f"Failed to load config: {e}")
        return []


def get_column_mapping(df: DataFrame, logger: logging.Logger) -> Dict[str, str]:
    """Extract column mapping from meta.view.columns."""
    try:
        # Select and alias the nested column to a flat name
        meta_columns_df = df.selectExpr("meta.view.columns as columns").filter("columns IS NOT NULL")
        
        if meta_columns_df.head(1):
            logger.info("meta.view.columns found and not null")
            meta_columns = meta_columns_df.collect()[0]["columns"]
        else:
            logger.error("meta.view.columns is missing or empty")
            raise ValueError("Invalid JSON structure: missing or empty meta.view.columns")

    except Exception as e:
        logger.error(f"Failed to extract meta.view.columns: {e}")
        raise ValueError("Invalid JSON structure: 'meta.view.columns' could not be extracted")

    # Build mapping from fieldName → name if field is not hidden
    mapping = {
        col_entry["fieldName"]: col_entry["name"]
        for col_entry in meta_columns
        if "fieldName" in col_entry
    }

    return mapping, meta_columns


def get_column_lists(mapping: Dict[str, str], meta_columns: List[dict], allowed_columns: List[str], logger: logging.Logger) -> List[Tuple[int, str]]:
    """Identify and filter allowed columns from metadata."""
    available_columns = list(mapping.values())

    new_columns = [col for col in available_columns if col not in allowed_columns]
    missing_columns = [col for col in allowed_columns if col not in available_columns]

    if new_columns:
        logger.warning(f"New columns detected: {new_columns}. Including only allowed columns.")
    if missing_columns:
        logger.warning(f"Missing expected columns: {missing_columns}")

    return [
        (i, col_entry["name"])
        for i, col_entry in enumerate(meta_columns)
        if "fieldName" in col_entry and col_entry["name"] in allowed_columns and col_entry["name"] in available_columns
    ]


def extract_vehicle_data(df: DataFrame, select_columns: List[Tuple[int, str]]) -> DataFrame:
    """Explode 'data' array and select allowed columns."""
    if "data" not in df.columns:
        raise ValueError("Invalid JSON structure: missing data array")

    exploded_df = df.select(explode(col("data")).alias("record"))

    select_expr = [col("record")[i].alias(name) for i, name in select_columns]

    return exploded_df.select(select_expr) if select_expr else exploded_df.select()


def extract_metadata(df: DataFrame) -> DataFrame:
    """Extract metadata from JSON structure."""
    metadata_fields = {
        "id": "dataset_id",
        "name": "dataset_name",
        "description": "description",
        "owner.displayName": "owner_name",
        "tags": "tags",
        "createdAt": "created_at",
        "publicationDate": "publication_date"
    }

    metadata_select = []
    for json_path, alias in metadata_fields.items():
        if df.selectExpr(f"meta.view.{json_path}").first():
            metadata_select.append(col(f"meta.view.{json_path}").alias(alias))

    return df.select(*metadata_select) if metadata_select else df.select()


def extract_approvals(df: DataFrame, spark: SparkSession, logger: logging.Logger) -> DataFrame:
    """Extract approvals section if present."""
    if df.selectExpr("meta.view.approvals").first():
        return df.select(explode(col("meta.view.approvals")).alias("approval")).select(
            col("approval.reviewedAt").alias("reviewed_at"),
            col("approval.reviewedAutomatically").alias("reviewed_automatically"),
            col("approval.state").alias("state"),
            col("approval.submissionId").alias("submission_id"),
            col("approval.submissionDetails.permissionType").alias("permission_type"),
            col("approval.submitter.displayName").alias("submitter_name"),
            col("approval.submissionOutcomeApplication.status").alias("outcome_status"),
            col("approval.submittedAt").alias("submitted_at")
        )
    else:
        logger.warning("No approvals data found, creating empty DataFrame")
        return spark.createDataFrame([], schema="reviewed_at string, reviewed_automatically boolean, state string, submission_id string, permission_type string, submitter_name string, outcome_status string, submitted_at string")

def convert_epoch_to_date(df: DataFrame, columns: List[str], logger: logging.Logger = None) -> DataFrame:
    """
    Convert specified epoch timestamp columns to date format (yyyy-MM-dd).  # changed here for epoch

    Args:
        df: Input DataFrame
        columns: List of column names containing epoch timestamps
        logger: Optional logger

    Returns:
        DataFrame with converted date columns
    """
    logger = logger or setup_logger()
    for col_name in columns:
        if col_name in df.columns:
            logger.info(f"Converting epoch column '{col_name}' to date format")  # changed here for epoch
            df = df.withColumn(col_name, to_date(from_unixtime(col(col_name).cast("long"))))  # changed here for epoch
        else:
            logger.warning(f"Column '{col_name}' not found in DataFrame, skipping conversion")  # changed here for epoch

    return df  # changed here for epoch

def flatten_json(
    df: DataFrame,
    allowed_columns: List[str],
    spark: SparkSession,
    logger: logging.Logger = None
) -> Tuple[DataFrame, DataFrame, DataFrame]:
    """
    Flatten JSON data, metadata, and approvals based on allowed_columns.
    Returns:
        - vehicle_df: Flattened vehicle data
        - metadata_df: Metadata fields
        - approvals_df: Approvals data
    """
    logger = logger or setup_logger()

    column_mapping, meta_columns = get_column_mapping(df, logger)
    select_columns = get_column_lists(column_mapping, meta_columns, allowed_columns, logger)

    vehicle_df = extract_vehicle_data(df, select_columns)
    metadata_df = extract_metadata(df)
    approvals_df = extract_approvals(df, spark, logger)

    vehicle_df = convert_epoch_to_date(vehicle_df, ["created_at", "updated_at"], logger)  # changed here for epoch
    vehicle_df.show(10)

    return vehicle_df, metadata_df, approvals_df

