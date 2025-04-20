from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, countDistinct
import logging
from typing import List, Dict, Optional
from pyspark.sql.types import NumericType

def setup_logger(name: str = __name__, level: int = logging.INFO) -> logging.Logger:
    """Set up and return a logger instance."""
    logger = logging.getLogger(name)
    if not logger.hasHandlers():
        logging.basicConfig(level=level)
    return logger


def check_completeness(df: DataFrame, critical_columns: List[str], logger: logging.Logger) -> bool:
    """Check for nulls in critical columns."""
    all_passed = True
    for col_name in critical_columns:
        null_count = df.filter(col(col_name).isNull()).count()
        if null_count > 0:
            logger.warning(f"Completeness check failed for {col_name}: {null_count} null values found.")
            print(f"Warning: Completeness check failed for {col_name}: {null_count} null values found.")
            all_passed = False
        else:
            logger.info(f"Completeness check passed for {col_name}: No null values.")
            print(f"Info: Completeness check passed for {col_name}: No null values.")
    return all_passed


# def check_range(df: DataFrame, numeric_columns: Dict[str, Dict[str, float]], logger: logging.Logger) -> bool:
#     """Check that numeric columns fall within allowed min/max range."""
#     all_passed = True
#     for col_name, constraints in numeric_columns.items():
#         min_val = df.agg({col_name: "min"}).collect()[0][0]
#         max_val = df.agg({col_name: "max"}).collect()[0][0]
#         if min_val is not None and max_val is not None:
#             if min_val < constraints["min"] or max_val > constraints["max"]:
#                 logger.warning(
#                     f"Range check failed for {col_name}: Min={min_val}, Max={max_val}, "
#                     f"Expected [{constraints['min']}, {constraints['max']}]"
#                 )
#                 print(
#                     f"Warning: Range check failed for {col_name}: Min={min_val}, Max={max_val}, "
#                     f"Expected [{constraints['min']}, {constraints['max']}]"
#                 )
#                 all_passed = False
#             else:
#                 logger.info(
#                     f"Range check passed for {col_name}: Min={min_val}, Max={max_val} within "
#                     f"[{constraints['min']}, {constraints['max']}]"
#                 )
#                 print(
#                     f"Info: Range check passed for {col_name}: Min={min_val}, Max={max_val} within "
#                     f"[{constraints['min']}, {constraints['max']}]"
#                 )
#         else:
#             logger.warning(f"Range check skipped for {col_name}: Column contains null values.")
#             print(f"Warning: Range check skipped for {col_name}: Column contains null values.")
#             all_passed = False
#     return all_passed

from pyspark.sql.types import NumericType

def check_range(df: DataFrame, numeric_columns: Dict[str, Dict[str, float]], logger: logging.Logger) -> bool:
    """Check that numeric columns fall within allowed min/max range."""
    all_passed = True
    for col_name, constraints in numeric_columns.items():
        try:
            # Validate the column is a numeric type
            col_dtype = dict(df.dtypes).get(col_name)
            if col_dtype not in ("int", "bigint", "double", "float", "decimal", "long"):
                logger.warning(f"Skipping range check for {col_name}: Non-numeric type '{col_dtype}'")
                print(f"Warning: Skipping range check for {col_name}: Non-numeric type '{col_dtype}'")
                all_passed = False
                continue

            min_val = df.agg({col_name: "min"}).collect()[0][0]
            max_val = df.agg({col_name: "max"}).collect()[0][0]

            if min_val is not None and max_val is not None:
                if float(min_val) < constraints["min"] or float(max_val) > constraints["max"]:
                    logger.warning(
                        f"Range check failed for {col_name}: Min={min_val}, Max={max_val}, "
                        f"Expected [{constraints['min']}, {constraints['max']}]"
                    )
                    print(
                        f"Warning: Range check failed for {col_name}: Min={min_val}, Max={max_val}, "
                        f"Expected [{constraints['min']}, {constraints['max']}]"
                    )
                    all_passed = False
                else:
                    logger.info(
                        f"Range check passed for {col_name}: Min={min_val}, Max={max_val} within "
                        f"[{constraints['min']}, {constraints['max']}]"
                    )
                    print(
                        f"Info: Range check passed for {col_name}: Min={min_val}, Max={max_val} within "
                        f"[{constraints['min']}, {constraints['max']}]"
                    )
            else:
                logger.warning(f"Range check skipped for {col_name}: Column contains null values.")
                print(f"Warning: Range check skipped for {col_name}: Column contains null values.")
                all_passed = False

        except Exception as e:
            logger.error(f"Error during range check for column {col_name}: {e}")
            print(f"Error: Range check failed for column {col_name}: {e}")
            all_passed = False

    return all_passed


def check_uniqueness(df: DataFrame, unique_column: str, logger: logging.Logger) -> bool:
    """Ensure that a column has unique values."""
    total_rows = df.count()
    unique_count = df.select(countDistinct(unique_column)).collect()[0][0]
    if unique_count != total_rows:
        logger.warning(f"Uniqueness check failed for {unique_column}: {total_rows - unique_count} duplicate values found.")
        print(f"Warning: Uniqueness check failed for {unique_column}: {total_rows - unique_count} duplicate values found.")
        return False
    else:
        logger.info(f"Uniqueness check passed for {unique_column}: No duplicates found.")
        print(f"Info: Uniqueness check passed for {unique_column}: No duplicates found.")
        return True


def run_quality_checks(
    spark: SparkSession,
    df: DataFrame,
    critical_columns: Optional[List[str]] = None,
    numeric_columns: Optional[Dict[str, Dict[str, float]]] = None,
    logger: Optional[logging.Logger] = None
) -> bool:
    """
    Run all data quality checks: completeness, range, and uniqueness.

    Args:
        spark: SparkSession
        df: Input DataFrame
        critical_columns: List of column names to check for nulls
        numeric_columns: Dictionary of numeric columns and their min/max constraints
        logger: Optional logger instance

    Returns:
        True if all checks pass, False otherwise.
"""
    logger = logger or setup_logger()

    try:
        critical_columns = critical_columns or ["VIN (1-10)", "Make"]
        numeric_columns = numeric_columns or {
            "Electric Range": {"min": 0, "max": 1000},
            "Model Year": {"min": 2000, "max": 2025},
            "Base MSRP": {"min": 0, "max": 200000}
        }

        checks = [
            check_completeness(df, critical_columns, logger),
            check_range(df, numeric_columns, logger),
            check_uniqueness(df, "VIN (1-10)", logger)
        ]

        return all(checks)
    except Exception as e:
        logger.error(f"Error running custom quality checks: {str(e)}")
        print(f"Error: Error running custom quality checks: {str(e)}")
        raise
