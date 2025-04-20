from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, year, current_date, round, avg, stddev, count
from pyspark.sql.types import IntegerType
import logging
from typing import Dict

def setup_logger(name: str = __name__, level: int = logging.INFO) -> logging.Logger:
    """Set up and return a logger instance."""
    logger = logging.getLogger(name)
    if not logger.hasHandlers():
        logging.basicConfig(level=level)
    return logger


def normalize_column_name(col_name: str) -> str:
    """Convert column name to lowercase and replace special characters with underscores."""
    return (col_name.lower()
            .replace(" - ","_")
            .replace(" ", "_")
            .replace("(", "")
            .replace(")", "")
            .replace("-", "_")
            .replace(".", "_"))


def normalize_dataframe_columns(df: DataFrame) -> DataFrame:
    """Return DataFrame with normalized column names."""
    return df.select([col(c).alias(normalize_column_name(c)) for c in df.columns])


def get_column_mapping() -> Dict[str, str]:
    """Return normalized column mapping for expected columns."""
    base_columns = [
        "make", "model", "model_year", "city", "county", "electric_vehicle_type",
        "state", "clean_alternative_fuel_vehicle_cafv_eligibility", "electric_utility",
        "electric_range", "base_msrp", "vehicle_count", "avg_electric_range",
        "stddev_electric_range", "avg_base_msrp", "vehicle_age","counties","congressional_districts","waofm_gis_legislative_district_boundary","created_at","updated_at"
    ]
    return {col: normalize_column_name(col) for col in base_columns}


def transform_to_curated_fact(df: DataFrame, column_mapping: Dict[str, str]) -> DataFrame:
    """Aggregate and transform normalized data into curated fact format."""
    grouped_df = df.groupBy(
        column_mapping["make"],
        column_mapping["model"],
        column_mapping["model_year"],
        column_mapping["city"],
        column_mapping["county"],
        column_mapping["electric_vehicle_type"],
        column_mapping["state"],
        column_mapping["clean_alternative_fuel_vehicle_cafv_eligibility"],
        column_mapping["electric_utility"],
        column_mapping["counties"],
        column_mapping["congressional_districts"],
        column_mapping["waofm_gis_legislative_district_boundary"],
        column_mapping["created_at"],
        column_mapping["updated_at"]        
    ).agg(
        count("*").alias(column_mapping["vehicle_count"]),
        round(avg(column_mapping["electric_range"]), 2).alias(column_mapping["avg_electric_range"]),
        round(stddev(column_mapping["electric_range"]), 2).alias(column_mapping["stddev_electric_range"]),
        round(avg(column_mapping["base_msrp"]), 2).alias(column_mapping["avg_base_msrp"])
    )

    final_df = grouped_df.withColumn(
        column_mapping["vehicle_age"],
        year(current_date()) - col(column_mapping["model_year"]).cast(IntegerType())
    ).filter(col(column_mapping["vehicle_age"]) >= 0)

    return final_df


def curate_data(spark: SparkSession, input_path: str, output_path: str, output_path_agg: str, logger: logging.Logger = None) -> None:
    """
    Transform silver data into curated fact table and write to ADLS.

    Args:
        spark: SparkSession object
        input_path: Path to input Parquet file
        output_path: Path to write curated Parquet file
        logger: Optional logger instance
    """
    logger = logger or setup_logger()
    try:
        logger.info(f"Reading silver layer from {input_path}")
        print(f"Reading silver layer from {input_path}")
        df = spark.read.parquet(input_path).filter("is_current=true")

        normalized_df = normalize_dataframe_columns(df)
        print("Scehma for df data")
        df.printSchema()
        print("Scehma for normalized_df data")
        normalized_df.printSchema()
        column_mapping = get_column_mapping()
        curated_df = transform_to_curated_fact(normalized_df, column_mapping)
        print("Scehma for curated_df data")
        curated_df.printSchema()

        logger.info(f"Writing curated data to {output_path}")
        print(f"Writing curated data to {output_path}")
#        normalized_df.write.mode("overwrite").parquet(output_path)
        normalized_df.drop("created_at","updated_at","vin_1_10","is_current","valid_from","valid_to").write.mode("overwrite").parquet(output_path)

        logger.info(f"Writing curated aggregated data to {output_path_agg}")
        print(f"Writing curated aggregated data to {output_path_agg}")
        curated_df.write.mode("overwrite").parquet(output_path_agg)

        logger.info("Curated layer pipeline completed successfully.")
        print("Curated layer pipeline completed successfully.")
    except Exception as e:
        logger.error(f"Error in curated layer pipeline: {str(e)}")
        print(f"Error: Error in curated layer pipeline: {str(e)}")
        raise
