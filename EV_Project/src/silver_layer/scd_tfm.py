from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, current_date, lit
from pyspark.sql.utils import AnalysisException
from src.constants import PROCESSED_PATH, KEY_COLUMN, CREATED_AT_COL, UPDATED_AT_COL

def apply_scd2_logic(
    spark: SparkSession,
    source_df: DataFrame,
    path: str = PROCESSED_PATH
) -> DataFrame:
    """
    Apply SCD Type 2 logic with historical tracking on Parquet data.
    Returns the final DataFrame that should be written to the processed path.
    """
    try:
        try:
            target_df = spark.read.parquet(path)
            print("Loaded existing target data")
        except AnalysisException:
            target_df = None
            print("No existing target data found — initializing SCD2 structure")

        new_df = source_df \
            .withColumn("is_current", lit(True)) \
            .withColumn("valid_from", current_date()) \
            .withColumn("valid_to", lit(None).cast("date"))

        if target_df is None:
            return new_df

        expected_cols = {"is_current", "valid_from", "valid_to"}
        missing_cols = expected_cols - set(target_df.columns)

        if missing_cols:
            print(f"Target data missing columns: {missing_cols}. Adding them...")
            for col_name in missing_cols:
                if col_name == "is_current":
                    target_df = target_df.withColumn(col_name, lit(True))
                elif col_name == "valid_from":
                    target_df = target_df.withColumn(col_name, current_date())
                elif col_name == "valid_to":
                    target_df = target_df.withColumn(col_name, lit(None).cast("date"))

        current_target = target_df.filter(col("is_current") == True)

        join_expr = source_df[KEY_COLUMN] == current_target[KEY_COLUMN]
        joined_df = source_df.alias("src").join(current_target.alias("tgt"), join_expr, "left")

        change_filter = (
            (col(f"tgt.{KEY_COLUMN}").isNull()) |
            (col(f"src.{CREATED_AT_COL}") > col(f"tgt.{CREATED_AT_COL}")) |
            (col(f"src.{UPDATED_AT_COL}") > col(f"tgt.{UPDATED_AT_COL}"))
        )

        new_records = joined_df.filter(change_filter).select("src.*")

        if new_records.count() == 0:
            print("No new or changed records detected.")
            return target_df

        new_df_with_tracking = new_records \
            .withColumn("is_current", lit(True)) \
            .withColumn("valid_from", current_date()) \
            .withColumn("valid_to", lit(None).cast("date"))

        keys_to_update = new_records.select(KEY_COLUMN).distinct()
        updated_old_df = current_target.join(keys_to_update, KEY_COLUMN, "inner") \
            .withColumn("is_current", lit(False)) \
            .withColumn("valid_to", current_date())

        unchanged_df = current_target.join(keys_to_update, KEY_COLUMN, "left_anti")

        final_df = unchanged_df.unionByName(updated_old_df).unionByName(new_df_with_tracking)
        return final_df

    except Exception as e:
        print(f"Error during SCD2 processing: {e}")
        raise
