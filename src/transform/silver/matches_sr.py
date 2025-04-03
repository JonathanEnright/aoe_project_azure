import os

from pyspark.sql.functions import col, to_date, to_timestamp
from pyspark.sql.types import (
    BooleanType,
    DateType,
    DecimalType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from src.common.base_utils import create_databricks_session
from src.common.logging_config import setup_logging
from src.common.transform_utils import (
    apply_target_schema,
    read_source_data,
    write_to_table,
)

logger = setup_logging()

# Define table names and spark context
SOURCE_TABLE = "bronze.matches_br"
TARGET_TABLE = "silver.matches_sr"
spark = create_databricks_session(catalog="aoe_dev", schema="bronze")


def define_target_schema():
    schema = StructType(
        [
            StructField("avg_elo", DecimalType(38, 2)),
            StructField("game_duration_secs", IntegerType()),
            StructField("game_id", IntegerType()),
            StructField("game_speed", StringType()),
            StructField("game_type", StringType()),
            StructField("actual_duration_secs", IntegerType()),
            StructField("leaderboard", StringType()),
            StructField("map", StringType()),
            StructField("mirror", BooleanType()),
            StructField("num_players", IntegerType()),
            StructField("patch", StringType()),
            StructField("raw_match_type", StringType()),
            StructField("replay_enhanced", BooleanType()),
            StructField("game_started_timestamp", TimestampType()),
            StructField("game_date", DateType()),
            StructField("starting_age", StringType()),
            StructField("team_0_elo", IntegerType()),
            StructField("team_1_elo", IntegerType()),
            StructField("ldts", TimestampType()),
            StructField("source", StringType()),
            StructField("file_date", DateType()),
        ]
    )
    return schema


def transform_dataframe(df):
    logger.info("Adding in transformation fields")

    # Conversion constant for nanoseconds to seconds.
    NANOS_TO_SECS = 1_000_000_000

    trans_df = (
        df.withColumn("game_duration_secs", col("duration") / NANOS_TO_SECS)
        .withColumn("actual_duration_secs", col("irl_duration") / NANOS_TO_SECS)
        .withColumn("ts_seconds", col("started_timestamp") / float(NANOS_TO_SECS))
        .withColumn("game_started_timestamp", to_timestamp(col("ts_seconds")))
        .withColumn("game_date", to_date(col("game_started_timestamp")))
        .filter(col("leaderboard") == "random_map")
    )
    return trans_df


def main():
    """
    Main ETL workflow:
      1. Define target schema.
      2. Read source data.
      3. Apply transformations.
      4. Enforce target schema.
      5. Write the final data to the target table.
    """
    target_schema = define_target_schema()
    df = read_source_data(spark, SOURCE_TABLE)
    trans_df = transform_dataframe(df)
    final_df = apply_target_schema(trans_df, target_schema, spark)
    write_to_table(final_df, TARGET_TABLE)
    logger.info(f"Script '{os.path.basename(__file__)}' complete.")


if __name__ == "__main__":
    main()
