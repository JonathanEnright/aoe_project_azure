from common.base_utils import create_databricks_session
from common.transform_utils import read_source_data, apply_target_schema, write_to_table, deduplicate_by_key
from common.logging_config import setup_logging
import os
from pyspark.sql.functions import col, explode, to_timestamp
from pyspark.sql.types import (
    StructType, StructField, IntegerType, StringType, TimestampType
)

logger = setup_logging()

# Define table names and spark context
SOURCE_TABLE = "bronze.relic_br"
TARGET_TABLE = "silver.leaderboards_sr"
spark = create_databricks_session(catalog='aoe_dev', schema='bronze')

def define_target_schema():
    schema = StructType([
        StructField("disputes", IntegerType()),
        StructField("drops", IntegerType()),
        StructField("highestrank", IntegerType()),
        StructField("highestranklevel", IntegerType()),
        StructField("highestrating", IntegerType()),
        StructField("last_match_date", TimestampType()),
        StructField("leaderboard_id", IntegerType()),
        StructField("losses", IntegerType()),
        StructField("rank", IntegerType()),
        StructField("ranklevel", IntegerType()),
        StructField("ranktotal", IntegerType()),
        StructField("rating", IntegerType()),
        StructField("regionrank", IntegerType()),
        StructField("regionranktotal", IntegerType()),
        StructField("statgroup_id", IntegerType()),
        StructField("streak", IntegerType()),
        StructField("wins", IntegerType()),
        StructField("ldts", TimestampType()),
        StructField("source", StringType())
    ])

    return schema


def transform_dataframe(df):
    logger.info(f"Adding in transformation fields")

    flattened_df = df.select(
        explode("leaderboardStats").alias("lstats"),
        "ldts",
        "source"
    ).select(
        col("lstats.*"),
        to_timestamp(col("lastmatchdate")).alias("last_match_date"),
        "ldts",
        "source"
    ).distinct()

    trans_df = deduplicate_by_key(flattened_df, 'statgroup_id', 'ldts')
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
