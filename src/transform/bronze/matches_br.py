from pathlib import Path
import sys

parent_dir = Path(__file__).resolve().parent.parent.parent.parent
sys.path.append(str(parent_dir))
from utils import connect_to_databricks, read_source_data, apply_target_schema, write_to_table
import logging
import os

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


from pyspark.sql import functions as F
from pyspark.sql.functions import col, to_timestamp, to_date
from pyspark.sql.types import (
    StructType, StructField, DecimalType, IntegerType, StringType, 
    BooleanType, TimestampType, DateType
)

spark = connect_to_databricks(catalog='aoe_dev', schema='bronze')


def define_target_schema():
    schema = StructType([
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
        StructField("source", IntegerType()),
        StructField("file_date", DateType())
    ])
    return schema


def transform_dataframe(df):
    logger.info(f"Adding in transformation fields")

    # Conversion constant for nanoseconds to seconds.
    NANOS_TO_SECS = 1_000_000_000
    
    trans_df = (
        df.withColumn("game_duration_secs", col("duration") / NANOS_TO_SECS)
          .withColumn("actual_duration_secs", col("irl_duration") / NANOS_TO_SECS)
          .withColumn("ts_seconds", col("started_timestamp") / float(NANOS_TO_SECS))
          .withColumn("game_started_timestamp", to_timestamp(col("ts_seconds")))
          .withColumn("game_date", to_date(col("game_started_timestamp")))
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
    # Define table names
    source_table = "matches_raw"
    target_table = "bronze.matches_br"
    
    target_schema = define_target_schema()
    df = read_source_data(spark, source_table)
    trans_df = transform_dataframe(df)
    final_df = apply_target_schema(trans_df, target_schema)
    write_to_table(final_df, target_table)
    logger.info(f"Script '{os.path.basename(__file__)}' complete.")
    

if __name__ == "__main__":
    main()
    print('Done')