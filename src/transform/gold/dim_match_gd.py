import os

from pyspark.sql.functions import col, concat_ws, current_date, md5

from src.common.base_utils import create_databricks_session
from src.common.dq_rules import DQRules
from src.common.env_setting import EnvConfig
from src.common.logging_config import setup_logging
from src.common.transform_utils import read_source_data, upsert_to_table

logger = setup_logging()

# Define table names and spark context
SOURCE_TABLE = "silver.matches_s2r"
TARGET_TABLE = "gold.dim_match_gd"
spark = create_databricks_session(catalog=EnvConfig.CATALOG_NAME, schema="silver")
pk = "match_pk"


def transform_dataframe(df):
    logger.info("Matches: applying transformation steps.")

    trans_df = df.withColumn(
        "match_pk",
        md5(
            concat_ws("~", col("game_id").cast("string"), col("source").cast("string"))
        ),
    ).select(
        col("match_pk"),
        col("game_id"),
        col("map"),
        col("avg_elo"),
        col("game_duration_secs"),
        col("actual_duration_secs"),
        col("game_started_timestamp"),
        col("game_date"),
        col("team_0_elo"),
        col("team_1_elo"),
        col("leaderboard"),
        col("mirror"),
        col("patch"),
        current_date().alias("load_date"),
        col("source"),
        col("file_date"),
    )
    return trans_df


def dq_checks(df):
    logger.info("Running data quality checks")
    DQRules.check_nulls(df, [pk])
    DQRules.check_unique(df, [pk])
    DQRules.check_non_empty(df)
    logger.info("All dq rules passed!")


def main():
    """
    Main ETL workflow:
      1. Read source data.
      2. Apply transformations.
      3. Pass DQ checks
      4. Write the final data to the target table.
    """
    df = read_source_data(spark, SOURCE_TABLE)
    trans_df = transform_dataframe(df)
    dq_checks(trans_df)
    upsert_to_table(spark, trans_df, TARGET_TABLE, pk, partition_col=None)
    logger.info(f"Script '{os.path.basename(__file__)}' complete.")


if __name__ == "__main__":
    main()
