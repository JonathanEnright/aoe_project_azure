import os

from pyspark.sql.functions import coalesce, col, lit

from src.common.base_utils import create_databricks_session
from src.common.env_setting import EnvConfig
from src.common.logging_config import setup_logging
from src.common.transform_utils import read_source_data, write_to_table

logger = setup_logging()

# Define table names and spark context
stat_group_data = "silver.statgroup_sr"
leaderboard_data = "silver.leaderboards_sr"
country_codes = "bronze.country_list_br"

TARGET_TABLE = "silver.player_leaderboard_stats_s2r"
spark = create_databricks_session(catalog=EnvConfig.CATALOG_NAME, schema="silver")


def transform_dataframe(stat_group_data, leaderboard_data, country_codes):
    logger.info("Adding in transformation fields")

    stat_group_data = read_source_data(spark, stat_group_data)
    stat_group_data_df = stat_group_data.select(
        col("profile_id"),
        col("alias").alias("gaming_name"),
        col("personal_statgroup_id"),
        col("country"),
    )

    country_codes = read_source_data(spark, country_codes)
    country_codes_df = country_codes.select(
        col("Country").alias("country_name"), col("Country_code").alias("country_cd")
    )

    leaderboard_data = read_source_data(spark, leaderboard_data)
    leaderboard_data_df = leaderboard_data.select(
        col("statgroup_id"),
        col("wins"),
        col("losses"),
        col("rank").alias("current_rank"),
        col("rating").alias("current_rating"),
        col("last_match_date"),
        col("ldts"),
        col("source"),
    )

    trans_df = (
        leaderboard_data_df.alias("ld")
        .join(
            stat_group_data_df.alias("sgd"),
            col("ld.statgroup_id") == col("sgd.personal_statgroup_id"),
            "inner",
        )
        .join(
            country_codes_df.alias("cc"),
            col("sgd.country") == col("cc.country_cd"),
            "left_outer",
        )
        .select(
            col("sgd.profile_id"),
            col("sgd.gaming_name"),
            col("sgd.country").alias("country_code"),
            coalesce(col("cc.country_name"), lit("Unknown")).alias("country_name"),
            col("ld.statgroup_id"),
            col("ld.wins"),
            col("ld.losses"),
            col("ld.current_rank"),
            col("ld.current_rating"),
            col("ld.last_match_date"),
            col("ld.ldts"),
            col("ld.source"),
        )
    )

    return trans_df


def main():
    """
    Main ETL workflow:
      1. Apply transformations.
      2. Write the final data to the target table.
    """
    trans_df = transform_dataframe(stat_group_data, leaderboard_data, country_codes)
    write_to_table(trans_df, TARGET_TABLE)
    logger.info(f"Script '{os.path.basename(__file__)}' complete.")


if __name__ == "__main__":
    main()
