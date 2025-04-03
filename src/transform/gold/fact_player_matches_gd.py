import os

from pyspark.sql.functions import col, current_date

from src.common.base_utils import create_databricks_session
from src.common.dq_rules import DQRules
from src.common.logging_config import setup_logging
from src.common.transform_utils import read_source_data, upsert_to_table

logger = setup_logging()

# Define table names and spark context
player_match_sr = "silver.players_s2r"
dim_civ = "gold.dim_civ_gd"
dim_match = "gold.dim_match_gd"
dim_player = "gold.dim_player_gd"
dim_date = "gold.dim_date_gd"

TARGET_TABLE = "gold.fact_player_matches_gd"
spark = create_databricks_session(catalog="aoe_dev", schema="gold")

pk = "fact_pk"


def transform_dataframe(player_match_sr, dim_civ, dim_match, dim_player, dim_date):
    logger.info("Adding in transformation fields")

    player_match_sr_df = read_source_data(spark, player_match_sr)
    dim_civ_df = read_source_data(spark, dim_civ)
    dim_match_df = read_source_data(spark, dim_match)
    dim_player_df = read_source_data(spark, dim_player)
    dim_date_df = read_source_data(spark, dim_date)

    trans_df = (
        player_match_sr_df.alias("pm")
        .join(dim_civ_df.alias("dc"), col("pm.civ_name") == col("dc.civ_name"), "inner")
        .join(dim_match_df.alias("dm"), col("pm.game_id") == col("dm.game_id"), "inner")
        .join(
            dim_player_df.alias("dp"),
            col("pm.profile_id") == col("dp.profile_id"),
            "inner",
        )
        .join(dim_date_df.alias("dd"), col("dm.game_date") == col("dd.date"), "inner")
        .select(
            col("pm.id").alias("fact_pk"),
            col("dp.player_pk").alias("player_fk"),
            col("dm.match_pk").alias("match_fk"),
            col("dc.civ_pk").alias("civ_fk"),
            col("dd.date_pk").alias("date_fk"),
            col("pm.team"),
            col("pm.winner"),
            col("pm.match_rating_diff"),
            col("pm.new_rating"),
            col("pm.old_rating"),
            col("pm.source"),
            col("pm.file_date"),
            current_date().alias("load_date"),
        )
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
      1. Apply transformations.
      2. Pass DQ checks
      3. Write the final data to the target table.
    """
    trans_df = transform_dataframe(
        player_match_sr, dim_civ, dim_match, dim_player, dim_date
    )
    dq_checks(trans_df)
    upsert_to_table(spark, trans_df, TARGET_TABLE, pk, partition_col=None)
    # write_to_table(trans_df, TARGET_TABLE)
    logger.info(f"Script '{os.path.basename(__file__)}' complete.")


if __name__ == "__main__":
    main()
