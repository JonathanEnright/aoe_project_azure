"""
Script to export gold data to ADLS2 parquet files for reading into Streamlit.
This is simply to save costs, in production Streamlit would read from Databricks iteself.
"""

import gzip
import io
import os

from pyspark.sql.functions import asc, col, current_timestamp, round

from src.common.base_utils import create_adls2_session, create_databricks_session
from src.common.load_utils import upload_to_adls2
from src.common.logging_config import setup_logging
from src.common.transform_utils import read_source_data

logger = setup_logging()

player_table = "aoe_dev.gold.dim_player_gd"

STORAGE_ACCOUNT = "jonoaoedlext"
CONTAINER = "dev"
DATA_ENVIRONMENT = "consumption"
TARGET_TABLE = "vw_leaderboard_analysis"


spark = create_databricks_session(catalog="aoe_dev", schema="gold")


def transform_dataframe(player_table):
    df = read_source_data(spark, player_table)

    trans_df = df.select(
        df.gaming_name.alias("player_name"),
        df.current_rank.alias("rank"),
        df.current_rating.alias("rating"),
        df.country_name.alias("country"),
        round((col("wins") / (col("wins") + col("losses")) * 100), 2).alias(
            "win_percentage"
        ),
        (col("wins") + col("losses")).alias("total_matches"),
        col("wins").alias("wins"),
        col("losses").alias("losses"),
        df.last_match_date.alias("last_played"),
    ).orderBy(asc("rank"))

    # Load date time stamp
    trans_df = trans_df.withColumn("ldts", current_timestamp())

    return trans_df


def main():
    """
    Main ETL workflow:
      1. Apply transformations.
      2. Write the final data to the target table.
    """
    adls2 = create_adls2_session()

    file_name = f"{TARGET_TABLE}.csv.gz"

    trans_df = transform_dataframe(player_table)

    # Convert the Spark DataFrame to a Pandas DataFrame
    pandas_df = trans_df.toPandas()

    # Write the Pandas DataFrame to a BytesIO buffer as a compressed CSV
    with io.BytesIO() as buffer:
        with gzip.GzipFile(fileobj=buffer, mode="w") as gzip_buffer:
            pandas_df.to_csv(gzip_buffer, index=False)
        buffer.seek(0)

        # Upload the compressed CSV buffer to ADLS Gen2
        upload_to_adls2(
            adls2, buffer, STORAGE_ACCOUNT, CONTAINER, f"{DATA_ENVIRONMENT}/{file_name}"
        )

    logger.info(
        f"Data for '{TARGET_TABLE}' now uploaded into {CONTAINER}/{DATA_ENVIRONMENT} as a compressed CSV file!"
    )

    logger.info(f"Script '{os.path.basename(__file__)}' complete.")


if __name__ == "__main__":
    main()
