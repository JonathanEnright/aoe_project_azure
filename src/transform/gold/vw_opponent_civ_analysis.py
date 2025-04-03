"""
Script to export gold data to ADLS2 parquet files for reading into Streamlit.
This is simply to save costs, in production Streamlit would read from Databricks iteself.
"""

import gzip
import io
import os

from pyspark.sql.functions import (
    asc,
    col,
    countDistinct,
    current_timestamp,
    round,
    sum,
    when,
)

from src.common.base_utils import create_adls2_session, create_databricks_session
from src.common.env_setting import EnvConfig
from src.common.load_utils import upload_to_adls2
from src.common.logging_config import setup_logging
from src.common.transform_utils import read_source_data

logger = setup_logging()

fact_table = f"{EnvConfig.CATALOG_NAME}.gold.fact_player_matches_gd"
match_table = f"{EnvConfig.CATALOG_NAME}.gold.dim_match_gd"
civ_table = f"{EnvConfig.CATALOG_NAME}.gold.dim_civ_gd"

STORAGE_ACCOUNT = "jonoaoedlext"
CONTAINER = EnvConfig.ENV_NAME
DATA_ENVIRONMENT = "consumption"
TARGET_TABLE = "vw_opponent_civ_analysis"


spark = create_databricks_session(catalog=EnvConfig.CATALOG_NAME, schema="gold")


def transform_dataframe(fact_table, match_table, civ_table):
    f1 = read_source_data(spark, fact_table)
    f2 = read_source_data(spark, fact_table)
    dm = read_source_data(spark, match_table)
    c1 = read_source_data(spark, civ_table)
    c2 = read_source_data(spark, civ_table)

    df = (
        f1.join(f2, f1.match_fk == f2.match_fk, "inner")
        .join(dm, f1.match_fk == dm.match_pk, "inner")
        .join(c1, f1.civ_fk == c1.civ_pk, "inner")
        .join(c2, f2.civ_fk == c2.civ_pk, "inner")
        .filter(c1.civ_name != c2.civ_name)
        .select(
            c1.civ_name.alias("civ"),
            c2.civ_name.alias("opponent_civ"),
            dm.map.alias("map"),
            when(dm.avg_elo <= 600, "600 or less")
            .when(dm.avg_elo <= 800, "600-800")
            .when(dm.avg_elo <= 1000, "800-1000")
            .when(dm.avg_elo <= 1200, "1000-1200")
            .when(dm.avg_elo <= 1400, "1200-1400")
            .when(dm.avg_elo <= 1600, "1400-1600")
            .when(dm.avg_elo <= 1800, "1600-1800")
            .when(dm.avg_elo > 1800, "1801 or more")
            .otherwise("unknown")
            .alias("match_elo_bucket"),
            f1.match_fk,
            f1.winner,
        )
    )
    trans_df = (
        df.groupBy("civ", "opponent_civ", "map", "match_elo_bucket")
        .agg(
            countDistinct("match_fk").alias("matches_played"),
            sum(when(col("winner") == True, 1).otherwise(0)).alias("wins"),
            round(
                when(countDistinct("match_fk") == 0, 0).otherwise(
                    sum(when(col("winner") == True, 1).otherwise(0))
                    / countDistinct("match_fk")
                    * 100
                ),
                2,
            ).alias("win_percentage"),
        )
        .orderBy(asc("civ"), asc("opponent_civ"), asc("map"), asc("match_elo_bucket"))
    )

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

    trans_df = transform_dataframe(fact_table, match_table, civ_table)

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
