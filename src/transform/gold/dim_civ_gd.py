import os

from pyspark.sql.functions import col, current_date, lit, md5

from src.common.base_utils import create_databricks_session
from src.common.dq_rules import DQRules
from src.common.env_setting import EnvConfig
from src.common.logging_config import setup_logging
from src.common.transform_utils import read_source_data, upsert_to_table

logger = setup_logging()

# Define table names and spark context
SOURCE_TABLE = "silver.players_s2r"
TARGET_TABLE = "gold.dim_civ_gd"
spark = create_databricks_session(catalog=EnvConfig.CATALOG_NAME, schema="silver")

pk = "civ_pk"


def transform_dataframe(df):
    logger.info("Adding in transformation fields")
    base_df = (
        df.withColumn("civ_pk", md5(col("civ_name").cast("string")))
        .select(
            col("civ_pk"),
            col("civ_name"),
            lit("").alias("civ_weaknesses"),
            lit("").alias("civ_strengths"),
        )
        .dropDuplicates()
    )

    trans_df = base_df.withColumn("load_date", current_date())

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
