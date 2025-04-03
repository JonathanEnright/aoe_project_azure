import os

from src.common.base_utils import create_databricks_session
from src.common.dq_rules import DQRules
from src.common.env_setting import EnvConfig
from src.common.logging_config import setup_logging
from src.common.transform_utils import read_source_data, upsert_to_table

logger = setup_logging()

# Define table names and spark context
SOURCE_TABLE = "silver.matches_sr"
TARGET_TABLE = "silver.matches_s2r"
spark = create_databricks_session(catalog=EnvConfig.CATALOG_NAME, schema="bronze")

pk = "game_id"


def transform_dataframe(df):
    logger.info("Adding in transformation fields")

    # Placeholder function. No transformation required for this table.
    trans_df = df
    return trans_df

def dq_checks(df):
    logger.info("Running data quality checks")
    DQRules.check_nulls(df, [pk])
    DQRules.check_unique(df, [pk])
    DQRules.check_valid_range(df, "game_date", '2021-01-01', '2026-01-01')
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
