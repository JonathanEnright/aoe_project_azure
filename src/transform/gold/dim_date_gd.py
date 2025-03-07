from src.common.base_utils import create_databricks_session
from src.common.transform_utils import read_source_data, upsert_to_table
from src.common.logging_config import setup_logging
import os
from pyspark.sql.functions import current_date, col, md5, concat_ws

logger = setup_logging()

# Define table names and spark context
SOURCE_TABLE = "silver.dim_date_sr" 
TARGET_TABLE = "gold.dim_date_gd"
spark = create_databricks_session(catalog='aoe_dev', schema='silver')
pk = 'date_pk'

def transform_dataframe(df):
    logger.info("Dim Date: applying transformation steps.")

    # Assuming df is your input DataFrame
    trans_df = df.select(col("date_pk"),
        col("date"),
        col("year"),
        col("month"),
        col("day"),
        col("day_of_week"),
        col("is_weekend"),
        current_date().alias("load_date")
    )
    return trans_df


def main():
    """
    Main ETL workflow:
      1. Read source data.
      2. Apply transformations.
      3. Write the final data to the target table.
    """
    df = read_source_data(spark, SOURCE_TABLE)
    trans_df = transform_dataframe(df)
    upsert_to_table(spark, trans_df, TARGET_TABLE, pk, partition_col=None)
    logger.info(f"Script '{os.path.basename(__file__)}' complete.")


if __name__ == "__main__":
    main()