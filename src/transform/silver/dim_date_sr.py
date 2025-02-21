from common.base_utils import create_databricks_session
from common.transform_utils import read_source_data, upsert_to_table
from common.logging_config import setup_logging
import os
from pyspark.sql.functions import date_format, col

logger = setup_logging()

# Define table names and spark context
SOURCE_TABLE = "bronze.dim_date_br"
TARGET_TABLE = "silver.dim_date_sr"
spark = create_databricks_session(catalog='aoe_dev', schema='bronze')

pk = "date_pk"

def transform_dataframe(df):
    logger.info(f"Adding in transformation fields")
    trans_df = df.withColumn("date_pk", date_format(col("date"), "yyyyMMdd").cast("int"))
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
