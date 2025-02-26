import pandas as pd
from datetime import datetime
from common.base_utils import create_databricks_session
from common.transform_utils import apply_target_schema, write_to_table
from common.logging_config import setup_logging
import os
from pyspark.sql.types import (
    StructType, StructField, IntegerType, BooleanType, DateType
)

logger = setup_logging()
START_YEAR = 2020
END_YEAR = 2029
TARGET_TABLE = "bronze.dim_date_br"
spark = create_databricks_session(catalog='aoe_dev', schema='bronze')


def define_target_schema():
    schema = StructType([
        StructField("date", DateType()),
        StructField("year", IntegerType()),
        StructField("month", IntegerType()),
        StructField("day", IntegerType()),
        StructField("day_of_week", IntegerType()),
        StructField("is_weekend", BooleanType())
    ])
    return schema


def generate_dim_date(spark):
    """Generates a date dimension table"""

    # Define the start and end dates for the dimension table
    start_date = datetime(START_YEAR, 1, 1)
    end_date = datetime(END_YEAR, 12, 31)

    # Generate a date range
    date_range = pd.date_range(start=start_date, end=end_date, freq="D")

    dim_date_df = pd.DataFrame(
        {
            "date": date_range,
            "year": date_range.year,
            "month": date_range.month,
            "day": date_range.day,
            "day_of_week": date_range.dayofweek,  # 0=Monday, 6=Sunday
        }
    )

    dim_date_df["is_weekend"] = dim_date_df["day_of_week"].isin([5, 6])

    # Convert all column names to lowercase
    dim_date_df = dim_date_df.rename(columns=str.lower)
    spark_df = spark.createDataFrame(dim_date_df)

    return spark_df

def main():
    """
    Main ETL workflow:
      1. Define target schema.
      2. Generate calendar from pandas.
      3. Enforce target schema.
      4. Write the final data to the target table.
    """
    target_schema = define_target_schema()
    df = generate_dim_date(spark)
    final_df = apply_target_schema(df, target_schema, spark)
    write_to_table(final_df, TARGET_TABLE)
    logger.info(f"Script '{os.path.basename(__file__)}' complete.")
        

if __name__ == "__main__":
    main()