import os

from pyspark.sql.functions import col, concat_ws, initcap, md5
from pyspark.sql.types import (
    BooleanType,
    DateType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

from src.common.base_utils import create_databricks_session
from src.common.logging_config import setup_logging
from src.common.transform_utils import (
    apply_target_schema,
    read_source_data,
    write_to_table,
)

logger = setup_logging()

# Define table names and spark context
SOURCE_TABLE = "bronze.players_br"
TARGET_TABLE = "silver.players_sr"
spark = create_databricks_session(catalog="aoe_dev", schema="bronze")


def define_target_schema():
    schema = StructType(
        [
            StructField("id", StringType(), False),
            StructField("game_id", IntegerType()),
            StructField("team", StringType()),
            StructField("profile_id", IntegerType()),
            StructField("civ_name", StringType()),
            StructField("winner", BooleanType()),
            StructField("match_rating_diff", IntegerType()),
            StructField("new_rating", IntegerType()),
            StructField("old_rating", IntegerType()),
            StructField("source", StringType()),
            StructField("file_date", DateType()),
        ]
    )
    return schema


def transform_dataframe(df):
    logger.info("Players: applying transformation steps.")
    filtered_df = df.filter(col("profile_id").isNotNull())

    # Compute ID using md5 of the concatenation of game_id and profile_id (with '~' separator).
    trans_df = filtered_df.withColumn(
        "id",
        md5(
            concat_ws(
                "~", col("game_id").cast("string"), col("profile_id").cast("string")
            )
        ),
    ).withColumn("civ_name", initcap(col("civ")))
    return trans_df


def main():
    """
    Main ETL workflow for players_sr:
      1. Define the target schema.
      2. Read source data.
      3. Apply transformations.
      4. Enforce the target schema (and type casts).
      5. Write the final data to the target table.
    """
    target_schema = define_target_schema()
    df = read_source_data(spark, SOURCE_TABLE)
    trans_df = transform_dataframe(df)
    final_df = apply_target_schema(trans_df, target_schema, spark)
    write_to_table(final_df, TARGET_TABLE)
    logger.info(f"Script '{os.path.basename(__file__)}' complete.")


if __name__ == "__main__":
    main()
