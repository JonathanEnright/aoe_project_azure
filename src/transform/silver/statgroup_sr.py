import os

from pyspark.sql.functions import col, explode
from pyspark.sql.types import (
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from src.common.base_utils import create_databricks_session
from src.common.logging_config import setup_logging
from src.common.transform_utils import (
    apply_target_schema,
    deduplicate_by_key,
    read_source_data,
    write_to_table,
)

logger = setup_logging()

# Define table names and spark context
SOURCE_TABLE = "bronze.relic_br"
TARGET_TABLE = "silver.statgroup_sr"
spark = create_databricks_session(catalog="aoe_dev", schema="bronze")


def define_target_schema():
    schema = StructType(
        [
            StructField("id", IntegerType()),
            StructField("group_name", StringType()),
            StructField("type", IntegerType()),
            StructField("alias", StringType()),
            StructField("country", StringType()),
            StructField("leaderboardregion_id", IntegerType()),
            StructField("level", IntegerType()),
            StructField("name", StringType()),
            StructField("personal_statgroup_id", IntegerType()),
            StructField("profile_id", IntegerType()),
            StructField("xp", IntegerType()),
            StructField("ldts", TimestampType()),
            StructField("source", StringType()),
        ]
    )

    return schema


def transform_dataframe(df):
    logger.info("Adding in transformation fields")

    flattened_df = (
        df.select(explode("statGroups").alias("gstats"), "ldts", "source")
        .select(
            col("gstats.id"),
            col("gstats.name").alias("group_name"),
            col("gstats.type"),
            explode("gstats.members").alias("m"),
            "ldts",
            "source",
        )
        .select(
            "id",
            "group_name",
            "type",
            col("m.alias"),
            col("m.country"),
            col("m.leaderboardregion_id"),
            col("m.level"),
            col("m.name"),
            col("m.personal_statgroup_id"),
            col("m.profile_id"),
            col("m.xp"),
            "ldts",
            "source",
        )
        .distinct()
    )

    trans_df = deduplicate_by_key(flattened_df, "profile_id", "ldts")
    return trans_df


def main():
    """
    Main ETL workflow:
      1. Define target schema.
      2. Read source data.
      3. Apply transformations.
      4. Enforce target schema.
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
