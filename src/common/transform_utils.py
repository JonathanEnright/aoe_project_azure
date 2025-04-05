import logging

from delta.tables import DeltaTable
from pyspark.sql import functions as F
from pyspark.sql.functions import (
    col,
    current_timestamp,
    input_file_name,
    lit,
    split,
    to_date,
)
from pyspark.sql.types import StringType
from pyspark.sql.window import Window

from src.transform.master_date import master_run_date, master_run_end_date

logger = logging.getLogger(__name__)


def add_metadata_columns(spark, cfg, df):

    # Add filename
    df = df.withColumn("fn", split(input_file_name(), cfg["location"])[1])

    # Extract file_date if specified
    if cfg["file_date"]:
        df = df.withColumn("file_date", to_date(split(col("fn"), "\\.")[0]))
        df = df.filter((col('file_date') >= master_run_date) & (col('file_date') <= master_run_end_date))

    # Record source of data
    df = df.withColumn("source", lit(cfg["source"]).cast(StringType()))

    # Remove duplicates
    df = df.dropDuplicates()

    # Load date time stamp
    df = df.withColumn("ldts", current_timestamp())

    # Add filter condition to reduce number of days of data stored (i.e. last 45 days)
    # TODO: Add filter condition function call

    return df


def read_source_data(spark, table_name):
    """
    Reads data from the specified table in the current Unity Catalog context.

    Args:
        spark (SparkSession): The active Spark session.
        table_name (str): The name of the table to read.

    Returns:
        DataFrame: The DataFrame containing the data from the specified table.
    """
    try:
        # Get the current Unity Catalog context (catalog and schema)
        context_query = "SELECT current_catalog() || '.' || current_schema() AS context"
        context = spark.sql(context_query).first()["context"]

        # Log the operation
        logger.info(f"Reading data from '{context}.{table_name}'.")

        # Read the table
        df = spark.read.table(table_name)
        logger.info(f"Successfully read data from '{context}.{table_name}'.")
        return df

    except Exception as e:
        logger.error(f"Failed to read data from table '{table_name}': {str(e)}")
        raise


def apply_target_schema(df, target_schema, spark):
    # Enable ANSI mode to enforce an error on incorrect field casts:
    spark.conf.set("spark.sql.ansi.enabled", "true")

    logger.info("Casting and ordering fields to match the target schema.")
    final_df = df.select(
        [F.col(field.name).cast(field.dataType) for field in target_schema.fields]
    )
    return final_df


def write_to_table(df, table_name, mode="overwrite", partition_col=None):
    logger.info(f"Writing to target table '{table_name}' with mode '{mode}'.")
    try:
        writer = df.write.format("delta").mode(mode).option("overwriteSchema", "true")
        if partition_col:
            writer = writer.partitionBy(partition_col)
        writer.saveAsTable(table_name)
        logger.info(f"Table '{table_name}' now created/updated.")
    except Exception as e:
        logger.error(f"Error updating Table '{table_name}' - {e}.")


def deduplicate_by_key(df, pk, date_field):
    window_spec = Window.partitionBy(pk).orderBy(col(date_field).desc())
    deduped_df = (
        df.withColumn("row_num", F.row_number().over(window_spec))
        .filter(F.col("row_num") == 1)
        .drop("row_num")
    )

    return deduped_df


def upsert_to_table(spark, df, table_name, pk, partition_col=None):
    """
    Performs an upsert on target table based on primary key.
    Source dataframe must match schema of target table.
    If target table does not exist, creates one based on source dataframe.
    """

    # Check if the target table exists using Spark's catalog.
    if not spark.catalog.tableExists(table_name):
        logger.info(f"Table '{table_name}' does not exist. Creating table.")
        write_to_table(df, table_name, partition_col=partition_col)
    else:
        logger.info(
            f"Table '{table_name}' exists. Performing upsert merge on primary key '{pk}'."
        )

        # Get the DeltaTable object for the target table.
        deltaTable = DeltaTable.forName(spark, table_name)

        # Execute the merge:
        #  - when matched, update all columns with the source data.
        #  - when not matched, insert all columns.
        merge_result = (
            deltaTable.alias("tgt")
            .merge(source=df.alias("src"), condition=f"tgt.{pk} = src.{pk}")
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )

        # Check if merge_result is None (table creation)
        if merge_result is not None:
            # Extract metrics from the returned DataFrame (it's a single-row DataFrame)
            metrics = merge_result.first().asDict()
            print(metrics)
            # Extract relevant metrics
            num_inserted = metrics.get("num_inserted_rows", 0)
            num_updated = metrics.get("num_updated_rows", 0)
            logger.info(f"Table '{table_name}' has been upserted (merge completed).")
            logger.info(
                f"Upsert Metrics - Inserted: {num_inserted}, Updated: {num_updated}"
            )
        else:
            # Handle the case where the table was just created (no actual merge)
            # We get the count of the dataframe, which is the same number inserted
            num_inserted = df.count()
            num_updated = 0
            logger.info(f"Table '{table_name}' has been upserted (first insert).")
            logger.info(
                f"Upsert Metrics - Inserted: {num_inserted}, Updated: {num_updated}"
            )
