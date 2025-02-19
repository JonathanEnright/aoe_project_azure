
from pyspark.sql.functions import col, input_file_name, split, lit, to_date, current_timestamp
from pyspark.sql.types import StringType
from pyspark.sql import functions as F
import logging

logger = logging.getLogger(__name__)


def create_external_table(spark, cfg):
    tbl = cfg['table']

    # Configure Spark to handle Parquet timestamps correctly
    spark.conf.set("spark.sql.legacy.parquet.nanosAsLong", "true")

    # If table in Unity Catalogue, refresh metadata to read latest files
    if spark.catalog.tableExists(tbl):
        spark.catalog.refreshTable(tbl)
        logger.info(f"External Table '{tbl}' exists, metadata now refreshed!")

    # Else, register as new External table in Databricks Unity Catalogue
    else:
        create_table_sql = f"""
            CREATE TABLE {tbl}
            USING {cfg['format']}
            OPTIONS ( {cfg['options']} )
            LOCATION "{cfg['location']}"
        """
        spark.sql(create_table_sql)

        logger.info(f"External Table '{tbl}' successfully created!")
    return spark, cfg
    

def add_metadata_columns(spark, cfg):
    
    # Use DataFrame API to read the table
    df = spark.read.table(f"{cfg['table']}")

    # Add filename
    df = df.withColumn("fn", split(input_file_name(), cfg['location'])[1])
    
    # Extract file_date if specified
    if cfg['file_date']:
        df = df.withColumn("file_date", to_date(split(col("fn"), '\\.')[0]))

    # Record source of data
    df = df.withColumn("source", lit(cfg['source']).cast(StringType()))

    # Load date time stamp
    df = df.withColumn("ldts", current_timestamp())

    # Remove duplicates
    df = df.dropDuplicates()   

    # Add filter condition
    # TODO: Add filter condition function call

    # Create or replace the managed table in Unity Catalog
    df.write.format("delta").mode("overwrite").saveAsTable(f"{cfg['managed_table']}")
    
    logger.info(f"Managed Table '{cfg['managed_table']}' successfully created!")


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

def apply_target_schema(df, target_schema):
    logger.info(f"Casting and ordering fields to match the target schema.")
    final_df = df.select([
        F.col(field.name).cast(field.dataType)
        for field in target_schema.fields
    ])
    return final_df

def write_to_table(df, table_name, mode="overwrite", partition_col=None):
    logger.info(f"Writing to target table '{table_name}' with mode '{mode}'.")
    try:
        writer = df.write.format("delta").mode(mode)
        if partition_col:
            writer = writer.partitionBy(partition_col)
        writer.saveAsTable(table_name)
        logger.info(f"Table '{table_name}' now updated.")
    except Exception as e:
        logger.error(f"Error updating Table '{table_name}' - {e}.")