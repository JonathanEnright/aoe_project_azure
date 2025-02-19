import yaml
from datetime import datetime, timedelta
import requests
import os
from typing import Dict, Optional, BinaryIO
import io
import time
import logging
# from import_secrets import *
from azure.identity import ClientSecretCredential
from azure.storage.filedatalake import DataLakeServiceClient
from databricks.connect.session import DatabricksSession
from databricks.sdk.core import Config as DBX_Config
from pyspark.sql.functions import col, input_file_name, split, lit, to_date, current_timestamp
from pyspark.sql.types import StringType
from pyspark.sql import functions as F

logger = logging.getLogger(__name__)

# Get the Azure SDK logger
azure_logger = logging.getLogger("azure")

# Set the logging level to WARNING to reduce verboseness
azure_logger.setLevel(logging.WARNING) 

# # Load environment variables from databricks secrets
# from pyspark.sql import SparkSession
# from pyspark.dbutils import DBUtils

# spark = SparkSession.builder.getOrCreate()  # dbutils requires a SparkSession to be created
# dbutils = DBUtils(spark)

# # Load environment variables from Databricks secrets
# os.environ["AWS_ACCESS_KEY"] = dbutils.secrets.get("aoe-scope", "AWS_ACCESS_KEY")
# os.environ["AWS_SECRET_ACCESS_KEY"] = dbutils.secrets.get("aoe-scope", "AWS_SECRET_ACCESS_KEY")
# os.environ["AWS_REGION"] = dbutils.secrets.get("aoe-scope", "AWS_REGION")


# -----------------------------------------------------------------------------
# Classes
# -----------------------------------------------------------------------------


class Config:
    def __init__(self, yaml_file: str):
        with open(yaml_file, "r") as f:
            self.config_dict = yaml.safe_load(f)

        # Set defaults from the parent configuration
        parent_defaults = self.config_dict.get('set_defaults', {})
        self.run_date = self.parse_date(
            parent_defaults.get('backdate_days_start'), 
            parent_defaults.get('target_run_date'), 
            parent_defaults.get('date_format')
        )
        self.run_end_date = self.parse_date(
            parent_defaults.get('backdate_days_end'), 
            parent_defaults.get('target_run_end_date'), 
            parent_defaults.get('date_format')
        )
    @staticmethod
    def parse_date(backdate_days: int, specific_date: str, date_format: str):
        """Creates a date object on initialisation. If target_run_date is specified,
        it takes priority, otherwise uses a number of backdated days from current date.
        """
        if specific_date:
            result = datetime.strptime(specific_date, date_format).date()
        else:
            result = (datetime.now() - timedelta(days=backdate_days)).date()
        return result

class Datasource:
    def __init__(self, name, config): 
        self.name = name
        datasource_config = config.config_dict['datasource'].get(name, {})
        
        # Dynamically set all attributes from the configuration
        for key, value in datasource_config.items():
            setattr(self, key, value)

        # Access attributes from the Config object
        self.run_date = config.run_date
        self.run_end_date = config.run_end_date


# -----------------------------------------------------------------------------
# Functions
# -----------------------------------------------------------------------------


def fetch_api_file(
    base_url: str, endpoint: str, params: Optional[Dict] = None
) -> BinaryIO | None:
    """Fetches a file from an API endpoint and returns it as a BytesIO object."""
    try:
        url = base_url + endpoint
        response = requests.get(url, params=params)
        response.raise_for_status()
        if not response.content:
            logger.warning("Received empty response from API.")
            return None
        content = io.BytesIO(response.content)
        return content
    except requests.RequestException as e:
        logger.error(f"Error fetching data: {e}")
        return None


def fetch_api_json(base_url: str, endpoint: str, params: dict) -> dict | None:
    """Fetches JSON data from an API endpoint with retry logic."""
    try:
        url = base_url + endpoint
        response = requests.get(url, params=params)
        response.raise_for_status()
        return response.json()
    except requests.RequestException as e:
        logger.warning(f"API request failed: {e}. Retrying...")
        raise



def create_adls2_session(adls2=None):
    if adls2 == None:
        logger.info("Authenticating to ADLS2.")
        adls2 = ClientSecretCredential(
            tenant_id=os.getenv("AZURE_TENANT_ID"),
            client_id=os.getenv("AZURE_CLIENT_ID"),
            client_secret=os.getenv("AZURE_CLIENT_SECRET")
        )
    return adls2

def upload_to_adls2(adls2, data, storage_account, container, file_path):
    try:
        adls2_client = DataLakeServiceClient(
            account_url=f'https://{storage_account}.dfs.core.windows.net',
            credential=adls2
        )
        file_system_client = adls2_client.get_file_system_client(file_system=container)
        file_client = file_system_client.get_file_client(file_path)
        file_client.upload_data(data, overwrite=True)
        logger.info(
            f"File '{container}/{file_path}' uploaded to ADLS2 storage_account '{storage_account}' successfully!"
        )
    except Exception as e:
        logger.error(f"Error uploading file: {e}")


def timer(func):
    """A simple timer decorator to record how long a function took to run."""

    def wrapper(*args, **kwargs):
        start_time = time.time()
        result = func(*args, **kwargs)
        end_time = time.time()
        elapsed_time = end_time - start_time
        logger.info(
            f"Function '{func.__name__}' took {elapsed_time:.1f} seconds to run."
        )
        return result

    return wrapper


def connect_to_databricks(
    catalog: str | None = os.getenv("DATABRICKS_CATALOG"),
    schema: str | None = os.getenv("DATABRICKS_SCHEMA"),
    ) -> DatabricksSession:
    """
    Uses environment variables (store in .env) to connect to Databricks.
    Optional: to specify catalog or schema.
    Returns: A databricks pyspark session object.
    """
    
    profile=os.getenv("DATABRICKS_PROFILE")
    cluster_id=os.getenv("DATABRICKS_CLUSTER_ID")
    dbx_session = DBX_Config(profile=profile, cluster_id=cluster_id)
    spark = DatabricksSession.builder.sdkConfig(dbx_session).getOrCreate()
    try:
        spark.catalog.setCurrentCatalog(catalog)
        spark.catalog.setCurrentDatabase(schema)
    except Exception as e:
        logger.warning(f"Could not set catalog/schema: {str(e)}")
    logger.info(f"Successfully connected to Databricks ('{catalog}.{schema}')")
    return spark


def load_yaml_data(yaml_file, yaml_key):
    with open(yaml_file, "r") as f:
        ext_config = yaml.safe_load(f)
    cfg = ext_config.get(yaml_key)
    logger.info(f"Successfully loaded '{yaml_key}' dict values from '{yaml_file}'!")
    return cfg


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