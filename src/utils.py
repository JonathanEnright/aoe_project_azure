import yaml
from datetime import datetime, timedelta
import requests
import os
# import boto3
from typing import Dict, Optional, BinaryIO
import io
import time
import logging
# from import_secrets import *
from azure.identity import ClientSecretCredential
from azure.storage.filedatalake import DataLakeServiceClient
from databricks.connect.session import DatabricksSession as SparkSession
from databricks.sdk.core import Config as DBX_Config
from pyspark.sql import SparkSession

logger = logging.getLogger(__name__)

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
            self.__dict__.update(yaml.safe_load(f))
        self.run_date = self.parse_date(
            self.backdate_days_start, self.target_run_date, self.date_format
        )
        self.run_end_date = self.parse_date(
            self.backdate_days_end, self.target_run_end_date, self.date_format
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


# def create_s3_session(s3=None):
#     if s3 == None:
#         logger.info("Authenticating to S3.")
#         s3 = boto3.client(
#             "s3",
#             aws_access_key_id=os.getenv("AWS_ACCESS_KEY"),
#             aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
#             region_name=os.getenv("AWS_REGION"),
#         )
#     return s3


# def upload_to_s3(s3, data, bucket_name, path_key):
#     try:
#         s3.upload_fileobj(data, bucket_name, path_key)
#         logger.info(
#             f"File '{path_key}' uploaded to S3 bucket '{bucket_name}' successfully!"
#         )
#     except Exception as e:
#         logger.error(f"Error uploading file: {e}")


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
    ) -> SparkSession:
    """
    Uses environment variables (store in .env) to connect to Databricks.
    Optional: to specify catalog or schema.
    Returns: A databricks pyspark session object.
    """
    
    profile=os.getenv("DATABRICKS_PROFILE")
    cluster_id=os.getenv("DATABRICKS_CLUSTER_ID")
    dbx_session = DBX_Config(profile=profile, cluster_id=cluster_id)
    spark = SparkSession.builder.sdkConfig(dbx_session).getOrCreate()
    try:
        spark.catalog.setCurrentCatalog(catalog)
        spark.catalog.setCurrentDatabase(schema)
    except Exception as e:
        logger.warning(f"Could not set catalog/schema: {str(e)}")
    return spark


# def sf_connect(
#     db: str | None = os.getenv("SF_DATABASE"),
#     schema: str | None = os.getenv("SF_SCHEMA"),
# ) -> snowflake.connector.connection.SnowflakeConnection:
#     """
#     Uses environment variables (store in .env) to connect to Snowflake.
#     Optional: to specify database or schema.
#     Returns: A snowflake connection object.
#     """

#     logger.info("Connecting to Snowflake...")
#     connection_parameters = {
#         "account": os.getenv("SF_ACCOUNT_NAME"),
#         "user": os.getenv("SF_USERNAME"),
#         "password": os.getenv("SF_PASSWORD"),
#         "warehouse": os.getenv("SF_WAREHOUSE"),
#         "role": os.getenv("SF_ROLE"),
#         "database": db,
#         "schema": schema,
#     }
#     connection = snowflake.connector.connect(**connection_parameters)
#     logger.info("Connection successfully created")
#     return connection