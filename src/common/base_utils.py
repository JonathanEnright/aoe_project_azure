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
from databricks.connect.session import DatabricksSession
from databricks.sdk.core import Config as DBX_Config
from pyspark.sql import SparkSession


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


def timer(func):
    """A simple timer decorator to record how long a function took to run."""

    def wrapper(*args, **kwargs):
        start_time = time.perf_counter()
        result = func(*args, **kwargs)
        end_time = time.perf_counter()
        elapsed_time = end_time - start_time
        logger.info(
            f"Function '{func.__name__}' took {elapsed_time:.1f} seconds to run."
        )
        return result

    return wrapper


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


def create_databricks_session(
    catalog: str | None = os.getenv("DATABRICKS_CATALOG"),
    schema: str | None = os.getenv("DATABRICKS_SCHEMA"),
    ) -> DatabricksSession:
    """
    Uses environment variables (store in .env) to connect to Databricks.
    Optional: to specify catalog or schema.
    Returns: A databricks pyspark session object.
    """
    if os.getenv("DATABRICKS_RUNTIME_VERSION"):
        # Running within a Databricks cluster
        spark = SparkSession.builder.getOrCreate()
    else:    
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
    logger.info(f"Successfully loaded '{yaml_key}' dict values from '{os.path.basename(yaml_file)}'!")
    return cfg