import os
from enum import Enum


class CatalogEnvironment(str, Enum):
    """Defines the environment contexts and their corresponding catalog names."""
    DEV = "aoe_dev"
    PROD = "aoe_prod"

class EnvConfig:
    """Holds application configuration, derived from environment variables."""

    # Read the desired environment setting (e.g., 'DEV', 'PROD')
    # Defaults to 'DEV' if the environment variable isn't set.
    CATALOG_ENV_NAME = os.getenv("CATALOG_ENV", "PROD").upper()
    ENV_NAME = os.getenv("ENV_NAME", "PROD").lower()


    # Determine the actual catalog name based on the environment setting
    try:
        CATALOG_NAME = getattr(CatalogEnvironment, CATALOG_ENV_NAME).value
    except AttributeError:
        print(f"Warning: Invalid CATALOG_ENV '{CATALOG_ENV_NAME}'. Falling back to DEV.")
        CATALOG_NAME = CatalogEnvironment.DEV.value
