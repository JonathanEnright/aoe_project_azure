from utils import Config, Datasource, timer, fetch_api_json, create_adls2_session
from extract import RelicResponse, validate_json_schema, fetch_relic_chunk
from load import load_json_data
import logging
import os
from pathlib import Path

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# Get the directory of the current script
script_dir = Path(__file__).resolve().parent

YAML_CONFIG = os.path.join(script_dir, "config.yaml")


@timer
def main(*args, **kwargs):
    # Generic Setup:
    adls2 = create_adls2_session()
    config = Config(YAML_CONFIG)
    ds = Datasource("relic", config)    

    # Script specific variables
    _validation_schema = RelicResponse

    # Extract phase
    logger.info("Starting data extraction.")
    content_chunk = fetch_relic_chunk(ds.base_url, ds.endpoint, ds.params)

    for i, json_data in enumerate(content_chunk):
        fn = f"{ds.suffix}_{i+1}"

        # Validate phase
        validated_data = validate_json_schema(json_data, _validation_schema)

        # Load phase
        load_json_data(validated_data, ds.container, fn, ds.storage_account, adls2)
    logger.info("Script complete.")


if __name__ == "__main__":
    main()
