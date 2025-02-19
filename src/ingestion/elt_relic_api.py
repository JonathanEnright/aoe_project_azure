from common.base_utils import Config, Datasource, timer, create_adls2_session
from common.extract_utils import validate_json_schema, fetch_relic_chunk
from common.pydantic_models import RelicResponse
from common.load_utils import load_json_data
import os
from pathlib import Path
from common.logging_config import setup_logging

logger = setup_logging()

# Get the directory of the current script
script_dir = Path(__file__).resolve().parent

YAML_CONFIG = os.path.join(script_dir, "config.yaml")


@timer
def main(*args, **kwargs):
    # Setup:
    adls2 = create_adls2_session()
    ds = Datasource("relic", Config(YAML_CONFIG))    
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
