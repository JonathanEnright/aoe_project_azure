from common.base_utils import Config, Datasource, timer, fetch_api_json, create_adls2_session
from common.extract_utils import validate_json_schema
from common.pydantic_models import ApiSchema
from common.load_utils import load_json_data
from pathlib import Path
from common.logging_config import setup_logging

YAML_KEY = "metadata"
yaml_fn = "config.yaml"
logger = setup_logging()

# Get the directory of the current script
script_dir = Path(__file__).resolve().parent

YAML_CONFIG = str(script_dir/yaml_fn)


@timer
def main(*args, **kwargs):
    # Setup:
    adls2 = create_adls2_session()
    ds = Datasource(YAML_KEY, Config(YAML_CONFIG))    
    _validation_schema = ApiSchema
    _fn = f"{ds.run_end_date}_{ds.suffix}"

    try:
        # Extract phase
        logger.info("Starting data extraction.")
        json_data = fetch_api_json(ds.base_url, ds.endpoint, ds.params)

        if json_data is None:
            logger.error(
                f"Failed to fetch data after 3 attempts for endpoint: {ds.endpoint}"
            )
            raise

        # Validate phase
        logger.info("Validating data.")
        validated_data = validate_json_schema(json_data, _validation_schema)

        # Load phase
        logger.info("Starting data loading.")
        load_json_data(validated_data, ds.container, _fn, ds.storage_account, adls2)

        logger.info("ELT process completed successfully.")
    except Exception as e:
        logger.error(f"ELT process failed: {e}")
        raise
    logger.info(f"Script '{Path(__file__).stem}' finished!")


if __name__ == "__main__":
    main()
