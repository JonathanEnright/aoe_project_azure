from src.common.base_utils import Config, Datasource, timer, fetch_api_file, create_adls2_session
from src.common.extract_utils import (
    generate_weekly_queries,
    create_stats_endpoints,
    validate_parquet_schema,
)
from src.common.pydantic_models import Players
from src.common.load_utils import load_parquet_data
from pathlib import Path
from src.common.logging_config import setup_logging

YAML_KEY = "players"
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
    _validation_schema = Players    
    _base_url = ds.base_url + ds.dir_url

    # Pre-extract phase
    weekly_querys = generate_weekly_queries(ds.run_date, ds.run_end_date)
    endpoints = create_stats_endpoints(ds.suffix, weekly_querys)

    for i, endpoint in enumerate(endpoints):
        endpoint_url = endpoint["endpoint_str"]
        fn = endpoint["file_date"]

        # Extract phase
        content = fetch_api_file(_base_url, endpoint_url, ds.params)

        if content is None:
            logger.error(
                f"Failed to fetch data after 3 attempts for endpoint: {endpoint_url}"
            )
            continue  # Skip to the next iteration of the loop

        # Validate phase
        validated_data = validate_parquet_schema(content, _validation_schema)

        # Load phase
        load_parquet_data(validated_data, ds.container, fn, ds.storage_account, adls2)
        logger.info(f"{i+1}/{len(endpoints)} loaded.")
    logger.info(f"Script '{Path(__file__).stem}' finished!")


if __name__ == "__main__":
    main()
