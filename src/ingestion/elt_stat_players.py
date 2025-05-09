from pathlib import Path

from src.common.base_utils import (
    Config,
    Datasource,
    create_adls2_session,
    fetch_api_file,
    timer,
)
from src.common.env_setting import EnvConfig
from src.common.extract_utils import (
    create_stats_endpoints,
    generate_weekly_queries,
    validate_parquet_schema,
)
from src.common.load_utils import load_parquet_data
from src.common.logging_config import setup_logging
from src.common.pydantic_models import Players

YAML_KEY = "players"
yaml_fn = "config.yaml"
logger = setup_logging()

# Get the directory of the current script
script_dir = Path(__file__).resolve().parent

YAML_CONFIG = str(script_dir / yaml_fn)


@timer
def main(*args, **kwargs):
    # Setup:
    adls2 = create_adls2_session()
    ds = Datasource(YAML_KEY, Config(YAML_CONFIG))
    _container = f"{EnvConfig.ENV_NAME}/{ds.container_suffix}"
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
        validated_data = validate_parquet_schema(content, _validation_schema, ds.cast_mapping)

        if validated_data is None:
            logger.error(
                f"No valid data found for file: {endpoint_url}"
            )
            continue  # Skip to the next iteration of the loop
        # Load phase
        load_parquet_data(validated_data, _container, fn, ds.storage_account, adls2)
        logger.info(f"{i + 1}/{len(endpoints)} loaded.")
    logger.info(f"Script '{Path(__file__).stem}' finished!")


if __name__ == "__main__":
    main()
