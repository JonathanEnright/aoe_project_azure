from src.common.base_utils import Config, Datasource, timer, create_adls2_session
from src.common.load_utils import upload_to_adls2
from pathlib import Path
from src.common.logging_config import setup_logging

YAML_KEY = "country_list"
yaml_fn = "config.yaml"
csv_fn = "country_list.csv"
logger = setup_logging()

# Get the directory of the current script
script_dir = Path(__file__).resolve().parent

YAML_CONFIG = str(script_dir/yaml_fn)
CSV_FILE = str(script_dir/csv_fn)

@timer
def main(*args, **kwargs):
    # Setup:
    adls2 = create_adls2_session()
    ds = Datasource(YAML_KEY, Config(YAML_CONFIG))
    
    # Extract phase
    with open(CSV_FILE, 'r') as f:
        data = f.read() 

    # Load phase
    upload_to_adls2(adls2, data, ds.storage_account, ds.container, ds.file_name)
    logger.info(f"Script '{Path(__file__).stem}' finished!")


if __name__ == "__main__":
    main()
