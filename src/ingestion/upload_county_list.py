from common.base_utils import Config, Datasource, timer, create_adls2_session
from common.load_utils import upload_to_adls2
import os
from pathlib import Path
from common.logging_config import setup_logging

logger = setup_logging()

# Get the directory of the current script
script_dir = Path(__file__).resolve().parent

YAML_CONFIG = os.path.join(script_dir, "config.yaml")
CSV_FILE = os.path.join(script_dir, "country_list.csv")

@timer
def main(*args, **kwargs):
    # Setup:
    adls2 = create_adls2_session()
    ds = Datasource("country_list", Config(YAML_CONFIG))
    
    # Extract phase
    with open(CSV_FILE, 'r') as f:
        data = f.read() 

    # Load phase
    upload_to_adls2(adls2, data, ds.storage_account, ds.container, ds.file_name)
    logger.info("Script complete.")


if __name__ == "__main__":
    main()
