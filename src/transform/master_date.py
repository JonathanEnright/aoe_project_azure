from pathlib import Path

from src.common.base_utils import Config, Datasource

# Get the directory of the current script
script_dir = Path(__file__).resolve().parent.parent

# Use the ingestion config file as the 'master_date' for limiting pyspark reads on transform
yaml_fn = 'ingestion/config.yaml'
YAML_KEY = "metadata"
YAML_CONFIG = str(script_dir / yaml_fn)

ds = Datasource(YAML_KEY, Config(YAML_CONFIG))
# print(Config.run_date)
master_run_date = ds.run_date
master_run_end_date = ds.run_end_date
