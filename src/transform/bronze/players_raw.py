from pathlib import Path
import sys
parent_dir = Path(__file__).resolve().parent.parent.parent
sys.path.append(str(parent_dir))
from utils import create_external_table, load_yaml_data, connect_to_databricks, add_metadata_columns
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# Script to create both a external and then managed table over the top

yaml_file = '_raw_tables.yaml'
yaml_key = 'players'

cfg = load_yaml_data(yaml_file, yaml_key)
spark = connect_to_databricks(cfg['catalog'],cfg['database'])

create_external_table(spark, cfg)
add_metadata_columns(spark, cfg)

logger.info('Script Complete!')
