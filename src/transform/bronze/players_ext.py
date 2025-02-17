from pathlib import Path
import sys
parent_dir = Path(__file__).resolve().parent.parent.parent
sys.path.append(str(parent_dir))
from utils import create_external_table
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

yaml_file = 'ext_tables_schema.yaml'
yaml_key = 'players'
create_external_table(yaml_file, yaml_key)
