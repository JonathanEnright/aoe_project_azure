from src.common.logging_config import setup_logging
from src.transform.bronze._template_bronze import bronze_pipeline

# All configuration of the external table is defined in yaml.
# Simply pass the yaml (dictionary) key from '_br_tables.yaml'
yaml_key = 'players' 

logger = setup_logging()
bronze_pipeline(yaml_key, logger)