from common.logging_config import setup_logging
from transform.bronze._template_ext import ext_pipeline

# All configuration of the external table is defined in yaml.
# Simply pass the yaml (dictionary) key from '_raw_tables.yaml'
yaml_key = 'relic' 

logger = setup_logging()
ext_pipeline(yaml_key, logger)