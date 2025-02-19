from pathlib import Path
import os
from common.base_utils import load_yaml_data, create_databricks_session
from common.transform_utils import create_external_table, add_metadata_columns

def ext_pipeline(yaml_key, logger):
    script_dir = Path(__file__).resolve().parent
    YAML_CONFIG = os.path.join(script_dir, '_raw_tables.yaml')

    cfg = load_yaml_data(YAML_CONFIG, yaml_key)
    spark = create_databricks_session(cfg['catalog'], cfg['database'])

    create_external_table(spark, cfg)
    add_metadata_columns(spark, cfg)

    logger.info(f"'{yaml_key.capitalize()}' external table processing complete!")