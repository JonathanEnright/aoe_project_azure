from pathlib import Path
import os
from src.common.base_utils import load_yaml_data, create_databricks_session
from src.common.transform_utils import create_external_table, add_metadata_columns, write_to_table

def bronze_pipeline(yaml_key, logger):
    """
    Pipeline to create an External Table pointing to file location in adls2.
    A Managed Table is built over the top with metadata fields added in Unity 
    Catalog containing the latest 45* days worth of data (*this can be overidden).
    """
    script_dir = Path(__file__).resolve().parent
    YAML_CONFIG = os.path.join(script_dir, '_br_tables.yaml')

    cfg = load_yaml_data(YAML_CONFIG, yaml_key)
    spark = create_databricks_session(cfg['catalog'], cfg['database'])

    create_external_table(spark, cfg)
    df = add_metadata_columns(spark, cfg)
    
    # Create or replace the managed table in Unity Catalog
    write_to_table(df, table_name=f"{cfg['managed_table']}")
    logger.info(f"Managed Table '{cfg['managed_table']}' successfully created!")
