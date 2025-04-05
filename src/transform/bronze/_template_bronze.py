import os
from pathlib import Path

from src.common.base_utils import create_databricks_session, load_yaml_data
from src.common.env_setting import EnvConfig
from src.common.transform_utils import (
    add_metadata_columns,
    write_to_table,
)


def bronze_pipeline(yaml_key, logger):
    """
    Pipeline to create an External Table pointing to file location in adls2.
    A Managed Table is built over the top with metadata fields added in Unity
    Catalog containing the latest 45* days worth of data (*this can be overidden).
    """
    script_dir = Path(__file__).resolve().parent
    YAML_CONFIG = os.path.join(script_dir, "_br_tables.yaml")

    cfg = load_yaml_data(YAML_CONFIG, yaml_key)
    spark = create_databricks_session(catalog=EnvConfig.CATALOG_NAME, schema=cfg["database"])
    cfg['location'] = f"abfss://{EnvConfig.ENV_NAME}@{cfg['adls_location']}"

    spark.conf.set("spark.sql.legacy.parquet.nanosAsLong", "true")

    df = spark.read.format(cfg['format']).options(**cfg['options']).load(cfg['location'])

    df = add_metadata_columns(spark, cfg, df)
    try:
        write_to_table(df, table_name=f"{cfg['managed_table']}")
        logger.info(f"Managed Table '{cfg['managed_table']}' successfully created!")
    except Exception:
        logger.error(f"Managed Table '{cfg['managed_table']}' failed to create")
