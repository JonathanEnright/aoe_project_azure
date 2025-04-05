from src.common.logging_config import setup_logging
from src.transform.bronze._template_bronze import bronze_pipeline


def main():
    yaml_key = "players"
    logger = setup_logging()
    bronze_pipeline(yaml_key, logger)


if __name__ == "__main__":
    main()
