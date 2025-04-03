from src.orchestration.deploy_runner import deploy_pipeline_from_yaml

pipeline = "bronze"


def main():
    deploy_pipeline_from_yaml(pipeline)


if __name__ == "__main__":
    main()
