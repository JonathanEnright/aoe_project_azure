from src.orchestration.deploy_runner import deploy_pipeline_from_yaml

pipelines = ["ingestion", "bronze", "silver", "gold"]


def main():
    for pipeline in pipelines:
        deploy_pipeline_from_yaml(pipeline)


if __name__ == "__main__":
    main()
