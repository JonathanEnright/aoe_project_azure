from pathlib import Path
from deploy import DeploymentConfig, deploy_pipeline, load_pipeline_config

def main():
    script_dir = Path(__file__).resolve().parent
    yaml_config = script_dir / "pipelines" / "ingestion.yaml"
    pipeline_config = load_pipeline_config(str(yaml_config))

    config = DeploymentConfig(
        job_name=pipeline_config["job_name"],
        cron_expression=pipeline_config["schedule"]["cron_expression"],
        timezone_id=pipeline_config["schedule"].get("timezone_id", "UTC"),
        timeout_seconds=pipeline_config.get("parameters", {}).get("timeout_seconds", 600),
        check_interval_secs=30 
    )

    notifications = pipeline_config.get("notifications", {})

    deploy_pipeline(
        config,
        pipeline_config["tasks"],
        notifications=notifications, 
        run_now=True,
        monitor_run=True 
    )

if __name__ == "__main__":
    main()