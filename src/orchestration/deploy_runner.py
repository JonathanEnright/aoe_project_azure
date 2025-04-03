from pathlib import Path

from src.orchestration.deploy import (
    DeploymentConfig,
    deploy_pipeline,
    load_pipeline_config,
)


def deploy_pipeline_from_yaml(pipeline_name, run_now=True, monitor_run=True):
    script_dir = Path(__file__).resolve().parents[1]
    yaml_config = script_dir / "pipelines" / f"{pipeline_name}.yaml"
    pipeline_config = load_pipeline_config(str(yaml_config))

    config = DeploymentConfig(
        job_name=pipeline_config["job_name"],
        cron_expression=pipeline_config["schedule"]["cron_expression"],
        timezone_id=pipeline_config["schedule"].get("timezone_id", "UTC"),
        timeout_seconds=pipeline_config.get("parameters", {}).get(
            "timeout_seconds", 1200
        ),
        check_interval_secs=pipeline_config.get("parameters", {}).get(
            "check_interval_secs", 30
        ),
    )

    notifications = pipeline_config.get("notifications", {})

    deploy_pipeline(
        config,
        pipeline_config["tasks"],
        notifications=notifications,
        run_now=run_now,
        monitor_run=monitor_run,
    )
