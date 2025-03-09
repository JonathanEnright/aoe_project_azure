import os
import subprocess
import shutil
import yaml
import time
import logging
from pathlib import Path
from dataclasses import dataclass, field
from typing import List, Dict, Any
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.jobs import (
    CronSchedule, PauseStatus, Task, PythonWheelTask, TaskDependency, 
    RunLifeCycleState, RunResultState, JobEmailNotifications, JobSettings
)
from databricks.sdk.service.compute import Library
from databricks.sdk.service.workspace import ImportFormat


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

logger = logging.getLogger(__name__)

@dataclass
class DeploymentConfig:
    job_name: str
    cron_expression: str
    timezone_id: str = "UTC"
    local_wheel_dir: str = "dist"
    workspace_wheel_dir: str = "/Workspace/local_to_db/"
    timeout_seconds: int = 600
    check_interval_secs: int = 30
    email_notifications: Dict[str, List[str]] = field(default_factory=dict)

    cluster_id: str = field(default_factory=lambda: os.environ.get("DATABRICKS_CLUSTER_ID"))
    databricks_host: str = field(default_factory=lambda: os.environ.get("DATABRICKS_HOST_URL"))
    databricks_token: str = field(default_factory=lambda: os.environ.get("DATABRICKS_PAT"))

    def __post_init__(self):
        required_envs = {
            "DATABRICKS_CLUSTER_ID": self.cluster_id,
            "DATABRICKS_HOST_URL": self.databricks_host,
            "DATABRICKS_PAT": self.databricks_token
        }
        missing_envs = [k for k, v in required_envs.items() if not v]
        if missing_envs:
            raise EnvironmentError(f"Missing environment variables: {', '.join(missing_envs)}")

def load_pipeline_config(yaml_file: str) -> Dict[str, Any]:
    logger.info(f"Loading pipeline configuration from file: {yaml_file}")
    with open(yaml_file) as f:
        config = yaml.safe_load(f)
    required = ["job_name", "tasks", "schedule"]
    missing = [r for r in required if r not in config]
    if missing:
        raise ValueError(f"Pipeline config missing required keys: {missing}")
    logger.info("Successfully loaded pipeline configuration.")
    return config

class DatabricksDeployer:
    def __init__(self, config: DeploymentConfig):
        self.config = config
        self.client = WorkspaceClient(
            host=config.databricks_host,
            token=config.databricks_token
        )

    def build_wheel(self) -> Path:
        logger.info("Building the Python wheel package locally...")
        shutil.rmtree(self.config.local_wheel_dir, ignore_errors=True)
        subprocess.run(
            ["python", "setup.py", "bdist_wheel", "-d", self.config.local_wheel_dir],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.STDOUT,
            check=True
        )
        wheels = list(Path(self.config.local_wheel_dir).glob("*.whl"))
        if not wheels:
            raise FileNotFoundError("No wheel file found post-build.")
        wheel_path = wheels[0]
        logger.info(f"Wheel successfully built: {wheel_path}")
        return wheel_path

    def upload_wheel(self, wheel_path: Path) -> str:
        workspace_path = f"{self.config.workspace_wheel_dir}{wheel_path.name}"
        logger.info(f"Uploading wheel '{wheel_path.name}' to Databricks workspace at '{workspace_path}'...")
        with open(wheel_path, "rb") as f:
            self.client.workspace.upload(
                path=workspace_path,
                content=f.read(),
                format=ImportFormat.AUTO,
                overwrite=True
            )
        logger.info(f"Wheel uploaded successfully to '{workspace_path}'.")
        return workspace_path

    
    def create_or_update_job(self, tasks: List[Dict[str, Any]], wheel_path: str, notifications: Dict[str, Any]) -> int:
        package_name = wheel_path.split("/")[-1].split("-")[0]

        job_tasks = [
            Task(
                task_key=t["task_key"],
                existing_cluster_id=self.config.cluster_id,
                python_wheel_task=PythonWheelTask(
                    package_name=package_name,
                    entry_point=t["entry_point"]
                ),
                libraries=[Library(whl=wheel_path)],
                depends_on=[TaskDependency(task_key=d) for d in t.get("depends_on", [])],
                description=t.get("description", ""),
            ) for t in tasks
        ]

        # Construct job settings
        job_settings = JobSettings(
            name=self.config.job_name,
            tasks=job_tasks,
            schedule=CronSchedule(
                quartz_cron_expression=self.config.cron_expression,
                timezone_id=self.config.timezone_id,
                pause_status=PauseStatus.UNPAUSED
            ),
            email_notifications=JobEmailNotifications(on_start=notifications.get('on_start', [])),
            timeout_seconds=self.config.timeout_seconds
        )

        # Check if job already exists
        existing_jobs = self.client.jobs.list()
        job_id = None
        for job in existing_jobs:
            if job.settings.name == self.config.job_name:
                job_id = job.job_id
                logger.info(f"Existing job '{self.config.job_name}' found (Job ID: {job_id}). Updating it.")
                break

        if job_id:
            # Update existing job
            self.client.jobs.reset(job_id=job_id, new_settings=job_settings)
            logger.info(f"Job '{self.config.job_name}' (Job ID: {job_id}) updated successfully.")
        else:
            # Create new job
            response = self.client.jobs.create(**job_settings.as_dict())
            job_id = response.job_id
            logger.info(f"New job '{self.config.job_name}' created successfully (Job ID: {job_id}).")

        return job_id

    def run_job(self, job_id: int) -> int:
        logger.info(f"Triggering immediate run for Job ID: {job_id}...")
        run_response = self.client.jobs.run_now(job_id=job_id)
        run_id = run_response.run_id
        logger.info(f"Job run triggered successfully. Run ID: {run_id}")
        return run_id

    def monitor_run(self, run_id: int, job_id: int):
        logger.info(f"Monitoring job run '{run_id}'. Visit: {self.config.databricks_host}/jobs/{job_id}/runs/{run_id}")
        start_time = time.time()

        try:
            while True:
                run_info = self.client.jobs.get_run(run_id)
                state = run_info.state.life_cycle_state
                result_state = run_info.state.result_state
                elapsed = time.time() - start_time

                if state in [RunLifeCycleState.PENDING, RunLifeCycleState.RUNNING, RunLifeCycleState.QUEUED]:
                    logger.info(f"[{elapsed:.0f}s elapsed] Job run status: {state.value}. Continuing monitoring...")
                    time.sleep(self.config.check_interval_secs)
                elif result_state == RunResultState.SUCCESS:
                    logger.info(f"✅ Job run completed successfully in {elapsed:.0f} seconds.")
                    break
                else:
                    logger.error(f"❌ Job run ended with state: {state.value}, result: {result_state.value}")
                    break

        except KeyboardInterrupt:
            logger.warning("Keyboard interrupt received. Attempting to gracefully cancel the Databricks job...")
            self.client.jobs.cancel_run(run_id=run_id)
            logger.info(f"Cancellation request sent for run ID: {run_id}.")
            raise
        finally:
            logger.info("Job monitoring completed.")

def deploy_pipeline(config: DeploymentConfig, pipeline_tasks: List[Dict[str, Any]], notifications: Dict[str, Any]=None, run_now=False, monitor_run=False):
    deployer = DatabricksDeployer(config)
    wheel_local_path = deployer.build_wheel()
    wheel_workspace_path = deployer.upload_wheel(wheel_local_path)
    job_id = deployer.create_or_update_job(pipeline_tasks, wheel_workspace_path, notifications or {})

    if run_now:
        run_id = deployer.run_job(job_id)
        if monitor_run:
            deployer.monitor_run(run_id, job_id)
    else:
        logger.info(f"Pipeline '{config.job_name}' deployed successfully and scheduled.")