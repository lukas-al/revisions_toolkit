from __future__ import annotations
from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

from kedro.framework.session import KedroSession
from kedro.framework.project import configure_project


class KedroOperator(BaseOperator):
    @apply_defaults
    def __init__(
        self,
        package_name: str,
        pipeline_name: str,
        node_name: str,
        project_path: str | Path,
        env: str,
        *args, **kwargs
    ) -> None:
        super().__init__(*args, **kwargs)
        self.package_name = package_name
        self.pipeline_name = pipeline_name
        self.node_name = node_name
        self.project_path = project_path
        self.env = env

    def execute(self, context):
        configure_project(self.package_name)
        with KedroSession.create(project_path=self.project_path,
                                 env=self.env) as session:
            session.run(self.pipeline_name, node_names=[self.node_name])


# Kedro settings required to run your pipeline
env = "local"
pipeline_name = "__default__"
project_path = Path.cwd()
package_name = "revisions_toolkit"

# Using a DAG context manager, you don't have to specify the dag property of each task
with DAG(
    dag_id="revisions-toolkit",
    start_date=datetime(2023,1,1),
    max_active_runs=3,
    # https://airflow.apache.org/docs/stable/scheduler.html#dag-runs
    schedule_interval="@once",
    catchup=False,
    # Default settings applied to all tasks
    default_args=dict(
        owner="airflow",
        depends_on_past=False,
        email_on_failure=False,
        email_on_retry=False,
        retries=1,
        retry_delay=timedelta(minutes=5)
    )
) as dag:
    tasks = {
        "load-mgdp-data": KedroOperator(
            task_id="load-mgdp-data",
            package_name=package_name,
            pipeline_name=pipeline_name,
            node_name="Load_MGDP_data",
            project_path=project_path,
            env=env,
        ),
        "load-qgdp-data": KedroOperator(
            task_id="load-qgdp-data",
            package_name=package_name,
            pipeline_name=pipeline_name,
            node_name="Load_QGDP_data",
            project_path=project_path,
            env=env,
        ),
        "load-deflator-data": KedroOperator(
            task_id="load-deflator-data",
            package_name=package_name,
            pipeline_name=pipeline_name,
            node_name="Load_deflator_data",
            project_path=project_path,
            env=env,
        ),
        "load-expenditure-data": KedroOperator(
            task_id="load-expenditure-data",
            package_name=package_name,
            pipeline_name=pipeline_name,
            node_name="Load_expenditure_data",
            project_path=project_path,
            env=env,
        ),
        "load-income-data": KedroOperator(
            task_id="load-income-data",
            package_name=package_name,
            pipeline_name=pipeline_name,
            node_name="Load_income_data",
            project_path=project_path,
            env=env,
        ),
        "clean-mgdp-data": KedroOperator(
            task_id="clean-mgdp-data",
            package_name=package_name,
            pipeline_name=pipeline_name,
            node_name="Clean_MGDP_data",
            project_path=project_path,
            env=env,
        ),
        "clean-qgdp-data": KedroOperator(
            task_id="clean-qgdp-data",
            package_name=package_name,
            pipeline_name=pipeline_name,
            node_name="Clean_QGDP_data",
            project_path=project_path,
            env=env,
        ),
        "clean-deflator-data": KedroOperator(
            task_id="clean-deflator-data",
            package_name=package_name,
            pipeline_name=pipeline_name,
            node_name="Clean_deflator_data",
            project_path=project_path,
            env=env,
        ),
        "clean-expenditure-data": KedroOperator(
            task_id="clean-expenditure-data",
            package_name=package_name,
            pipeline_name=pipeline_name,
            node_name="Clean_expenditure_data",
            project_path=project_path,
            env=env,
        ),
        "clean-income-data": KedroOperator(
            task_id="clean-income-data",
            package_name=package_name,
            pipeline_name=pipeline_name,
            node_name="Clean_income_data",
            project_path=project_path,
            env=env,
        ),
        "transform-mgdp-data": KedroOperator(
            task_id="transform-mgdp-data",
            package_name=package_name,
            pipeline_name=pipeline_name,
            node_name="Transform_MGDP_data",
            project_path=project_path,
            env=env,
        ),
        "transform-qgdp-data": KedroOperator(
            task_id="transform-qgdp-data",
            package_name=package_name,
            pipeline_name=pipeline_name,
            node_name="Transform_QGDP_data",
            project_path=project_path,
            env=env,
        ),
        "transform-deflator-data": KedroOperator(
            task_id="transform-deflator-data",
            package_name=package_name,
            pipeline_name=pipeline_name,
            node_name="Transform_deflator_data",
            project_path=project_path,
            env=env,
        ),
        "transform-expenditure-data": KedroOperator(
            task_id="transform-expenditure-data",
            package_name=package_name,
            pipeline_name=pipeline_name,
            node_name="Transform_expenditure_data",
            project_path=project_path,
            env=env,
        ),
        "transform-income-data": KedroOperator(
            task_id="transform-income-data",
            package_name=package_name,
            pipeline_name=pipeline_name,
            node_name="Transform_income_data",
            project_path=project_path,
            env=env,
        ),
        "save-mgdp-data": KedroOperator(
            task_id="save-mgdp-data",
            package_name=package_name,
            pipeline_name=pipeline_name,
            node_name="Save_MGDP_data",
            project_path=project_path,
            env=env,
        ),
        "save-qgdp-data": KedroOperator(
            task_id="save-qgdp-data",
            package_name=package_name,
            pipeline_name=pipeline_name,
            node_name="Save_QGDP_data",
            project_path=project_path,
            env=env,
        ),
        "save-deflator-data": KedroOperator(
            task_id="save-deflator-data",
            package_name=package_name,
            pipeline_name=pipeline_name,
            node_name="Save_deflator_data",
            project_path=project_path,
            env=env,
        ),
        "save-expenditure-data": KedroOperator(
            task_id="save-expenditure-data",
            package_name=package_name,
            pipeline_name=pipeline_name,
            node_name="Save_expenditure_data",
            project_path=project_path,
            env=env,
        ),
        "save-income-data": KedroOperator(
            task_id="save-income-data",
            package_name=package_name,
            pipeline_name=pipeline_name,
            node_name="Save_income_data",
            project_path=project_path,
            env=env,
        ),
    }

    tasks["clean-mgdp-data"] >> tasks["transform-mgdp-data"]
    tasks["clean-qgdp-data"] >> tasks["transform-qgdp-data"]
    tasks["clean-expenditure-data"] >> tasks["transform-expenditure-data"]
    tasks["load-income-data"] >> tasks["clean-income-data"]
    tasks["transform-income-data"] >> tasks["save-income-data"]
    tasks["transform-expenditure-data"] >> tasks["save-expenditure-data"]
    tasks["load-qgdp-data"] >> tasks["clean-qgdp-data"]
    tasks["load-deflator-data"] >> tasks["clean-deflator-data"]
    tasks["load-mgdp-data"] >> tasks["clean-mgdp-data"]
    tasks["transform-mgdp-data"] >> tasks["save-mgdp-data"]
    tasks["transform-deflator-data"] >> tasks["save-deflator-data"]
    tasks["clean-deflator-data"] >> tasks["transform-deflator-data"]
    tasks["load-expenditure-data"] >> tasks["clean-expenditure-data"]
    tasks["clean-income-data"] >> tasks["transform-income-data"]
    tasks["transform-qgdp-data"] >> tasks["save-qgdp-data"]
