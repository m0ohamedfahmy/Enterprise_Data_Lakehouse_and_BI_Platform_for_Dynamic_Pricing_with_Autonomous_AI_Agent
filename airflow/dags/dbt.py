from pathlib import Path
from datetime import datetime
from cosmos import DbtDag, ExecutionConfig, LoadMode, ProfileConfig, ProjectConfig, RenderConfig
from cosmos.constants import ExecutionMode


DBT_PROJECT_DIR = "/usr/local/airflow/dbt/my_lakehouse_project"  
DBT_EXECUTABLE_PATH = "/usr/local/airflow/dbt_venv/bin/dbt"

project_config = ProjectConfig(
    DBT_PROJECT_DIR,
    manifest_path=f"{DBT_PROJECT_DIR}/target/manifest.json",  
)
render_config = RenderConfig(
    load_method=LoadMode.DBT_MANIFEST
)
profile_config = ProfileConfig(
    profile_name="my_lakehouse_project",
    target_name="dev",
    profiles_yml_filepath=f"{DBT_PROJECT_DIR}/profiles.yml",
)

execution_config = ExecutionConfig(
    execution_mode=ExecutionMode.LOCAL,
    dbt_executable_path=DBT_EXECUTABLE_PATH,
)

lakehouse_dbt_dag = DbtDag(
    project_config=project_config,
    profile_config=profile_config,
    render_config=render_config,
    execution_config=execution_config,
    dag_id="lakehouse_dbt_dag_122",
    schedule="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args={"retries": 0},
)

