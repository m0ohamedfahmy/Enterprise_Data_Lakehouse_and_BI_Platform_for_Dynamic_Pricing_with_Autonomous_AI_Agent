from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.models.param import Param
from airflow.providers.docker.operators.docker import DockerOperator
from cosmos.constants import ExecutionMode
from cosmos import DbtTaskGroup, ExecutionConfig, LoadMode, ProfileConfig, RenderConfig,ProjectConfig
from docker.types import Mount

default_args = {
    'owner': 'mohamed_fahmy',
    'depends_on_past': False,
    'retries': 0,
    'retry_delay': timedelta(seconds=10)}

INGESTION_CONTAINER_IMAGE = "my-spark-ingestion-job:v1"
ML_CONTAINER_IMAGE = "pricing-engine-api:v1.0"
NETWORK_NAME = "lakehouse-network"

ML_SCRIPT_PATH = "/app/src/run_pricing_pipeline.py"
DBT_PROJECT_DIR = Path("/usr/local/airflow/dbt/my_lakehouse_project")
DBT_EXECUTABLE_PATH = Path("/usr/local/airflow/dbt_venv/bin/dbt")

COMMON_MOUNTS = [
    Mount(source="pricing-models", target="/app/models", type="volume", read_only=False),
    Mount(source="pricing-output", target="/app/output", type="volume", read_only=False) ]

project_config = ProjectConfig(
    DBT_PROJECT_DIR,
    manifest_path=f"{DBT_PROJECT_DIR}/target/manifest.json")

render_config = RenderConfig( load_method=LoadMode.DBT_MANIFEST)

profile_config = ProfileConfig(
    profile_name="my_lakehouse_project",
    target_name="dev",
    profiles_yml_filepath=f"{DBT_PROJECT_DIR}/profiles.yml")

execution_config = ExecutionConfig(
    execution_mode=ExecutionMode.LOCAL,
    dbt_executable_path=DBT_EXECUTABLE_PATH)

with DAG(
    dag_id='end_to_end_lakehouse_ml_pipeline',
    default_args=default_args,
    description='Full Pipeline: Ingestion -> dbt -> ML Training -> ML Scoring',
    schedule='0 0 * * 0',  
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=['lakehouse', 'ingestion', 'dbt', 'ml', 'spark', 'scoring', 'weekly'],
    params={
        "model_path": Param("/app/models/demand_model.joblib", type="string", description="Path to save/load model"),
        "output_path": Param("/app/output/price_recommendations.parquet", type="string", description="Scoring output path"),
        "min_margin_pct": Param(0.10, type="number", description="Minimum margin percentage"),
        "max_competitive_gap_pct": Param(0.05, type="number", description="Maximum competitive gap percentage"),
        "n_candidates": Param(20, type="integer", description="Number of candidate price points to test")
    },
) as dag:

    # 1: Spark Ingestion
    run_spark_ingestion = DockerOperator(
        task_id='run_spark_processing',
        image=INGESTION_CONTAINER_IMAGE,
        network_mode=NETWORK_NAME,
        mount_tmp_dir=False,
        auto_remove=True,
        environment={
                "NESSIE_URI": "{{ var.value.NESSIE_URI }}",
                "MINIO_ACCESS_KEY": "{{ conn.minio_default.login }}",
                "MINIO_SECRET_KEY": "{{ conn.minio_default.password }}",
                "AWS_REGION": "us-east-1",
                "AWS_DEFAULT_REGION": "us-east-1",
            },
        docker_url="unix://var/run/docker.sock")

    # 2: dbt Transformations (Task Group)
    dbt_transformations = DbtTaskGroup(
        group_id="dbt_transformations",
        project_config=project_config,
        profile_config=profile_config,
        render_config=render_config,
        execution_config=execution_config,
        dag=dag )

    # 3: ML Model Training
    train_model_task = DockerOperator(
        task_id='train_model_via_docker',
        image=ML_CONTAINER_IMAGE,
        command=[
            "python",
            ML_SCRIPT_PATH,
            "train",
            "--model-path",
            "{{ params.model_path }}"],
        network_mode=NETWORK_NAME,
        mount_tmp_dir=False,
        auto_remove=True,
        mounts=COMMON_MOUNTS,
        environment={
            'PYTHONPATH': '/app:/app/src'
        },
        docker_url="unix://var/run/docker.sock")

    # 4: ML Model Scoring
    pipe_score_task = DockerOperator(
        task_id='pipeline_score_task',
        image=ML_CONTAINER_IMAGE,
        command=[
                    "/bin/sh", "-c",
                    "python " + ML_SCRIPT_PATH + " score "
                    "--model-path '{{ params.model_path }}' "
                    "--output-path '{{ params.output_path }}' "
                    "--min-margin-pct {{ params.min_margin_pct }} "
                    "--max-competitive-gap-pct {{ params.max_competitive_gap_pct }} "
                    "--n-candidates {{ params.n_candidates }}"
                ],
        network_mode=NETWORK_NAME,
        mount_tmp_dir=False,
        auto_remove=True,
        mounts=COMMON_MOUNTS,
        environment={
            'PYTHONPATH': '/app:/app/src' },
        docker_url="unix://var/run/docker.sock" )

    run_spark_ingestion >> dbt_transformations >> train_model_task >> pipe_score_task