from datetime import datetime, timedelta
from airflow import DAG
from airflow.models.param import Param
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount

default_args = {
    'owner': 'mohamed_fahmy',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(seconds=10),
}

CONTAINER_IMAGE = "pricing-engine-api:v1.0" 
NETWORK_NAME = "lakehouse-network"  
SCRIPT_PATH = "/app/src/run_pricing_pipeline.py"



COMMON_MOUNTS = [
    Mount(source="pricing-models", target="/app/models", type="volume",read_only=False),
    Mount(source="pricing-output", target="/app/output", type="volume",read_only=False),
]


with DAG(
    dag_id='monthly_demand_model_training_docker_test',
    default_args=default_args,
    description='Trains ML demand model inside Docker container on 1st of every month',
    schedule='0 0 1 * *', 
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=['ml', 'training', 'docker', 'monthly'],
    params={
        "model_path": Param(
            "/app/models/demand_model.joblib",
            type="string",
            description="Path to save the trained .joblib model",
        ),
    },
) as train_dag:

    train_task = DockerOperator(
        task_id='train_model_via_docker',
        image=CONTAINER_IMAGE,
        command=[
        "python",
        SCRIPT_PATH,
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
        docker_url="unix://var/run/docker.sock"
    )