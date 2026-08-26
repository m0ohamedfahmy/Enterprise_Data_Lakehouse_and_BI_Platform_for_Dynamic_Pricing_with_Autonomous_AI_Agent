from datetime import datetime, timedelta
from airflow import DAG
from airflow.models.param import Param
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount
from docker.types import Mount

default_args = {
    'owner': 'mohamed_fahmy',
    'depends_on_past': False,
    'retries': 0,
    'retry_delay': timedelta(seconds=10),
}


INGESTION_CONTAINER_IMAGE = "my-spark-ingestion-job:v1" 
NETWORK_NAME = "lakehouse-network"  
INGESTION_SCRIPT_PATH = "/app/00_Data_ingestion.py"


with DAG(
    dag_id='ingestion_test',
    default_args=default_args,
    description='Trains ML demand model inside Docker container on 1st of every month',
    schedule='0 0 1 * *', 
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=['ml', 'training', 'docker', 'monthly'],

) as train_dag:

    test_spark_conf = DockerOperator(
        task_id="test_spark_iceberg_connection",
        image=INGESTION_CONTAINER_IMAGE,
        command="python3 /app/spark_test_config.py",
        network_mode=NETWORK_NAME,
        mount_tmp_dir=False,        
        environment={
                "NESSIE_URI": "{{ var.value.NESSIE_URI }}",
                "MINIO_ACCESS_KEY": "{{ conn.minio_default.login }}",
                "MINIO_SECRET_KEY": "{{ conn.minio_default.password }}",
                "AWS_REGION": "us-east-1",
                "AWS_DEFAULT_REGION": "us-east-1",
            },
        auto_remove=True,
        docker_url="unix://var/run/docker.sock" )


    train_task = DockerOperator(
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
            "AWS_DEFAULT_REGION": "us-east-1"
        },
        docker_url="unix://var/run/docker.sock"
    )
    test_spark_conf >> train_task
