from datetime import datetime, timedelta
from airflow import DAG
from airflow.models.param import Param
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount

default_args = {
    'owner': 'mohamed_fahmy',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(seconds=5),
}

# 1. إعدادات المسارات الحاويات
CONTAINER_IMAGE = "pricing-engine-api:v1.0"   # نفس اسم صورة FastAPI
NETWORK_NAME = "lakehouse-network"  # الشبكة المشتركة مع Dremio/MinIO
SCRIPT_PATH = "/app/src/run_pricing_pipeline.py"


# 3. الـ Mounts المشتركة بين الحاوية و Docker Host
COMMON_MOUNTS = [
    Mount(source="pricing-models", target="/app/models", type="volume",read_only=False),
    Mount(source="pricing-output", target="/app/output", type="volume",read_only=False),
]


# Training ML Model DAG Monthly
with DAG(
    dag_id='monthly_demand_model_training_docker',
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
        command="""
            python """ + SCRIPT_PATH + """ train \
                --model-path '{{ params.model_path }}'
        """,
        network_mode=NETWORK_NAME,
        auto_remove=True,
        mounts=COMMON_MOUNTS,
        docker_url="unix://var/run/docker.sock",
    )

# Scoring ML Model DAG Weekly

with DAG(
    dag_id='weekly_demand_model_scoring_docker',
    default_args=default_args,
    description='Runs weekly price recommendations scoring inside Docker container',
    schedule='0 0 * * 1',  
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=['ml', 'scoring', 'docker', 'weekly'],
    params={
        "model_path": Param(
            "/app/models/demand_model.joblib",
            type="string",
            description="Model path inside container",
        ),
        "output_path": Param(
            "/app/output/price_recommendations.parquet",
            type="string",
            description="Output parquet path inside container",
        ),
        "min_margin_pct": Param(0.10, type="number"),
        "max_competitive_gap_pct": Param(0.05, type="number"),
        "n_candidates": Param(20, type="integer", description="Number of candidate price points to test"),
    },
) as score_dag:

    score_task = DockerOperator(
        task_id='score_model_via_docker',
        image=CONTAINER_IMAGE,
        command=[
                    "/bin/sh", "-c",
                    "python " + SCRIPT_PATH + " score "
                    "--model-path '{{ params.model_path }}' "
                    "--output-path '{{ params.output_path }}' "
                    "--min-margin-pct {{ params.min_margin_pct }} "
                    "--max-competitive-gap-pct {{ params.max_competitive_gap_pct }} "
                    "--n-candidates {{ params.n_candidates }}"
                ],
        network_mode=NETWORK_NAME,
        auto_remove=True,
        mounts=COMMON_MOUNTS,
        mount_tmp_dir=False,
        environment={
                'PYTHONPATH': '/app:/app/src'  
            },
        docker_url="unix://var/run/docker.sock",
    )

# Train and Score DAGs can be triggered independently or in sequence as needed.
with DAG(
    dag_id='full_retrain_and_score_pipeline_docker',
    default_args=default_args,
    description='Pipeline: Retrains the model FIRST, then runs scoring immediately',
    schedule=None,  
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=['ml', 'pipeline', 'docker'],
    params={
        "model_path": Param("/app/models/demand_model.joblib", type="string"),
        "output_path": Param("/app/output/price_recommendations.parquet", type="string"),
        "min_margin_pct": Param(0.10, type="number"),
        "max_competitive_gap_pct": Param(0.05, type="number"),
    },
) as pipeline_dag:

    pipe_train_task = DockerOperator(
        task_id='pipeline_train_task',
        image=CONTAINER_IMAGE,
        command="""
            python """ + SCRIPT_PATH + """ train \
                --model-path '{{ params.model_path }}'
        """,
        network_mode=NETWORK_NAME,
        auto_remove=True,
        mounts=COMMON_MOUNTS,
        docker_url="unix://var/run/docker.sock",
    )

    pipe_score_task = DockerOperator(
        task_id='pipeline_score_task',
        image=CONTAINER_IMAGE,
        command="""
            python """ + SCRIPT_PATH + """ score \
                --model-path '{{ params.model_path }}' \
                --output-path '{{ params.output_path }}' \
                --min-margin-pct {{ params.min_margin_pct }} \
                --max-competitive-gap-pct {{ params.max_competitive_gap_pct }}
        """,
        network_mode=NETWORK_NAME,
        auto_remove=True,
        mounts=COMMON_MOUNTS,
        docker_url="unix://var/run/docker.sock",
    )

    pipe_train_task >> pipe_score_task    