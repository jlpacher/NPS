from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocCreateClusterOperator,
    DataprocDeleteClusterOperator,
    DataprocSubmitJobOperator
)
from airflow.operators.dummy import DummyOperator
from airflow.utils.trigger_rule import TriggerRule
from google.protobuf.duration_pb2 import Duration

import os

import argparse

# Google
import google.auth
from google.cloud import bigquery
from google.cloud import storage
import os
import json

# Pega requirements
storage_client = storage.Client()
bucket = storage_client.bucket('gcloud-dataproc')
blob = bucket.blob('notebooks/jupyter/Projeto_NPS/Producao/src/requirements.txt')
requirements = blob.download_as_text().replace('\n',',')

# Variables configuration
GCP_PROJECT_ID = 'myproject'
GCP_CONN_ID = 'google_cloud_datascience'
DATAPROC_CLUSTER_NAME = 'prediction-nps-airflow'
DATAPROC_REGION = 'us-central1'
DATAPROC_SCRIPTS = 'gs://gcloud-dataproc/notebooks/jupyter/Projeto_NPS/Producao/src'
PIP_INSTALL_SH = 'gs://gcloud-dataproc/notebooks/jupyter/Projeto_NPS/Producao/src/pip-install.sh'
DATAPROC_CLUSTER_CONFIG = {
    "temp_bucket": "myproject",
    "gce_cluster_config": {
        "zone_uri": "us-central1-a",
        "subnetwork_uri": "k8s-datalabs-datasciense-us-central",
        "tags": ["dataproc"],
        "service_account_scopes": ["https://www.googleapis.com/auth/cloud-platform"],
        "metadata": {
            "PIP_PACKAGES": 'google-cloud-bigquery==3.4.1 google-cloud-bigquery-storage==2.17.0 google-cloud-storage==2.7.0 numpy==1.21.6 pandas-gbq==0.18.1 pyarrow==10.0.1 scikit-learn==1.1.3 scipy==1.8.1 statsmodels==0.13.2 xverse==1.0.5 joblib==1.3.2 pmdarima==2.0.4',
            "gcs-connector-version": "2.2.0",
            "bigquery-connector-version": "1.2.0",
            "spark-bigquery-connector-version": "0.19.1"
        },
    },
    "master_config": {
        "disk_config": {
            "boot_disk_type": "pd-standard",
            "boot_disk_size_gb": 250,
        },
        "machine_type_uri": "n1-highmem-32"
        },
    "software_config": {
        "image_version": "2.0-ubuntu18",
    },
    "worker_config": {
        "disk_config": {
            "boot_disk_type": "pd-standard",
            "boot_disk_size_gb": 200,
        },
        "machine_type_uri": "n1-highmem-16",
        "num_instances": 2,
    },
    "initialization_actions": [
        {
            "executable_file": PIP_INSTALL_SH,
            "execution_timeout":  Duration().FromSeconds(500),
        }
    ],
    "endpoint_config": {"enable_http_port_access": True},
}


def get_dataproc_task(task):
    if "task_args" in task.keys():
        job = {
            "reference": {"project_id": GCP_PROJECT_ID},
            "placement": {"cluster_name": DATAPROC_CLUSTER_NAME},
            "pyspark_job": {
                "main_python_file_uri": f"{DATAPROC_SCRIPTS}/{task['task_callable']}",
                "jar_file_uris": ["gs://spark-lib/bigquery/spark-bigquery-latest_2.11.jar"],
                "args": task['task_args']
            },
        }
    else:
        job = {
            "reference": {"project_id": GCP_PROJECT_ID},
            "placement": {"cluster_name": DATAPROC_CLUSTER_NAME},
            "pyspark_job": {
                "main_python_file_uri": f"{DATAPROC_SCRIPTS}/{task['task_callable']}",
                "jar_file_uris": ["gs://spark-lib/bigquery/spark-bigquery-latest_2.11.jar"],
            },
        }
    task = DataprocSubmitJobOperator(
        task_id=task['task_id'],
        gcp_conn_id='gcp-datalabs-datascience',
        project_id=GCP_PROJECT_ID,
        region=DATAPROC_REGION,
        job=job,
        dag=dag
    )
    return task



default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(year=2024, month=3, day=11, hour=0),
    'retries': 0
}


with DAG(
    dag_id='prediction_nps',
    description='This pipeline runs nps prediction',
    user_defined_macros={'PROJECT': GCP_PROJECT_ID},
    default_args=default_args,
    dagrun_timeout=timedelta(hours=10),
    catchup=True,
    schedule_interval='0 13 * * 1,4'
) as dag:
    create_dataproc_cluster = DataprocCreateClusterOperator(
        task_id='create_dataproc_cluster',
        project_id=GCP_PROJECT_ID,
        cluster_config=DATAPROC_CLUSTER_CONFIG,
        region=DATAPROC_REGION,
        cluster_name=DATAPROC_CLUSTER_NAME
    )

    delete_dataproc_cluster = DataprocDeleteClusterOperator(
        task_id='delete_dataproc_cluster',
        project_id=GCP_PROJECT_ID,
        cluster_name=DATAPROC_CLUSTER_NAME,
        region=DATAPROC_REGION,
        trigger_rule=TriggerRule.ALL_DONE
    )

    ######### tasks ########################

    task_get_data_diarios = {
        'task_id': 'task_get_data_diarios',
        'task_callable': '01_get_data_diarios.py'
    }
    
    task_spine_semanal = {
        'task_id': 'task_spine_semanal',
        'task_callable': '02_spine_semanal.py'
    }

    task_predicao_semanal = {
        'task_id': 'task_predicao_semanal',
        'task_callable': '03_predicao_semanal.py'
    }
    
    task_predicao_mensal = {
        'task_id': 'task_predicao_mensal',
        'task_callable': '04_predicao_mensal.py'
    }

    task_save_history = {
        'task_id': 'task_save_history',
        'task_callable': '05_save_history.py'
    }

    ######### DAG ########################
    (
    create_dataproc_cluster >> 
    get_dataproc_task(task_get_data_diarios) >> 
    get_dataproc_task(task_spine_semanal) >>
    get_dataproc_task(task_predicao_semanal) >>
    get_dataproc_task(task_predicao_mensal) >>
    get_dataproc_task(task_save_history) >>
    delete_dataproc_cluster
    )
