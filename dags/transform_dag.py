from datetime import datetime, timedelta
from airflow import DAG
from helpers.configurations import Config
from operators.move import LoadS3Operator
from airflow.operators.dummy_operator import DummyOperator
from airflow.contrib.operators.emr_create_job_flow_operator import EmrCreateJobFlowOperator
from airflow.contrib.operators.emr_add_steps_operator import EmrAddStepsOperator
from airflow.contrib.sensors.emr_step_sensor import EmrStepSensor
from airflow.contrib.operators.emr_terminate_job_flow_operator import (
    EmrTerminateJobFlowOperator,
)
from load_subdag import load_files_subdag
from airflow.operators.subdag_operator import SubDagOperator

# Configurations

#--#--# SET S3 BUCKET NAME #--#--#
BUCKET_NAME = 
#--#--#--#--#--#--#--#--#--#--#--#

transform_script = "transform.py"
quality_script = "quality.py"


#Files to load
files = ['./dags/scripts/transform.py', './dags/scripts/quality.py', 
        './dags/data/airport-codes_csv.csv',
        './dags/data/GlobalLandTemperaturesByCity.csv',
        './dags/data/us-cities-demographics.csv']

#S3 keys of loaded files
keys = ["transform.py", "quality.py", 'airport-codes_csv.csv', 
        'GlobalLandTemperaturesByCity.csv',
        'us-cities-demographics.csv']

#---#---# Define DAG #---#---#
default_args = {
    'owner': 'jose manuel',
    'depends_on_past': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5)
}


dag = DAG('transform',
          default_args = default_args,
          start_date = datetime(2015,12,31),
          end_date = datetime(2016,12,31),
          description = 'Load and transform data in EMR with PySpark',
          schedule_interval = '@yearly',
        )

#---#---# Define Operators #---#---#

start_operator = DummyOperator(
  task_id='Begin_execution',
  dag=dag
)

load_files_subdag = SubDagOperator(
    subdag = load_files_subdag(
        'transform',
        'load_files',
        BUCKET_NAME,
        files,
        keys,
        start_date= datetime(2015,12,31)
    ),
    task_id='load_files',
    dag=dag
)


# Create an EMR cluster
create_emr_cluster = EmrCreateJobFlowOperator(
    task_id="create_emr_cluster",
    job_flow_overrides=Config.JOB_FLOW_OVERRIDES,
    aws_conn_id="aws_default",
    emr_conn_id="emr_default",
    region_name='us-east-1',
    dag=dag,
)


step_adder = EmrAddStepsOperator(
    task_id="add_steps",
    job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster', key='return_value') }}",
    aws_conn_id="aws_default",
    steps=Config.SPARK_STEPS,
    params={
        "BUCKET_NAME": BUCKET_NAME,
        "transform_script": transform_script,
        "quality_script": quality_script,

    },
    dag=dag,
)

last_step = len(Config.SPARK_STEPS) - 1

step_checker = EmrStepSensor(
    task_id="watch_step",
    job_flow_id="{{ task_instance.xcom_pull('create_emr_cluster', key='return_value') }}",
    step_id="{{ task_instance.xcom_pull(task_ids='add_steps', key='return_value')["
    + str(last_step)
    + "] }}",
    aws_conn_id="aws_default",
    dag=dag,
)

terminate_emr_cluster = EmrTerminateJobFlowOperator(
    task_id="terminate_emr_cluster",
    job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster', key='return_value') }}",
    aws_conn_id="aws_default",
    dag=dag,
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

#---#---# Define Task Dependencies #---#---#

start_operator >> [load_files_subdag, create_emr_cluster]  >> step_adder
step_adder >> step_checker >> terminate_emr_cluster
terminate_emr_cluster >> end_operator
