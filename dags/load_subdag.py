from airflow import DAG
from operators.move import LoadS3Operator

def load_files_subdag(
    parent_dag_name,
    task_id,
    s3_bucket,
    files,
    keys,
    *args, **kwargs):
    dag =DAG(
        f"{parent_dag_name}.{task_id}",
        **kwargs
    )

    load_transform_script = LoadS3Operator(
    task_id='load_transform_script',
    dag=dag,
    filename=files[0],
    s3_bucket=s3_bucket,
    s3_key=keys[0],
    )

    load_quality_script = LoadS3Operator(
    task_id='load_quality_script',
    dag=dag,
    filename=files[1],
    s3_bucket=s3_bucket,
    s3_key=keys[1],
    )

    load_airport_data = LoadS3Operator(
    task_id='load_airport_data',
    dag=dag,
    filename=files[2],
    s3_bucket=s3_bucket,
    s3_key=keys[2],
    )

    load_temperature_data = LoadS3Operator(
    task_id='load_temperature_data',
    dag=dag,
    filename=files[3],
    s3_bucket=s3_bucket,
    s3_key=keys[3],
    )


    load_demographic_data = LoadS3Operator(
    task_id='load_demographic_data',
    dag=dag,
    filename=files[4],
    s3_bucket=s3_bucket,
    s3_key=keys[4],
    )

    return dag