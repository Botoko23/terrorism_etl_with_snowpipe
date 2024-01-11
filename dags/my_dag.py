from datetime import timedelta, datetime
from util.helper import Transfrom, get_secrets, multipart_upload, create_file_name, query_and_log

import boto3


from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator



default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': None,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(seconds=20)
}

s3 = boto3.client('s3')
transfer_config = boto3.s3.transfer.TransferConfig(
    multipart_threshold=20 * 1024 * 1024,  # Set the multipart upload threshold (20MB)
    max_concurrency=3  # Set the maximum number of concurrent threads for multipart upload
)

secretsmanager = boto3.client('secretsmanager', region_name='us-east-1')



log_bucket = 'logbucket-293449056871'
output_bucket = 'snowpipebucket-293449056871'
file_key2 = 'zipped/globalterrorismdb_2021Jan-June_1222dist.csv.gz'
file_key1 = 'zipped/globalterrorismdb_0522dist.csv.gz'

def get_zipped_files(**kwargs):
    # Get the object from S3
    saved_filenames = []
    bucket_name = kwargs['bucket_name']
    zipped_files = kwargs['zipped_files']
    for i, file in enumerate(zipped_files):
        saved_filename = file.split('/')[-1]
        obj = kwargs['boto3_client'].get_object(Bucket=bucket_name, Key=file)
        with open(saved_filename, 'wb') as local_file:
            local_file.write(obj['Body'].read())
            saved_filenames.append(saved_filename)

    kwargs['ti'].xcom_push(key='filenames', value=saved_filenames)

def transform(**context):

    print(*context['ti'].xcom_pull(task_ids='tsk_extract_data', key='filenames'))
    # filename2 = context['ti'].xcom_pull(task_ids='tsk_extract_data', key='filename_2')

    merged_df= Transfrom(*context['ti'].xcom_pull(task_ids='tsk_extract_data', key='filenames'))

    merged_df.dataframe['idate'] = merged_df.add_date_col('iyear', 'imonth', 'iday')

    merged_df.to_date('idate')

    merged_df.determine_doubtterr(1, 1, 1)
    cols_start_with_n = [col for col in merged_df.dataframe.columns if col.startswith('n') and not col.endswith('txt')]
    merged_df.dataframe[cols_start_with_n] = merged_df.fillna_for_certain_cols(cols_start_with_n)
    
    print(merged_df.dataframe.shape)
    merged_df.dataframe.to_csv('final_data.csv', index=False, sep='\t')

    context['ti'].xcom_push(key='file_to_upload', value='final_data.csv')

def s3_upload(**kwargs):
    file_path = kwargs['ti'].xcom_pull(task_ids='tsk_transform_data', key='file_to_upload')
    object_key = create_file_name('output/snowpipe/', 'data', 'csv')
    multipart_upload(kwargs['boto3_client'], kwargs['transfer_config'], file_path, 
                     kwargs['bucket_name'], object_key)

with DAG('my_dag',
        default_args=default_args,
        # schedule_interval = '@weekly',
        catchup=False) as dag:
    
        tsk_extract_data = PythonOperator(
        task_id= 'tsk_extract_data',
        python_callable=get_zipped_files,
        provide_context=True,
        op_kwargs={
            'boto3_client': s3,
            'bucket_name':log_bucket,
            'zipped_files': [file_key1, file_key2]
        }
        )

        tsk_transform_data = PythonOperator(
        task_id='tsk_transform_data',
        python_callable=transform,
        provide_context=True,
        )

        tsk_s3_upload = PythonOperator(
        task_id='tsk_s3_upload',
        python_callable=s3_upload,
        provide_context=True,
        op_kwargs={
             'boto3_client':s3,
             'transfer_config': transfer_config,
             'bucket_name': output_bucket
        }
        )

        tsk_wait_pipe = BashOperator(
            task_id='tsk_wait_pipe',
            bash_command="sleep 30s",
            dag=dag,
        )

        tsk_get_secrets = PythonOperator(
        task_id='tsk_get_secrets',
        python_callable=get_secrets,
        op_args=[secretsmanager, "snowflake/secrets"]
        )

        tsk_query_and_log = PythonOperator(
        task_id='tsk_query_and_log',
        python_callable=query_and_log,
        op_kwargs={
            'user': "{{ task_instance.xcom_pull(task_ids='tsk_get_secrets')['user'] }}",
            'password': "{{ task_instance.xcom_pull(task_ids='tsk_get_secrets')['password'] }}",
            'account': "{{ task_instance.xcom_pull(task_ids='tsk_get_secrets')['account'] }}",
            'boto3_client': s3,
            'bucket_name': log_bucket
        },
        trigger_rule='all_success'
        )

        tsk_extract_data >> tsk_transform_data >> tsk_s3_upload >> [tsk_get_secrets, tsk_wait_pipe] >> tsk_query_and_log






