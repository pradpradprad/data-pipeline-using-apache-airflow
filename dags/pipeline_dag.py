from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.amazon.aws.transfers.local_to_s3 import LocalFilesystemToS3Operator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.amazon.aws.operators.emr import (
    EmrServerlessCreateApplicationOperator,
    EmrServerlessStartJobOperator,
    EmrServerlessDeleteApplicationOperator
)
from airflow.providers.amazon.aws.sensors.emr import EmrServerlessJobSensor
from airflow.utils.task_group import TaskGroup

from datetime import timedelta
import pendulum
import os

from scripts import utils, redshift_sql, extract


# define variables
province_list = utils.get_province_list()

start_datetime = os.getenv('START_DATE')
end_datetime = os.getenv('END_DATE')

start_date_unix = utils.convert_to_unix(start_datetime)
end_date_unix = utils.convert_to_unix(end_datetime)

raw_bucket = os.getenv('RAW_BUCKET')
processed_bucket = os.getenv('PROCESSED_BUCKET')
emr_bucket = os.getenv('EMR_BUCKET')

emr_role_arn = os.getenv('EMR_IAM')
redshift_role_arn = os.getenv('REDSHIFT_IAM')


# settings for DAG
default_args = {
    'owner': 'me',
    'depends_on_past': False,
    'start_date': pendulum.datetime(2025, 1, 1, tz='Asia/Bangkok'),
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    'dagrun_timeout': timedelta(minutes=30)
}


# define DAG
with DAG(
    'pipeline_dag',
    description='air quality ETL pipeline',
    default_args=default_args,
    schedule_interval=None
) as dag:
    
    # setup task group
    with TaskGroup('setup') as setup:

        # create aws connection
        create_aws_conn = PythonOperator(
            task_id='create_aws_conn',
            python_callable=utils.create_aws_conn
        )

        # create redshift connection
        create_redshift_conn = PythonOperator(
            task_id='create_redshift_conn',
            python_callable=utils.create_redshift_conn
        )

        # upload spark_job file to s3
        upload_spark_file = LocalFilesystemToS3Operator(
            task_id='upload_spark_file',
            filename='/opt/airflow/dags/spark/spark_job.py',
            dest_key='spark/spark_job.py',
            dest_bucket=f'{emr_bucket}',
            replace=True
        )

        # setup tables in redshift
        redshift_setup = SQLExecuteQueryOperator(
            task_id='redshift_setup',
            sql=redshift_sql.setup_tables,
            conn_id='redshift_conn'
        )

        create_aws_conn >> create_redshift_conn >> upload_spark_file >> redshift_setup


    # data extraction task group
    with TaskGroup('extraction') as extraction:
        
        # extract data from API to s3
        extract_data = PythonOperator(
            task_id='extract_data',
            python_callable=extract.extract_data,
            op_kwargs={
                'province_list': province_list,
                'start': start_date_unix,
                'end': end_date_unix
            }
        )

        # check for extracted files
        extraction_check = PythonOperator(
            task_id='extraction_check',
            python_callable=extract.file_check,
            op_kwargs={
                'province_list': province_list
            }
        )

        extract_data >> extraction_check


    # data transformation task group
    with TaskGroup('transformation') as transformation:

        # create spark application
        create_emr_spark_app = EmrServerlessCreateApplicationOperator(
            task_id='create_emr_spark_app',
            release_label='emr-7.2.0',
            job_type='SPARK',
            config={
                'name': 'emr-serverless-transformation',
                'maximumCapacity': {
                    'cpu': '16vCPU',
                    'memory': '64GB',
                    'disk': '100GB'
                },
                'autoStopConfiguration': {
                    'enabled': True,
                    'idleTimeoutMinutes': 5
                }
            },
            waiter_delay=10
        )

        # task returned application id
        application_id = create_emr_spark_app.output

        # start pyspark job in created application
        spark_submit_job = EmrServerlessStartJobOperator(
            task_id='spark_submit_job',
            application_id=application_id,
            execution_role_arn=emr_role_arn,
            job_driver={
                'sparkSubmit': {
                    'entryPoint': f's3://{emr_bucket}/spark/spark_job.py',
                    'entryPointArguments': [(',').join(province_list), raw_bucket, processed_bucket, start_datetime, end_datetime]
                }
            },
            configuration_overrides={
                'monitoringConfiguration': {
                    's3MonitoringConfiguration': {
                        'logUri': f's3://{emr_bucket}/emr-log/'
                    }
                }
            },
            name='ETL_Transformation',
            waiter_delay=10,
            waiter_max_attempts=180
        )

        # task returned job id
        job_id = spark_submit_job.output

        # check if the running job is finished
        check_job_status = EmrServerlessJobSensor(
            task_id='check_job_status',
            application_id=application_id,
            job_run_id=job_id
        )

        # delete application after finished spark job
        delete_emr_spark_app = EmrServerlessDeleteApplicationOperator(
            task_id='delete_emr_spark_app',
            application_id=application_id,
            waiter_delay=10
        )

        create_emr_spark_app >> spark_submit_job >> check_job_status >> delete_emr_spark_app


    # data loading task group
    with TaskGroup('loading') as loading:

        # load processed data from s3 into redshift
        load_to_redshift = SQLExecuteQueryOperator(
            task_id='load_to_redshift',
            sql=redshift_sql.load_data,
            conn_id='redshift_conn',
            params={
                'bucket': processed_bucket,
                'iam_role': redshift_role_arn
            }
        )

        # final check after loading data
        data_quality_check = BashOperator(
            task_id='data_quality_check',
            bash_command='soda scan -d redshift_data_source -c \
                /opt/airflow/dags/soda/configuration.yml /opt/airflow/dags/soda/checks.yml'
        )

        load_to_redshift >> data_quality_check


    setup >> extraction >> transformation >> loading