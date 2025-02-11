from airflow.models.connection import Connection
from airflow.exceptions import AirflowFailException
from airflow.utils.session import create_session

import pandas as pd
import pendulum
import os
import json


def get_province_list() -> list:
    """
    Get all provinces in Thailand from online csv.

    Parameter:
        None.
        
    Return:
        List of provinces.
    """
    
    # read and change column name
    df_province = pd.read_csv('https://raw.githubusercontent.com/prasertcbs/thailand_gis/refs/heads/main/province/province.csv')
    df_province = df_province.rename(columns={'ADM1_EN': 'province_name'})['province_name'].to_frame()

    return df_province['province_name'].to_list()


def convert_to_unix(timestamp: str) -> int:
    """
    Convert timestamp to Unix time in Bangkok timezone.
    
    Parameter:
        timestamp: Timestamp value.
        
    Return:
        Unix time.
    """
    
    dt = pendulum.from_format(timestamp, 'YYYY-MM-DD HH:mm:ss', tz='Asia/Bangkok')
    
    return dt.int_timestamp


def create_aws_conn():
    """
    Create aws_default airflow connection.

    Parameter:
        None.
    
    Return:
        None.
    """

    # get credentials
    AWS_ACCESS_KEY_ID = os.getenv('AWS_ACCESS_KEY_ID')
    AWS_SECRET_ACCESS_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')
    REGION_NAME = os.getenv('REGION_NAME')

    credentials = [
        AWS_ACCESS_KEY_ID,
        AWS_SECRET_ACCESS_KEY,
        REGION_NAME,
    ]

    # check existing credentials
    if not all(credentials):
        raise AirflowFailException('Environment variables were not passed.')

    # create session and add connection if not exists
    with create_session() as session:
        try:
            existing_conn = session.query(Connection).filter(Connection.conn_id=='aws_default').first()

            if existing_conn:
                print('Connection aws_default already exists.')

            else:

                aws_conn = Connection(
                    conn_id='aws_default',
                    conn_type='aws',
                    login=AWS_ACCESS_KEY_ID,
                    password=AWS_SECRET_ACCESS_KEY,
                    extra=json.dumps({'region_name': REGION_NAME})
                )

                session.add(aws_conn)
                session.commit()

                print('Connection aws_default created successfully.')

        except Exception as e:
            raise AirflowFailException(f'Error creating connection to Airflow :{e}')


def create_redshift_conn():
    """
    Create redshift_conn airflow connection.

    Parameter:
        None.
    
    Return:
        None.
    """

    # get credentials
    REDSHIFT_USERNAME = os.getenv('REDSHIFT_USERNAME')
    REDSHIFT_PASSWORD = os.getenv('REDSHIFT_PASSWORD')
    REDSHIFT_HOST = os.getenv('REDSHIFT_HOST')
    REDSHIFT_WORKGROUP_NAME = os.getenv('REDSHIFT_WORKGROUP_NAME')
    REDSHIFT_DB = os.getenv('REDSHIFT_DB')
    REGION_NAME = os.getenv('REGION_NAME')

    # check existing credentials
    credentials = [
        REDSHIFT_USERNAME,
        REDSHIFT_PASSWORD,
        REDSHIFT_HOST,
        REDSHIFT_WORKGROUP_NAME,
        REDSHIFT_DB,
        REGION_NAME
    ]

    if not all(credentials):
        raise AirflowFailException('Environment variables were not passed.')
    
    # create session and add connection if not exists
    with create_session() as session:
        try:
            existing_conn = session.query(Connection).filter(Connection.conn_id=='redshift_conn').first()

            if existing_conn:
                print('Connection redshift_conn already exists.')

            else:

                redshift_conn = Connection(
                    conn_id='redshift_conn',
                    conn_type='redshift',
                    host=REDSHIFT_HOST,
                    login=REDSHIFT_USERNAME,
                    password=REDSHIFT_PASSWORD,
                    schema=REDSHIFT_DB,
                    port=5439,
                    extra=json.dumps({
                        'is_serverless': True,
                        'serverless_work_group': REDSHIFT_WORKGROUP_NAME,
                        'region': REGION_NAME
                    })
                )

                session.add(redshift_conn)
                session.commit()

                print('Connection redshift_conn created successfully.')

        except Exception as e:
            raise AirflowFailException(f'Error creating connection to Airflow :{e}')