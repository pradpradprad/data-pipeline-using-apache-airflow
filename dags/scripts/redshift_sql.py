# create schema
create_schema = 'CREATE SCHEMA IF NOT EXISTS report;'


# drop existing tables
drop_dim_province = 'DROP TABLE IF EXISTS report.dim_province CASCADE'
drop_dim_date = 'DROP TABLE IF EXISTS report.dim_date CASCADE'
drop_dim_time = 'DROP TABLE IF EXISTS report.dim_time CASCADE'
drop_fact_air_quality = 'DROP TABLE IF EXISTS report.fact_air_quality CASCADE'


# create main tables
create_dim_province = '''
CREATE TABLE IF NOT EXISTS report.dim_province (
    province_id         INT,
    province_name       VARCHAR(255) NOT NULL,
    latitude            DECIMAL(10, 4) NOT NULL,
    longitude           DECIMAL(10, 4) NOT NULL,
    PRIMARY KEY (province_id)
);
'''

create_dim_date = '''
CREATE TABLE IF NOT EXISTS report.dim_date (
    date_key            VARCHAR(8),
    date                DATE NOT NULL,
    year                INT NOT NULL,
    quarter             INT NOT NULL,
    month               INT NOT NULL,
    day                 INT NOT NULL,
    day_of_week         VARCHAR(3) NOT NULL,
    start_of_year       DATE NOT NULL,
    start_of_quarter    DATE NOT NULL,
    start_of_month      DATE NOT NULL,
    PRIMARY KEY (date_key)
);
'''

create_dim_time = '''
CREATE TABLE IF NOT EXISTS report.dim_time (
    time_key            VARCHAR(2),
    hour                INT NOT NULL,
    PRIMARY KEY (time_key)
);
'''

create_fact_air_quality = '''
CREATE TABLE IF NOT EXISTS report.fact_air_quality (
    record_id           INT IDENTITY(1, 1),
    record_date         VARCHAR(8) NOT NULL,
    record_time         VARCHAR(2) NOT NULL,
    province_id         INT NOT NULL,
    pm2_5               DECIMAL(10, 1) NOT NULL,
    aqi                 INT NOT NULL,
    PRIMARY KEY (record_id),
    FOREIGN KEY (record_date) REFERENCES report.dim_date(date_key),
    FOREIGN KEY (record_time) REFERENCES report.dim_time(time_key),
    FOREIGN KEY (province_id) REFERENCES report.dim_province(province_id)
);
'''

# sql list to setup tables
setup_tables = [

    create_schema,

    drop_dim_province, drop_dim_date, drop_dim_time, drop_fact_air_quality,

    create_dim_province, create_dim_date, create_dim_time, create_fact_air_quality

]


# load data from s3 to redshift table
load_dim_province = '''
COPY report.dim_province
FROM 's3://{{ params.bucket }}/dim_province/part-'
IAM_ROLE '{{ params.iam_role }}'
FORMAT AS PARQUET;
'''

load_dim_date = '''
COPY report.dim_date
FROM 's3://{{ params.bucket }}/dim_date/part-'
IAM_ROLE '{{ params.iam_role }}'
FORMAT AS PARQUET;
'''

load_dim_time = '''
COPY report.dim_time
FROM 's3://{{ params.bucket }}/dim_time/part-'
IAM_ROLE '{{ params.iam_role }}'
FORMAT AS PARQUET;
'''

load_fact_air_quality = '''
COPY report.fact_air_quality(record_date, record_time, province_id, pm2_5, aqi)
FROM 's3://{{ params.bucket }}/fact_air_quality/part-'
IAM_ROLE '{{ params.iam_role }}'
FORMAT AS PARQUET;
'''

# sql list to load data
load_data = [
    load_dim_province, load_dim_date, load_dim_time, load_fact_air_quality
]