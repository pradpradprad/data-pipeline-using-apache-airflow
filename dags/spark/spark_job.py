from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import (
    StructField, StructType, DecimalType, ArrayType, IntegerType,
    LongType, TimestampType, DoubleType, StringType, DateType
)

import sys
import logging
from typing import Union


# setting log level
logging.basicConfig(level=logging.INFO)


# schema
schema_raw = StructType([
    StructField('coord', StructType([
        StructField('lon', DecimalType(10, 4), True),
        StructField('lat', DecimalType(10, 4), True)
    ]), True),
    StructField('list', ArrayType(StructType([
        StructField('components', StructType([
            StructField('pm2_5', DecimalType(10, 2), True)
        ]), True),
        StructField('dt', LongType(), True)
    ]), True), True)
])

schema_breakpoints = StructType([
    StructField("c_low", DoubleType(), False),
    StructField("c_high", DoubleType(), False),
    StructField("i_low", IntegerType(), False),
    StructField("i_high", IntegerType(), False)
])

schema_transformed = StructType([
    StructField("datetime", TimestampType(), False),
    StructField("latitude", DecimalType(10, 4), False),
    StructField("longitude", DecimalType(10, 4), False),
    StructField("province_name", StringType(), False),
    StructField("pm2_5", DecimalType(10, 1), False),
    StructField("pm2_5_24h_avg", DecimalType(10, 1), False)
])


def read_data(spark: SparkSession, schema: StructType, bucket: str, file: str) -> DataFrame:
    """
    Read json data from s3 bucket.
    
    Parameter:
        spark: Spark session.
        schema: Dataframe schema.
        bucket: S3 raw bucket name.
        file: File name in bucket.
        
    Return:
        Air quality data as dataframe format.
    """
    
    df = spark \
        .read \
        .format('json') \
        .schema(schema) \
        .load(f's3://{bucket}/air_quality/{file}.json')
    
    return df


def transform(spark: SparkSession, df_raw: DataFrame, province: str, start: str, end: str) -> DataFrame:
    """
    Filter columns, fill null values and calculate rolling average in each province.
    
    Parameter:
        spark: Spark session.
        df_raw: Dataframe in raw format.
        province: Province name for current processing dataframe.
        start: Start timestamp (yyyy-MM-dd HH:mm:ss) Bangkok timezone.
        end: End timestamp (yyyy-MM-dd HH:mm:ss) Bangkok timezone.
        
    Return:
        Transformed dataframe.
    """
    
    # expand to hourly record
    df_transformed = df_raw \
        .withColumn('record', F.explode(F.col('list'))) \
        .withColumn('province_name', F.lit(f'{province}'))
    
    # filter columns
    df_transformed = df_transformed.select(
        F.col('coord.lat').alias('latitude'),
        F.col('coord.lon').alias('longitude'),
        F.col('province_name'),
        F.from_unixtime(F.col('record.dt')).cast(TimestampType()).alias('datetime'),
        F.col('record.components.pm2_5').alias('pm2_5')
    )
    
    # drop negative value
    df_transformed = df_transformed.filter(F.col('pm2_5') >= 0)
    
    # rounding to 1 decimal place
    df_transformed = df_transformed.withColumn('pm2_5', F.round(F.col('pm2_5'), 1).cast(DecimalType(10, 1)))
    
    # create datetime range dataframe
    df_datetime_range = spark.sql(f"select explode(sequence(timestamp('{start}'), timestamp('{end}'), interval 1 hour)) as datetime")
    
    # left join to get missing datetime records
    df_transformed = df_datetime_range.join(df_transformed, how='left', on='datetime').orderBy('datetime')
    
    # fill null values
    df_transformed = df_transformed.fillna({
        'latitude': df_transformed.select(F.round(F.mean('latitude'), 4).astype('float')).collect()[0][0],
        'longitude': df_transformed.select(F.round(F.mean('longitude'), 4).astype('float')).collect()[0][0],
        'province_name': df_transformed.select(F.mode('province_name')).collect()[0][0],
        'pm2_5': df_transformed.select(F.round(F.mean('pm2_5'), 1).astype('float')).collect()[0][0]
    })
    
    # define window specification for 24-hour rolling window
    windowSpec_24h = Window.orderBy('datetime').rowsBetween(-23, Window.currentRow)

    # calculate the rolling average concentration
    df_transformed = df_transformed.withColumn('pm2_5_24h_avg', F.avg('pm2_5').over(windowSpec_24h).cast(DecimalType(10, 1)))
    
    return df_transformed


def create_breakpoints_df(spark: SparkSession, schema: StructType) -> DataFrame:
    """
    Create dataframe for breakpoint ranges to calculate AQI.
    
    Parameter:
        spark: Spark session.
        schema: Dataframe schema.
        
    Return:
        Breakpoints dataframe.
    """
    
    return spark.createDataFrame([
        (0.0, 12.0, 0, 50),       # Good
        (12.1, 35.4, 51, 100),    # Moderate
        (35.5, 55.4, 101, 150),   # Unhealthy for Sensitive Groups
        (55.5, 150.4, 151, 200),  # Unhealthy
        (150.5, 250.4, 201, 300), # Very Unhealthy
        (250.5, 350.4, 301, 400), # Hazardous
        (350.5, 500.4, 401, 500)  # Hazardous
    ], schema=schema)


def calculate_aqi(df_total: DataFrame, df_breakpoints: DataFrame) -> DataFrame:
    """
    Calculate AQI value from pm2.5 24-hour rolling average concentration.
    
    Parameter:
        df_total: Total dataframe containing air quality from all provinces.
        df_breakpoints: Breakpoints dataframe.
        
    Return:
        Dataframe with AQI.
    """
    
    # crossjoin with breakpoint dataframe to get all breakpoint ranges
    df_aqi = df_total.join(F.broadcast(df_breakpoints), how='cross')
    
    # query only records that pm2.5 concentration fall within their breakpoint range
    df_aqi = df_aqi.filter(
        (F.col('pm2_5_24h_avg').between(F.col('c_low'), F.col('c_high'))) | (F.col('pm2_5_24h_avg') > 500.4)
    )
    
    # define equation
    aqi_equation = ((F.col('i_high') - F.col('i_low')) / (F.col('c_high') - F.col('c_low'))) * (F.col('pm2_5_24h_avg') - F.col('c_low')) + F.col('i_low')
    
    # calculate AQI from equation
    # for records where concentration exceed 500.4 μg/m3 will be assigned with maximum AQI of 500
    df_aqi = df_aqi.withColumn(
        'aqi',
        F.when(
            F.col('pm2_5_24h_avg') <= 500.4,
            F.round(aqi_equation, 0).cast(IntegerType())
        ).otherwise(500))
    
    # drop any duplicates
    df_aqi = df_aqi.dropDuplicates(subset=['datetime', 'province_name'])
    
    # filter columns
    df_aqi = df_aqi.select(
        'datetime',
        'latitude',
        'longitude',
        'province_name',
        'pm2_5',
        'aqi'
    )
    
    return df_aqi


def create_dim_province(df_aqi: DataFrame) -> DataFrame:
    """
    Create province dimension table dataframe.

    Parameter:
        df_aqi: Dataframe with AQI.

    Return:
        Province dimension table dataframe.
    """
    
    # select distinct province
    # create id column for each records
    # filter columns
    df_dim_province = df_aqi \
        .select('province_name', 'latitude', 'longitude').distinct() \
        .withColumn('province_id', F.row_number().over(Window.orderBy('province_name'))) \
        .select('province_id', 'province_name', 'latitude', 'longitude')
    
    return df_dim_province


def create_dim_date(df_aqi: DataFrame) -> DataFrame:
    """
    Create date dimension table dataframe.

    Parameter:
        df_aqi: Dataframe with AQI.

    Return:
        Date dimension table dataframe.
    """
    
    # select date components
    df_dim_date = df_aqi.select(
        F.date_format('datetime', 'yyyyMMdd').alias('date_key'),
        F.to_date('datetime').alias('date'),
        F.year('datetime').alias('year'),
        F.quarter('datetime').alias('quarter'),
        F.month('datetime').alias('month'),
        F.day('datetime').alias('day'),
        F.date_format('datetime', 'E').alias('day_of_week'),
        F.trunc('datetime', 'year').alias('start_of_year'),
        F.trunc('datetime', 'quarter').alias('start_of_quarter'),
        F.trunc('datetime', 'month').alias('start_of_month')
    ).distinct()
    
    return df_dim_date


def create_dim_time(df_aqi: DataFrame) -> DataFrame:
    """
    Create time dimension table dataframe.

    Parameter:
        df_aqi: Dataframe with AQI.

    Return:
        Time dimension table dataframe.
    """
    
    # select time components
    df_dim_time = df_aqi.select(
        F.date_format('datetime', 'HH').alias('time_key'),
        F.hour('datetime').alias('hour')
    ).distinct()
    
    return df_dim_time


def create_fact_air_quality(df_aqi: DataFrame, df_dim_province: DataFrame) -> DataFrame:
    """
    Create air quality fact table dataframe.

    Parameter:
        df_aqi: Dataframe with AQI.
        df_dim_province: Province dimension dataframe.

    Return:
        Air quality fact table dataframe.
    """
    
    # join with dim_province to get province_id
    df_fact_air_quality = df_aqi.join(F.broadcast(df_dim_province), on='province_name', how='inner')
    
    # filter columns
    df_fact_air_quality = df_fact_air_quality.select(
        F.date_format('datetime', 'yyyyMMdd').alias('record_date'),
        F.date_format('datetime', 'HH').alias('record_time'),
        'province_id',
        'pm2_5',
        'aqi'
    )
    
    return df_fact_air_quality


def write_data(df: DataFrame, bucket: str, output_dir: str) -> None:
    """
    Save output dataframe to s3 bucket.

    Parameter:
        df: Target dataframe.
        bucket: S3 processed bucket name.
        output_dir: Directory name.

    Return:
        None.
    """
    
    df.coalesce(1).write \
        .format('parquet') \
        .mode('overwrite') \
        .option('maxRecordsPerFile', 3000000) \
        .save(f's3://{bucket}/{output_dir}/')


def main():

    # check for command line arguments
    if len(sys.argv) != 6:
        logging.error('Number of arguments is incorrect')
        sys.exit(1)
    
    # variables
    province_list = sys.argv[1].split(',')
    raw_bucket = sys.argv[2]
    processed_bucket = sys.argv[3]
    start_datetime = sys.argv[4]
    end_datetime = sys.argv[5]
    
    try:
        # create spark session
        logging.info('Creating spark session.')
        spark = SparkSession \
            .builder \
            .appName('Transformation') \
            .config('spark.sql.session.timeZone', 'Asia/Bangkok') \
            .getOrCreate()

        # empty dataframe to collect data from all provinces
        df_total = spark.createDataFrame([], schema_transformed)

        logging.info('Start transforming.')
        
        # loop for each province data
        for province in province_list:
            
            # read raw data and transform
            df_raw = read_data(spark, schema_raw, raw_bucket, province)
            df_transformed = transform(spark, df_raw, province, start_datetime, end_datetime)
            
            # append transformed data into main dataframe
            df_total = df_total.union(df_transformed)

        # create breakpoints dataframe to calculate AQI
        df_breakpoints = create_breakpoints_df(spark, schema_breakpoints)

        # calculate AQI
        logging.info('Calculating AQI.')
        df_aqi = calculate_aqi(df_total, df_breakpoints)

        # create fact and dimension table dataframes
        logging.info('Creating fact and dimension tables.')
        df_dim_province = create_dim_province(df_aqi)
        df_dim_date = create_dim_date(df_aqi)
        df_dim_time = create_dim_time(df_aqi)
        df_fact_air_quality = create_fact_air_quality(df_aqi, df_dim_province)

        #write to s3
        logging.info('Writing data to s3 bucket.')
        write_data(df_dim_province, processed_bucket, 'dim_province')
        write_data(df_dim_date, processed_bucket, 'dim_date')
        write_data(df_dim_time, processed_bucket, 'dim_time')
        write_data(df_fact_air_quality, processed_bucket, 'fact_air_quality')

        logging.info('Finished all transformation steps.')
        
    except Exception as e:
        logging.error(f'Error occurred: {e}')
        sys.exit(1)

    finally:
        # end session
        logging.info('Ending spark session.')
        spark.stop()


# run
if __name__ == '__main__':
    main()