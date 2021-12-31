import pandas as pd
import calendar
from pyspark.sql.types import DateType
from pyspark.sql import functions as f
from pyspark.sql.types import IntegerType, StringType, DoubleType
from pyspark.sql.window import Window
from pyspark.sql import SparkSession
from datetime import datetime, timedelta
import configparser
import re
import os

from code_mapping import port_codes, country_codes

import logging
logging.basicConfig(filename='staging.log', level=logging.DEBUG)
logging.getLogger("py4j").setLevel(logging.ERROR)


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config.get('aws', 'AWS_ACCESS_KEY_ID')
os.environ['AWS_SECRET_ACCESS_KEY']=config.get('aws','AWS_SECRET_ACCESS_KEY')
S3_STAGING = config.get('S3', 'STAGING')

def create_spark_session():
    spark = SparkSession \
    .builder \
    .config("spark.jars.repositories", "https://repos.spark-packages.org/") \
    .config("spark.jars.packages", "saurfang:spark-sas7bdat:2.0.0-s_2.11") \
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
    .enableHiveSupport().getOrCreate()
    
    return spark


def decode_i94visa(code):
    """
    Decode the I94 Visa codes
    
    Keyword arguments:
    code -- Visa value (int)
    
    """
    if code == 1:
        return 'Business'
    if code == 2:
        return 'Pleasure'
    if code == 3:
        return 'Student'
    return 'Invalid Visa Type'


def decode_mode(code):
    """
    Decode the I94 Mode codes
    
    Keyword arguments:
    code -- Mode value (int)
    
    """
    if code == 1:
        return 'Air'
    if code == 2:
        return 'Sea'
    if code == 3:
        return 'Land'
    return 'Not reported'


def convert_sas_datetime(dt):
    """
    Convert datetime from SAS data
    
    Keyword arguments:
    dt -- datetime value
    
    """
    if dt is None:
        return None
    return datetime(1960, 1, 1) + timedelta(days=int(dt))


def get_sas_day(days):
    """
    Get day value from SAS data
    
    Keyword arguments:
    dt -- datetime value
    
    """
    if days is None:
        return None
    return (datetime(1960, 1, 1) + timedelta(days=days)).day


def process_port_codes(spark, processed_path):
    """
    Process Port Codes
    
    Keyword arguments:
    spark -- Spark Context
    processed_path -- the S3 output data location
    """
    logging.info(f'Processing port codes {datetime.now()}')
    pc = spark.createDataFrame(port_codes)
    
    logging.info(f'Begin writting port codes {datetime.now()}')
    pc.write.mode("overwrite").parquet(processed_path + 'port_codes')
    logging.info(f'Finished writing port codes {datetime.now()}')
    
    
def process_country_codes(spark, processed_path):
    """
    Process Country Codes
    
    Keyword arguments:
    spark -- Spark Context
    processed_path -- the S3 output data location
    """
    logging.info(f'Processing country codes {datetime.now()}')
    cc_pdf = pd.DataFrame(list(country_codes.items()), columns=['country_code', 'country'])
    cc = spark.createDataFrame(cc_pdf)
    
    logging.info(f'Begin writting country codes {datetime.now()}')
    cc.write.mode("overwrite").parquet(processed_path + 'country_codes')
    logging.info(f'Finished writing country codes {datetime.now()}')


def process_airport_codes(spark, raw_path, processed_path):
    """
    Process Airport Codes
    
    
    Keyword arguments:
    spark -- Spark Context
    raw_path -- the input data location
    processed_path -- the S3 output data location
    
    """
    logging.info(f'Reading airport codes {datetime.now()}')
    airport = spark.read.option("mergeSchema", "true").csv('data/raw_data/airport-codes_csv.csv', inferSchema=True, header=True)
    
    airport = airport.select(['ident', 'type', 'name', 'elevation_ft',
                              'iso_country', 'municipality', 'gps_code', 'local_code', 'coordinates'])
    
    logging.info(f'Begin writing airport codes {datetime.now()}')
    airport.write.mode("overwrite").parquet(processed_path + 'airport_codes')
    logging.info(f'Finished writting airport codes {datetime.now()}')
    
    

def process_temperature(spark, raw_path, processed_path):
    """
    Process Temperature Data
    
    Keyword arguments:
    spark -- Spark Context
    raw_path -- the input data location
    processed_path -- the S3 output data location
    
    """
    logging.info(f'Reading temperature {datetime.now()}')
    temperature = spark.read.csv(f'{raw_path}/GlobalLandTemperaturesByCity.csv', header=True, inferSchema=True)
    
    norm_cols = [col[0].lower() + re.sub(r'(?!^)[A-Z]', lambda x: '_' + x.group(0).lower(), col[1:]) for col in temperature.columns]
    
    temperature = temperature.toDF(*norm_cols)
    temperature = temperature.dropna(subset=['average_temperature'])
    logging.info(f'Dropping temperatures {datetime.now()}')
    temperature = temperature.sort(['city', 'country', 'dt'], ascending=False).drop_duplicates(['city', 'country'])
    
    logging.info(f'Begin writing temperature {datetime.now()}')
    temperature.write.mode("overwrite").parquet(processed_path + 'temperature')
    logging.info(f'Finished writting temperatures {datetime.now()}')
    
    
def process_demographic(spark, raw_path, processed_path):
    """
    Process Demographic Data
    
    Keyword arguments:
    spark -- Spark Context
    raw_path -- the input data location
    processed_path -- the S3 output data location
    
    """
    logging.info(f'Reading demographics {datetime.now()}')
    demographic = spark.read.csv(f'{raw_path}/us-cities-demographics.csv', header=True, inferSchema=True, sep=';')
    norm_cols = [col.lower().replace(' ', '_').replace('-', '_') for col in demographic.columns]
    demographic = demographic.toDF(*norm_cols)
    
    logging.info(f'Begin writing demographics {datetime.now()}')
    demographic.write.mode("overwrite").parquet(processed_path + 'demographic')
    logging.info(f'Finished writting demographics {datetime.now()}')
    
    
def process_immigration_data(spark, processed_path):
    """
    Process I94 Data
    
    Keyword arguments:
    spark -- Spark Context
    processed_path -- the S3 output data location
    
    """
    i94visa_udf = f.udf(decode_i94visa, StringType())
    i94mode_udf = f.udf(decode_mode, StringType())
    i94date_udf = f.udf(convert_sas_datetime, DateType())
    i94day_udf = f.udf(get_sas_day, IntegerType())
    
    months = [m.lower() for m in list(calendar.month_abbr[1:])]
    
    logging.info('Begin loading immigration data')
    for mon in months:
        logging.info(f'Loading immigration data for {mon}')
        load_path = f'../../data/18-83510-I94-Data-2016/i94_{mon}16_sub.sas7bdat'
        i94 = spark.read.format('com.github.saurfang.sas.spark').load(load_path)
        #i94 = spark.read.parquet('sas_data/')
        i94 = i94 \
            .withColumn('year', i94['i94yr'].cast(IntegerType())) \
            .withColumn('month', i94['i94mon'].cast(IntegerType())) \
            .withColumn('origin_country_code', i94['i94cit'].cast(IntegerType()).cast(StringType())) \
            .withColumn('age', i94['i94bir'].cast(IntegerType())) \
            .withColumn('arrival_date', i94date_udf(i94['arrdate'])) \
            .withColumn('arrival_day', i94day_udf(i94['arrdate'])) \
            .withColumn('departure_date', i94date_udf(i94['depdate'])) \
            .withColumn('port_code', i94['i94port'].cast(StringType())) \
            .withColumn('mode', i94mode_udf(i94['i94mode'])) \
            .withColumn('visa_category', i94visa_udf(i94['i94visa']))
    
        i94 = i94.select([
            'cicid', 'year','month', 'origin_country_code', 'age', 'arrival_date',
            'arrival_day', 'departure_date', 'depdate', 'arrdate', 'port_code', 'mode', 'gender', 
            'visa_category', 'visatype'])
        
        logging.info(f'Writing immigration data for {mon}')
        i94.write.mode("append").partitionBy("year", "month", "arrival_day") \
                .parquet(f"{processed_path}immigration_data")
        logging.info(f'Finished writing immigration data for {mon}')
        break
    
def preprocess_data():
    spark = create_spark_session()
    raw_path = "data/raw_data"
    processed_path = "s3a://staging-immigration/"
    
    #process_port_codes(spark, processed_path)
    #process_country_codes(spark, processed_path)
    process_airport_codes(spark, raw_path, processed_path)    
    #process_temperature(spark, raw_path, processed_path)
    #process_demographic(spark, raw_path, processed_path)
    #process_immigration_data(spark, processed_path)
    
    spark.stop()
    

if __name__ == "__main__":
    preprocess_data()