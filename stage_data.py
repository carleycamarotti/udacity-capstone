import configparser
import re
import psycopg2
import os

from sql_queries import create_staging_port_table, create_staging_country_table, create_staging_airport_table, create_staging_temperature_table, create_staging_demographic_table, create_staging_immigration_table, staging_airport_copy, staging_demographic_copy, staging_port_codes_copy, staging_country_codes_copy, staging_temperature_copy, staging_immigration_copy


import logging
logging.basicConfig(filename='staging.log', level=logging.DEBUG)
logging.getLogger("py4j").setLevel(logging.ERROR)

create_staging_table_queries = [
    create_staging_port_table, create_staging_country_table, 
    create_staging_airport_table, create_staging_temperature_table,
    create_staging_demographic_table, create_staging_immigration_table
]

copy_staging_table_queries = [
    staging_airport_copy, staging_demographic_copy, staging_port_codes_copy,
    staging_country_codes_copy, staging_temperature_copy, staging_immigration_copy
]


def load_staging_tables(cur, conn):
    """
    Load PARQUET files into staging tables
    
    """
    for query in copy_staging_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """
    Execute CREATE TABLE queries
    
    """
    for query in create_staging_table_queries:
        cur.execute(query)
        conn.commit()


def stage_data():
    """
    Connect to DB, create staging tables, copy data from S3 to DB
    
    """
    config = configparser.ConfigParser()
    config.read('dl.cfg')

    logging.info('Connecting to DB')
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    logging.info('Creating Staging Tables')
    create_tables(cur, conn)
    logging.info('Copying Staging Tables')
    load_staging_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    stage_data()