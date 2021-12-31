import configparser
import re
import psycopg2
import os
import logging

logging.basicConfig(filename='staging.log', level=logging.DEBUG)
logging.getLogger("py4j").setLevel(logging.ERROR)

from sql_queries import create_countries_table, create_ports_table, create_airports_table, create_demographics_table, create_time_table, create_fact_immigration_table, extract_countries, extract_ports, extract_airports, extract_demographics, extract_time_data, extract_immigration_data

create_fact_dim_tables = [
    create_countries_table, create_ports_table, 
    create_airports_table, create_demographics_table, 
    create_time_table, create_fact_immigration_table
]

extract_tables = [
    extract_countries, extract_ports, extract_airports,
    extract_demographics,extract_time_data, extract_immigration_data
]

def create_tables(cur, conn):
    """
    Execute CREATE TABLE queries
    
    """
    for query in create_fact_dim_tables:
        cur.execute(query)
        conn.commit()
        
        
def insert_fact_dim_tables(cur, conn):
    """
    Insert fact and dimension tables
    
    """
    for query in extract_tables[:-1]:
        cur.execute(query)
        conn.commit()
        


def extract_data():
    """
    Connect to DB, create fact and dimension tables, extract data into tables from staging tables
    
    """
    config = configparser.ConfigParser()
    config.read('dl.cfg')

    logging.info('Connecting to DB')
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    logging.info('Creating Fact and Dimension Tables')
    create_tables(cur, conn)
    logging.info('Inserting Fact and Dimension Tables')
    insert_fact_dim_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    extract_data()