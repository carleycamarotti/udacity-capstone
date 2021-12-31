import configparser
import psycopg2
from sql_queries import staging_count_data_quality_check, staging_to_fact_data_quality_check

import logging
logging.basicConfig(filename='staging.log', level=logging.DEBUG)
logging.getLogger("py4j").setLevel(logging.ERROR)

def count_immigration_staging(cur, conn):
    """
    Ensure immigration data has made it to staging table
    
    """
    cur.execute(staging_count_data_quality_check)
    conn.commit()
    values = cur.fetchall()
    
    if values[0][0] < 1:
        logging.error("Staging immigration table returned no results".format(table))
        raise ValueError("Data quality check failed. Staging immigration returned no results")
        
    logging.info("Staging immigration table appears to be populated")
    

def count_immigration_fact(cur, conn):
    """
    Ensure immigration data has made it to fact table
    
    """
    cur.execute(staging_to_fact_data_quality_check)
    conn.commit()
    values = cur.fetchall()
    
    if values[0][0] > 0:
        logging.error("Fact immigration table returned zero rows")
        raise ValueError("Data quality check failed. Fact immigration returned no results")
        
    logging.info("Fact immigration table appears to be populated with staging data")
    


def check_data_quality():
    """
    Connect to DB, create staging tables, copy data from S3 to DB
    
    """
    config = configparser.ConfigParser()
    config.read('dl.cfg')

    logging.info('Connecting to DB')
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    logging.info('Data Quality Check 1')
    count_immigration_staging(cur, conn)
    logging.info('Data Quality Check 2')
    count_immigration_fact(cur, conn)

    conn.close()


if __name__ == "__main__":
    check_data_quality()