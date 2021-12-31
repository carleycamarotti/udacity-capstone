import configparser
import re
import psycopg2
import os
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt

from sql_queries import imigrants_by_country, arrival_days

def load_immigrants_by_country():
    config = configparser.ConfigParser()
    config.read('dl.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    cur.execute(imigrants_by_country)
    conn.commit()
    values = cur.fetchall()
    conn.close()
    
    return pd.DataFrame(values, columns=['Country', 'Number of Immigrants'])


def load_arrival_day():
    config = configparser.ConfigParser()
    config.read('dl.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    cur.execute(arrival_days)
    conn.commit()
    values = cur.fetchall()
    conn.close()
    
    return pd.DataFrame(values, columns=['Day of Week', 'Number of Immigrants'])

