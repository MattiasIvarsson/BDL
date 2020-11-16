import pandas as pd
import urllib.request
from sqlalchemy import create_engine
import pyodbc

config = {
    'server': 'localhost\BILLONG' ,
    'db': 'BDL-land',
    'user': 'admin',
    'pwd': 'T14he5971!'
}

conn = pyodbc.connect('Driver={SQL Server};'
                      'Server=' + config['server'] + ';'
                      'Database=' + config['db'] + ';'
                      'UID=' + config['user'] + ';'
                      'PWD=' + config['pwd'] + ';')
cursor = conn.cursor()


params = urllib.parse.quote_plus('Driver={SQL Server};'
                                 'Server=' + config['server'] + ';'
                                 'Database=' + config['db'] + ';'
                                 'UID=' + config['user'] + ';'
                                 'PWD=' + config['pwd'] + ';')

engine = create_engine("mssql+pyodbc:///?odbc_connect={}".format(params))
engine.fast_executemany = True


# SQL QUERIES




def sql_dbt_log_all():
    query = """
    SELECT  
        log_type
        ,log_date                           AS dbt_date
        ,event_started					    AS start_datetime
        ,CAST(event_started AS TIME)	    AS start_time_
        ,event_ended					    AS end_datetime	
        ,CAST(event_ended AS TIME)		    AS end_time
        ,run_time_second					AS run_time
        ,event_project						AS event_schema
        ,event_model						AS dbt_models 
        ,tot_run_time_second                AS tot_run_time

    FROM
        [DBT-DW-MI].[dbo].[log_table_PROD]
    WHERE
        log_type = 'DBT'
        AND CAST(log_date AS DATE) =  (SELECT 
                                            MAX(CAST(log_date AS DATE)) 
                                         FROM 
                                            [DBT-DW-MI].[dbo].[log_table_PROD] 
                                        WHERE 
                                            log_type='DBT')
"""
    return pd.read_sql_query(query, con=engine)
