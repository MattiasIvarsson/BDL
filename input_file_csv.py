# ADD IF EXIST STATEMENT
# ADD F string to us variable to put the table in the select query
# column to table -> land_created
# add number of rows
# LOOK AT dbt_test_coverage as syntax
# cursor.execute(F"IF EXISTS (SELECT * FROM " {database_name}"."{table_name}   \
#                 "BEGIN DROP TABLE "{database_name}"."{table_name}" END")
# DO the same for create table AND INSERT.
# ADD a in paramenter that define which database and schema and use csv or excel file for the table_name
# Try to figure out a way to load excel/csv file and dbt in one go, perhaps add the python in SQL? or in python run
#  start the dbt run i right directory
# TO SQL typ som from CSV
# Get csv into dataframe, merge/join all dataframes into one --> result into 1 sql table
# After scripts i done, try to do it in PREFECT

import pandas as pd
import pyodbc

# Import CSV

data = pd.read_excel(r'C:\Private\Economy\private_economy.xls')
# print(data)
df = pd.DataFrame(data, columns=['date_id', 'stock_id', 'current_value', 'rate', 'input', 'output'])
#print(df.to_sql())


# to_sql


config = {
    'server': 'localhost\BILLONG' ,
    'db': 'FOOTBALL-DBT',
    'user': 'admin',
    'pwd': 'T14he5971!'
}

conn = pyodbc.connect('Driver={SQL Server};'
                      'Server=' + config['server'] + ';'
                      'Database=' + config['db'] + ';'
                      'UID=' + config['user'] + ';'
                      'PWD=' + config['pwd'] + ';')


cursor = conn.cursor()


# IF EXISTS DROP TABLE
cursor.execute('''IF EXISTS (SELECT * FROM [ECONOMIC].[land].[economic_f_stocks]) 
               BEGIN DROP TABLE [ECONOMIC].[land].[economic_f_stocks] END''')
print(cursor)


# Create Table
cursor.execute('''CREATE TABLE [ECONOMIC].[land].[economic_f_stocks] 
               (date_id DATETIME
               ,stock_id INT
               ,current_value DECIMAL(18,2)
               ,rate DECIMAL(18,2)
               ,input DECIMAL(18,2)
               ,output DECIMAL(18,2)
               ,land_created DATETIME)''')


# Insert DataFrame to Table
for row in df.itertuples():
    cursor.execute('''  INSERT INTO [ECONOMIC].[land].[economic_f_stocks] 
                    (date_id, stock_id,current_value,rate,input,output,land_created)
                    VALUES (?,?,?,?,?,?,GETDATE())''',
                    row.date_id,
                    row.stock_id,
                    row.current_value,
                    row.rate,
                    row.input,
                    row.output,
                   )
    print(row)


conn.commit()