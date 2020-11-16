import glob
import csv
import yaml
import os
import pandas as pd
from os import listdir
import colorama
from config import engine, cursor, conn
# from glob import glob
from concurrent.futures import ThreadPoolExecutor as ThreadExecutor
import sqlalchemy as sa


colorama.init()


path = r"C:\Users\m.ivarsson\Desktop\FootballStats\football_data\England\test_data"


def list_csv_files(path, recursive=True):
    if recursive:
        csv_path = f"{path}/**/*.csv"
        csv_files_path = glob.glob(csv_path, recursive=recursive)
    return csv_files_path


def create_table_definitions(path):
    csv_files_list = list_csv_files(path)

    # Loop for every csv files paths
    created_tables_list = []
    for csv_file in csv_files_list:
        csv_file_schema = os.path.basename(os.path.dirname(csv_file))
        csv_file_name = os.path.basename(csv_file)[: -4]
        csv_file = open(csv_file)
        csv_reader = csv.reader(csv_file)
        csv_columns = next(csv_reader)
        create_table = "IF (EXISTS (SELECT *  FROM INFORMATION_SCHEMA.TABLES  " \
                       "WHERE TABLE_SCHEMA = '" + csv_file_schema + "' AND TABLE_NAME = '" + csv_file_name + "')) " \
                       "BEGIN DROP TABLE [" + csv_file_schema + "].[" + csv_file_name + "] END " \
                       "CREATE TABLE [" + csv_file_schema + "].[" + csv_file_name + "]" + "("

        # Loop for every column in csv_file
        for csv_column in csv_columns:
            column_name = "[" + csv_column + "] VARCHAR (255)"
            create_table += column_name + ", "
        # Remove last comma and add last bracket
        create_table = create_table[:-2]
        create_table += ")"

        # Create list of result, table_name + query
        created_tables_list.append([csv_file_name,csv_file_schema, create_table])

    return created_tables_list


def execute_create_table():
    created_tables_list = create_table_definitions(path)
    #print(created_tables_list)
    success = 0

    print(f" Start running BDL" )
    print(" ")

    for tables in created_tables_list:
        table_name = tables[0]
        table_schema = tables[1]
        sql_query = tables[2]
       # print(table_name)

        try:
            cursor.execute(sql_query)
            conn.commit()
            success += 1
            print(f" Created Table : {table_schema}.{table_name}")
        except:
            print("Error, something went wrong")


    print(" ")
    print( f" Total created tables : {success}" )



execute_create_table()

# def create_table_databases():
#     sql_queries = create_table_definitions(path)
#     for query in sql_queries:
#         print(query)
#
#         return pd.read_sql_query(query, con=engine)


# create_table_databases()














def generate_create_table_ddl(self, schema, table, columns):
    try:
        create_table_sql = "CREATE TABLE " + schema + "." + table + "(\n"
        for col in columns:

            ordinal_position = col[0]
            column_name = "[" + col[1] + "]"
            data_type = col[2]
            data_type = python_type_to_db_type(data_type)
            character_maximum_length = col[3]
            numeric_precision = col[4]
            numeric_scale = col[5]

            if data_type == "nvarchar":
                if character_maximum_length <= 0 or character_maximum_length > 4000:
                    column = column_name + " nvarchar(MAX)"
                else:
                    column = (
                        column_name
                        + " nvarchar"
                        + "("
                        + str(character_maximum_length)
                        + ")"
                    )
            elif data_type == "numeric":
                column = (
                    column_name
                    + " numeric("
                    + str(numeric_precision)
                    + ","
                    + str(numeric_scale)
                    + ")"
                )
            else:
                column = column_name + " " + data_type

            create_table_sql += column + ", \n"
        create_table_sql = create_table_sql[:-3]
        create_table_sql += ")"

        return create_table_sql
    except Exception as e:
        logger.error(e)
        logger.error("Failed generating create table script")



def insert_into_table():
    query = """
SELECT 
div
,date
,hometeam
FROM
[bdl-land].[dbo].[PL_1920]
"""
    return pd.read_sql_query(query, con=engine)


        # print(csv_files)



# print(csv_file_folder)
    #print(csv_files)
    #print(csv_file_folder)
















def import_into_temp_table(
    return_code,
    index,
    total,
    target,
    target_schema,
    target_table_tmp,
    temp_path_load,
    delimiter,
    load_name=None,
):
    try:
        csv_files = glob(os.path.join(temp_path_load, "*.csv"))
        target_schemas = []
        target_table_tmps = []
        temp_path_loads = []
        delimiters = []
        for file_path in csv_files:
            target_schemas.append(target_schema)
            target_table_tmps.append(target_table_tmp)
            temp_path_loads.append(file_path)
            delimiters.append(delimiter)

        table_workers = target._table_parallel_loads
        if len(temp_path_loads) < table_workers:
            table_workers = len(temp_path_loads)

        total_row_count = 0

        try:
            with ThreadExecutor(max_workers=table_workers) as executor:
                for row_count in executor.map(
                    target.import_file,
                    target_schemas,
                    target_table_tmps,
                    temp_path_loads,
                    delimiters,
                ):
                    total_row_count += row_count
                return_code = "RUN"
        except Exception as e:
            logger.error(e)
            return_code = "ERROR"

        # return_code, import_row_count = target.import_table(target_schema, target_table_tmp, temp_path_load, delimiter)
    except:
        return_code = "ERROR"
        #printer.print_load_line(
        #    index, total, return_code, load_name, msg="failed import into temptable"
        #)
    finally:
        return return_code, total_row_count


