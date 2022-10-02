from kft_dp.run_athena_query import *

def return_tables(database_name='credit_scoring'):
    return f"""SELECT table_name
              FROM   information_schema.tables 
              WHERE table_schema = '{database_name}'
          """
     
def return_schema_info(table_name):
    return f"""SELECT *
                FROM   information_schema.columns
                WHERE table_name = '{table_name}'
            """

def return_partition_info(table_name):
    try:
        return f"SHOW PARTITIONS {table_name}"
    except:
        logger.info(f"{table_name} is not partitioned")

def return_columns(table_name):
    return f"SHOW COLUMNS FROM {table_name}"

def return_all_cols_from_all_tables(database_name='credit_scoring'):
    return f"""SELECT * FROM information_schema.columns 
               WHERE table_schema = '{database_name}' 
            """

def return_col_info(table_name):
    return f"DESCRIBE {table_name}"


def runquery_decorator(database_name,query_generator,argument_passed = ''):
    if argument_passed:
        query = query_generator(argument_passed)
    else:
        query = query_generator()
    rq = RunAthenaQuery(query,"dataframe",**{'output_location':'table_info_query_results','database':database_name})
    return rq.query_results()
        
