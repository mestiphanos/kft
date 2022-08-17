from run_athena_query import *

def fetch_all(table_name,return_type,output_location="kft_query_output",date=[]):
    query = f"""SELECT 
                *
                FROM
                {table_name}
             """
    if date:
        query += f"WHERE date BETWEEN '{date[0]}' and '{date[1]}'"
    rq = RunAthenaQuery(query,return_type,**{'output_location':output_location})
    return rq.query_results()

