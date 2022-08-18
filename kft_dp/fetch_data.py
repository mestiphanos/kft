from kft_dp.run_athena_query import *
from kft_dp.logger import Logger
logger = Logger(__name__).log()



def fetch_all(table_name,return_type,output_location="kft_query_output",partition_0=""):
    query = f"""SELECT 
                *
                FROM
                {table_name}
             """
    logger.info(query)
    if partition_0:
        query += f"WHERE partition_0 = '{partition_0}'"
    rq = RunAthenaQuery(query,return_type,**{'output_location':output_location})
    return rq.query_results()

