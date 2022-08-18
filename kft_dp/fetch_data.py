from kft_dp.run_athena_query import *
from kft_dp.logger import Logger
logger = Logger(__name__).log()


def return_partitions():
    partitions = ['agro_Sheet2',
 'agro_Sheet3',
 'agro_agro',
 'agro_መጠጥ',
 'agro_እንሰሳት',
 'agro_እጸዋት',
 'mule_Garment',
 'mule_Textile',
 'mule_ቆዳ',
 'mule_የጨ',
 'mule_sheet2',
 '42_industry',
 'mule_ልደታ',
 'mule_ቂርቆስ',
 'mule_ቃሊተ1',
 'mule_ቃሊቲ',
 'mule_Arada',
 'mule_Sheet5',
 'mule_ቦሌ',
 'mule_ንፋስ ስልክ',
 'mule_ኮልፌ',
 'mule_የካ',
 'mule_ጉለሌ',
 'mule_addisketema',
 'minilik']
    return partitions

def return_tables():
    return 'merged_business_table'

def fetch_all(table_name,return_type='dataframe',output_location="kft_query_output",partition_0=""):
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

