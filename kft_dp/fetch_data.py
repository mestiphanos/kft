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
    return 'merged_business_data'

def fetch(dry_run = False,table_name='merged_business_data',return_type='dataframe',output_location="kft_query_output",dataset_names=[]):
    query = f"""SELECT 
                *
                FROM
                {table_name}
             """
    if dataset_names:
        query += "WHERE "+(" or ").join([f"partition_0 = '{dataset_name}'" for dataset_name in dataset_names])
    else:
        logger.info("Insert data_set name:")
        logger.info("Choose one/more of the following")
        logger.info(return_partitions())
        logger.info("example would be: fetch(dataset_name(['agro_Sheet2','agro_Sheet3'])")
        exit()

    if dry_run:
        return query
    rq = RunAthenaQuery(query,return_type,**{'output_location':output_location})
    return rq.query_results()

