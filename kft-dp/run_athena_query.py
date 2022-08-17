import time
from scripts.athena import AthenaQuery
from scripts.log_table import LogTable
from scripts.lrw_cache_store import delete_from_cache_store
from scripts.logger import Logger
import pandas as pd
logger = Logger(__name__).log()

class RunAthenaQuery:
    def __init__(self,query,return_type,**params):
        self.query = query
        self.return_type =return_type
        self.region = 'eu-west-1'
        self.database = params.get("database",'kft-staging')
        self.bucket = 'kft-lakehouse-staging'
        self.path = params.get("output_location")
        # self.dates = params.get("date",[])
        # self.template_name = params.get("template_name","")
        self.table_name = params.get("table_name","")
        self.initialize_athena_params()

    def initialize_athena_params(self):
        """
        Initialize parameters used for athena
        queries
        """
        # defines the partition structure from the
        # request date
        # folder_structure = ("/").join(str(self.dates.today()).split("-")) 
        self.params = {
            'region': self.region,
            'database': self.database,
            'bucket': self.bucket,
            'path': self.path,
            'query': self.query
            }
       
        
    def query_results(self,wait = True,iterations = 360 ): 
        """
        Run athena query 
        """
        ## iterations 360 means 30 mins
        try:
            aq = AthenaQuery(self.params)
            logger.info("Successfully created athena query class")
        except Exception as e:
            logger.exception("Failed to create athena query class.")

        try:
            aq_query_id = aq.start_query(self.params['query'])    
            logger.info(f"Athena query with query id{aq_query_id['QueryExecutionId']}started to run.")
                ## if you choose not to wait return execution
        ## query id
            if not wait:
                return aq_query_id['QueryExecutionId']
            else:
                self.query_output_name = aq_query_id['QueryExecutionId']
                ## If you choose to wait first get query information 
                aq_query_details = aq.get_query_information(aq_query_id)
                status = 'RUNNING'

                ## while waiting for the execution to finish
                while (iterations > 0):
                    iterations = iterations - 1
                    ## get query information and get the status
                    aq_query_details = aq.get_query_information(aq_query_id)
                    try:
                        status = aq_query_details['QueryExecution']['Status']['State']
                        logger.info(f"Status of started query is {status}")
                        try:
                            logger.info(f"Data scanned in Mb is {float(round(aq_query_details['QueryExecution']['Statistics']['DataScannedInBytes']/(1024*1024), 6))}")
                            logger.info(f"Time taken in seconds is {float(round(aq_query_details['QueryExecution']['Statistics']['TotalExecutionTimeInMillis']/1000,4))}")
                        except:
                            logger.info(f"")
                    except Exception as e:
                        logger.exception(f"Failed to get the status of started query.")
                    ## if status is failed stop the loop and return
                    ## why it failed
                    if (status == 'FAILED') or (status == 'CANCELLED') :
                        failure_reason = aq_query_details['QueryExecution']['Status']['StateChangeReason']
                        logger.info(f"query failed for {self.table_name}")
                        logger.error(failure_reason)
                        # self.log.update_log_table(aq_query_details)
                        if "CREATE" in self.params['query']:
                            delete_from_cache_store(self.table_name)
                        if self.return_type == 'dataframe':
                            return pd.DataFrame()
                        else:
                            return

                    ## if status is succeded get location of the query
                    ## result
                    elif status == 'SUCCEEDED':
                        location = aq_query_details['QueryExecution']['ResultConfiguration']['OutputLocation']
                        completion_date = aq_query_details['QueryExecution']['Status']['CompletionDateTime']
                        print("data_scanned in kb",round(aq_query_details['QueryExecution']['Statistics']['DataScannedInBytes']/1024, 4))
                        print("time taken in second",round(aq_query_details['QueryExecution']['Statistics']['TotalExecutionTimeInMillis']/1000,5))
                        logger.info(f"""Athena query completed successfully at {completion_date},Data successfully queried and stored in {location} for {self.table_name} table""")

                        if self.return_type == 'dataframe':
                            # try:
                            aq_query_result = aq.get_result(location)
                            logger.info(f"Query result retrieved successfully from {location} for {self.table_name} table.")
                            return aq_query_result
                        elif self.return_type == 'location':
                            # print(location)
                            return location
                            # except Exception as e:
                            #     logger.exception(f"Failed to get the query result from {location} for {self.table_name} table..")
            # round(response['QueryExecution']['Statistics']['DataScannedInBytes']/(1024*1024*1024)*5,4)
                        ## Function to get output results and get the results
                        # self.log.update_log_table(aq_query_details)
                        return
                    ## if status is neither failed or succeeded then sleep 
                    ## 5 secs and continue the loop 
                    else:
                        time.sleep(5)

                return False
        except Exception as e:
            logger.exception("Failed to start athena query.")
            return None