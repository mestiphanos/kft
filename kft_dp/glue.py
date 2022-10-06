from kft_dp.logger import Logger
import boto3
import time
logger = Logger(__name__).log()

class Glue:
    def __init__(self,dry_run,region_name='us-east-1'):
        self.region = region_name
        self.dry_run = dry_run
        self.client = self.initialize()

    def initialize(self):
        session = boto3.Session(region_name = 'us-east-1')
        client = session.client('glue')
        return client
    
    def get_table(self,table_name,database):
        response = client.get_table(
        DatabaseName=table_name,
        Name=database_name,
        )
        return response
    
    def wait_until_ready(self,crawler_name,abort_time = 600,retry_seconds=30) -> None:
        state_previous = None
        start = time.time()
        while True:
            response_get = self.client.get_crawler(Name=crawler_name)
            state = response_get["Crawler"]["State"]
            if state != state_previous:
                logger.info(f"Crawler {crawler_name} is {state.lower()}.")
                state_previous = state
            if state == "READY":  # Other known states: RUNNING, STOPPING
                return
            self.time_taken = time.time()-start
            if self.dry_run:
                logger.info(f"{state}")
                logger.info(f"{self.time_taken}")
            time.sleep(retry_seconds)

    def run_crawler(self,crawler_name):
        self.wait_until_ready(crawler_name)
        if self.dry_run:
            # logger.info("logtype passed to glue class", )
            logger.info(f"crawler name {crawler_name}")
        else:
            response_start = self.client.start_crawler(Name = crawler_name)
            assert response_start["ResponseMetadata"]["HTTPStatusCode"] == 200
            logger.info(f"Crawling {crawler_name}.")
            self.wait_until_ready(crawler_name)
            logger.info(f"{self.time_taken}")
            logger.info(f"Crawled {crawler_name}.")

    def get_partitions(self,tablename,database="",NextToken=""):
        # self.wait_until_ready()
        if NextToken:
            response_query_result = self.client.get_partitions(DatabaseName=database,
    TableName=tablename,NextToken=NextToken)
        else:
            response_query_result = self.client.get_partitions(DatabaseName=database,
        TableName=tablename)
        return response_query_result
