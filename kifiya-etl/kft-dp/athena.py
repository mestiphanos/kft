import pandas as pd
import boto3
# from scripts.logger import Logger
import pickle
from scripts.logger import Logger

logger = Logger(__name__).log()
def get_var_char_values(d):
    row_values = []
    for obj in d['Data']:
        if not bool(obj):
            row_values.append("")
        else:
            row_values.append(obj['VarCharValue'])
    return row_values

class AthenaQuery:
    """ 
    Run athena queries
    """
    def __init__(self,params,region_name = 'eu-west-1',wait=True):
        """
        Initialize parameters 
        """
        try:
            self.params = params
            self.region = region_name
            self.initialize_client()
        except Exception as e:
            logger.exception("Failed to initialize athena query parameters")
        
    def initialize_client(self):
        """
        Initialize to start athena
        client
        """
        try:
            session = boto3.Session(region_name=self.region)
            self.client = session.client('athena')
        except Exception as e:
            logger.exception("Failed to initialize athena client")
        
    def start_query(self,query=''):
        """
        Start athena query execution
        given parameters query string,
        database name and output location.
        Returns query id for the executed
        query
        """
        if query == '':
            query = self.params['query']
        response_query_execution_id = self.client.start_query_execution(
        QueryString = query ,
        QueryExecutionContext = {
            'Database' : self.params['database']
        },
        ResultConfiguration = {
            'OutputLocation': 's3://' + self.params['bucket'] + '/' + self.params['path']
                                }
        )
        return response_query_execution_id
    
    def create_named_query(self,query_name,description='',query=''):
        """ 
        Create a named query given
        query name, its description and
        the query that is to be named
        """
        if query == '':
             query = self.params['query']
        response = self.client.create_named_query(
        Name=query_name,
        Database=self.params['database'],
        QueryString=query,
        )
        return response
    
    def list_named_queries(self,max_results=50):
        """
        Lists named queries
        """
        response = self.client.list_named_queries(
#         NextToken=next_token,
        MaxResults=max_results
        )
        return response
    
    def get_named_query(self,query_id):
        """
        Return query information as a 
        dictionary of a query given its
        unique id.
        """
        response = self.client.get_named_query(
        NamedQueryId=query_id
        )
        return response
    
    def get_query_information(self,response_query_execution_id):
        """
        Returns query information 
        """
        execution_id = response_query_execution_id['QueryExecutionId']
        response_get_query_details = self.client.get_query_execution(
            QueryExecutionId = execution_id)
        return response_get_query_details
    
    def get_result(self,location):
        return pd.read_csv(location)
