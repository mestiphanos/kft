from kft_dp.run_athena_query import *
from kft_dp.logger import Logger
from kft_dp.table_info import *
from kft_dp.schema_registry import *
import pandas as pd
import s3fs


logger = Logger(__name__).log()
source_info = SchemaRegistry().return_sources()

def fetch_data_s3(path):
    if path.endswith(".csv"):
        try:
            return pd.read_csv(path)
        except:
            logger.error(f"Error reading the file {path}")
    elif path.endswith("/"):
        s3 = s3fs.S3FileSystem(anon=False)
        files = s3.glob(f"{path}*.csv")
        df_list = []
        for file in files:
            df = pd.read_csv(f"s3://{file}")
            df["dataset_name"] = file.split("/")[-1].replace(".csv",'')
            df_list.append(df)
        df = pd.concat(df_list)
        return df




class FetchDataAthena:
    def __init__(self,dry_run = False,cleaned=False,table_name='mse_data',database_name='credit_scoring'):
        self.dry_run = dry_run
        self.cleaned = cleaned
        self.table_name = table_name
        self.database_name = database_name
        self.return_type ='dataframe'
        self.output_location = "kft_query_output"
        self.return_partition_info()
        self.return_columns()
        self.return_table_info()
        

        try:
            self.partitions = self.return_partition_info()
        except:
            self.partitions = []


    def return_columns(self):
        self.column_info = runquery_decorator(database_name= self.database_name,argument_passed = self.table_name,query_generator=return_col_info)
        
    def return_partition_info(self):
        self.partition_info = runquery_decorator(database_name= self.database_name,argument_passed = self.table_name,query_generator=return_partition_info)
        if not self.partition_info.empty:
            logger.info(f"The table {self.table_name} is not partitioned")
        
    def return_table_info(self):
        self.tables = runquery_decorator(database_name= self.database_name,argument_passed = self.database_name,query_generator=return_tables)
        self.table_info = runquery_decorator(database_name= self.database_name,argument_passed = self.database_name,query_generator=return_all_cols_from_all_tables)

    def get_detail_info_tables(self):
        logger.info("****************************************************")
        logger.info("Information about tables available")
        logger.info("Available tables")
        logger.info(self.tables)
        logger.info("----------------------------------------------------------")
        return self.table_info
        
    def get_detail_info_table(self):
        logger.info("**********************************************************")
        logger.info(f"Column Information for the {self.table_name}")
        logger.info(self.column_info)
        logger.info("----------------------------------------------------------")
        logger.info("**********************************************************")
        logger.info(f"Partition Information for the {self.table_name}")
        logger.info(self.partition_info)
        logger.info("----------------------------------------------------------")
        logger.info("")

    def return_encoding_info(table_name):
        """
        Returns the possible values
        of an encoded column
        """
        pass

    def run(self,info=False,cols = [],dataset_names=[]):
        if self.cleaned:
            self.table_name += "_cleaned"

        if info:
            logger.info("If you want to fetch cols from the table, you can choose from the following:")
            logger.info(self.column_info)
            logger.info("")

        query = "SELECT "
        selected_cols = "*"
        if cols:
            selected_cols = (" ,").join([f""" "{col}" """ for col in cols])  
        query += selected_cols + f"  FROM {self.table_name}"
        if dataset_names:
            query += " WHERE "+(" or ").join([f"partition_0 = '{dataset_name}'" for dataset_name in dataset_names])
        else:
            if self.partitions:
                if info:
                    logger.info("You can choose one/more of the following partitions")
                    logger.info(self.partitions)
                    logger.info("example would be: fetch(dataset_name(['agro_Sheet2','agro_Sheet3'])")

        if self.dry_run:
            return query
        rq = RunAthenaQuery(query,self.return_type,**{'output_location':self.output_location,'database':self.database_name})
        df = rq.query_results()

        return df


def list_sources(stage=''):
        """ Lists all the available sources we have on
            the data lake in 'stage_0' and 'stage_1 :
            Parameters
            ----------
            stage : string 
                The stage the data sources reside in, which 
                are currently either 'stage_0' or 'stage_1'
                'stage_0' : contains raw data
                'stage_1' : contains cleaned data

            Returns
            -------
            source_names : dict
                Source names found under the stages.
        """
        
        if stage:
            source_names = {stage : list(source_info.keys())}
        else:
            # source_names = [list(schema_name.keys()) for schema_name in ]
            source_names = {}
            for key in source_info.keys():
                source_names[key] = list(source_info[key].keys())

            return source_names

def get_details(stage='',source_name=''):
    """ Get detailed information about the provided data
        source from the data lake which can be either 
        raw/cleaned or in our case 'stage_0' or 'stage_1' :
        Parameters
        ----------
        stage : string 
            The stage the data sources reside in, which 
            are currently either 'stage_0' or 'stage_1'
            'stage_0' : contains raw data
            'stage_1' : contains cleaned data
        source_name : string
            The name of the source to be fetched, source_name
            is schema_name for raw/stage_0 data sources and
            table_name for cleaned/stage_1 data sources. All
            available sources name can be found by calling 
            list_sources().

        Returns
        -------
        detailed_source_info : dict
            Detailed schema information.
    """
    detailed_source_info = source_info[stage][source_name]
    return detailed_source_info

def get_data(stage,source_name):
    """ Fetch the source provided from the data lake 
        which can be either raw/cleaned or in our case
        'stage_0' or 'stage_1' :
        Parameters
        ----------
        stage : string 
            The stage the data sources reside in, which 
            are currently either 'stage_0' or 'stage_1'
            'stage_0' : contains raw data
            'stage_1' : contains cleaned data
        source_name : string
            The name of the source to be fetched, source_name
            is schema_name for raw/stage_0 data sources and
            table_name for cleaned/stage_1 data sources. All
            available sources name can be found by calling 
            list_sources().

        Returns
        -------
        df : DataFrame
            Data Fetched.
    """
    if stage == 'stage_0':
        path = source_info[stage][source_name]['path']
        if path:
            df = fetch_data_s3(path)
            return df
    elif stage == 'stage_1':
        db_name = source_info[stage][source_name]['DatabaseName']
        try:
            df = FetchDataAthena(table_name=source_name,database_name=db_name).run()
        except:
            df = fetch_data_s3(source_info[stage][source_name]['StorageDescriptor']['Location'])
        return df
