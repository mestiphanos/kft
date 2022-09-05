from kft_dp.run_athena_query import *
from kft_dp.logger import Logger
from kft_dp.table_info import *
logger = Logger(__name__).log()

class FetchData:
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
        # self.columns = self.return_column_info()

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

