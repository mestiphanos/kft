from kft_dp.schema_registry import *
from kft_dp.fd_utility import *
source_info = SchemaRegistry().return_sources()

def list_sources(stage=''):
        """ Lists all the available sources we have on
            the data lake in 'stage_0', 'stage_1' and 'stage_2' :
            Parameters
            ----------
            stage : string 
                The stage the data sources reside in, which 
                are currently in 'stage_0', 'stage_1' or 'stage_2'
                'stage_0' : contains raw data
                'stage_1' : contains cleaned data
                'stage_2' : contains feature data

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
    if stage == 'stage_0' or 'stage_2':
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
    
def save(df, name, description = ''):
    """ saves the file in a csv format in stage_2
        'stage_0' or 'stage_1' :
        Parameters
        ----------
        df : DataFrame 
            the dataframe that features has been added 
            and needs to be saved in stage_2
        name : string
            The name that the df needs to saved in.

        Returns
        -------
        None
    """
    
    
    doc = d.doc()
    if name.endswith(".csv"): 
        df.to_csv(f"s3://kft-lakehouse-processing/stage_2/{name}")
        doc.make_file(f"s3://kft-lakehouse-processing/stage_2/{name}", description=description)
    else:
        df.to_csv(f"s3://kft-lakehouse-processing/stage_2/{name}.csv")
        doc.make_file(f"s3://kft-lakehouse-processing/stage_2/{name}.csv", description=description)
