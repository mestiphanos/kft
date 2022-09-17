import boto3
import os
from kft_dp.helper_methods import get_datetime_now
from kft_dp.run_athena_query import *
from kft_dp.logger import Logger
import sys
logger = Logger(__name__).log()


class FeatureStore:
    """
        Store and track a feature in the feature
        store. By default if a feature with the 
        same path is pushed, The feature store 
        will be checked and the new feature will
        be added as a new version of that feature.
        If a different path is passed it will be
        stored as a version 1.
        """
    def __init__(self,tag):
        # self.path = path
        self.tag = tag
        # self.description = description
        self.version = 1
        self.feature_store_location = 's3://kft-lakehouse-staging/feature_store'
        

    def copy_to_feature_store(self,path,feature_name,file_ext,region='us-east-1'):
        s3_removed_path = path.replace("s3://","")
        source_bucket = s3_removed_path.split('/')[0]
        source_key = s3_removed_path.replace(f"{source_bucket}/",'')
        # print('source',source_bucket,source_key)
        feature_name = source_key.split("/")[-1].split(".")[0]
        print('feature_name',feature_name)
        if self.feature_name_exists(feature_name):
            self.version += 1

        print('version',self.version)
        destination_bucket,destination_key = self.prepare_feature_path(feature_name,file_ext)

        session = boto3.Session(region_name=region)
        s3_client = session.client('s3')
        s3_client.copy_object(
        CopySource = {'Bucket': source_bucket, 'Key': source_key},
        Bucket = destination_bucket,
        Key =  destination_key
        )


    def get_feature(self,detail=False):
        query = self.query_for_accessing_metadata()
        rq = RunAthenaQuery(query,"dataframe",**{'output_location':'test'})
        df = rq.query_results()
        logger.info(f"Getting {df['feature_name']} from the feature_store")
        if detail:
            logger.info(f"The description for this {df.loc[0,'description']}")
            logger.info(f"The version is {df.loc[0,'version']}")
            logger.info(f"The data started being tracked at {df.loc[0,'time_stamp']}")
        path = f"{self.feature_store_location}/{df.loc[0,'feature_name']}/{df.loc[0,'version']}/{df.loc[0,'tag']}/feature.csv"
        return pd.read_csv(path)

    def tag_exists(self):
        """
        Checks if there is a feature
        already stored in the feature store
        with this name
        """
        query = f""" SELECT feature_name from cse_table.feature_store_metadata where tag = '{self.tag}'; """
        try:
            rq = RunAthenaQuery(query,"dataframe",**{'output_location':'test'})
            df = rq.query_results()
            print("tag info",df.loc[0,'feature_name'])
            if df.empty:
                print("might be null")
                return False
            else:
                logger.error(f"The tag already exists with the feature_name {df.loc[0,'feature_name']}")
                return True

        except:
            logger.error("couldnot run the query to get the tag successfully")
        
            


    def feature_name_exists(self,feature_name):
        """
        Checks if there is a feature
        already stored in the feature store
        with this name
        """
        query = f""" SELECT max(version) as version from cse_table.feature_store_metadata where feature_name = '{feature_name}'; """
        try:
            rq = RunAthenaQuery(query,"dataframe",**{'output_location':'test'})
            df = rq.query_results()
            print("metadata info",df.loc[0,'version'])
            if df.empty:
                return False
            else:
                self.version = int(df.loc[0,'version'])
                return True
        except:
            logger.error("couldnot run the query to get the version successfully")
        


    def push_to_feature_store(self,path,description=''):
        """
        Stores a feature to a feature store
        and track it by inserting to athena
        table
        """
        if self.tag_exists():
            exit()
        filename = path.split("/")[-1]
        feature_name = filename.split(".")[0]
        file_ext = filename.split(".")[-1]
        try:
            self.copy_to_feature_store(path,feature_name,file_ext)
            logger.info("successfully copied to feature store")
        except Exception as e:
            raise e
        query = self.query_for_registering_metadata(feature_name,description)
        try:
            rq = RunAthenaQuery(query,"",**{'output_location':'test'})
            rq.query_results()
            logger.info("Insert metadata for the path to be tracked successfull")
        except:
            logger.error("Insert metadata for the path to be tracked failed")
        

    
    def query_for_registering_metadata(self,feature_name,description):
        """
        Prepares the query to insert
        values necessary to track a 
        feature which are 
        ( time_stamp string,feature_name string, 
        tag string, version int, description string
        """
        now,_ = get_datetime_now()
        query = f""" INSERT INTO cse_table.feature_store_metadata (time_stamp,feature_name,
                            tag, version, description) VALUES ('{now}','{feature_name}',
                            '{self.tag}',{self.version},'{description}') ;"""
        return query

    def query_for_accessing_metadata(self):
        query = f""" SELECT * from cse_table.feature_store_metadata where tag = '{self.tag}'; """
        return query
        


    def prepare_feature_path(self,feature_name,file_ext):
        """
        Prepare the path the feature is
        going to be stored in.
        The way each feature is stored
        as filename/version/tag/feature
        """
        s3_removed_feature_store = self.feature_store_location.replace("s3://","")
        bucket = s3_removed_feature_store.split("/")[0]
        key = os.path.join(s3_removed_feature_store.replace(f"{bucket}/",''),feature_name,str(self.version),self.tag,f"feature.{file_ext}")
        # self.feature_path = f"{self.feature_store_location}/"\
        #                     f"{self.feature_name}/{self.version}/{self.tag}/"\
        #                     f"feature.{self.file_ext}"
        return bucket,key




