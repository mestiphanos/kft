from kft_dp.glue import Glue
import json

class SchemaRegistry(Glue):  
    """
    Inherits from the Glue
    class and contains methods
    related to creating/managing
    schemas found in the schema
    registry
    """
    def __init__(self,registry_name='Stage_0',schema_name='',region_name='us-east-1',dry_run=False,**kwargs):
        self.schema_name = schema_name
        self.registry_name = registry_name
        self.additional_params = kwargs
        self.sources = {}
        super().__init__(dry_run,region_name)
        
    def create_registry(self):
        """
        Create a schema registry
        """
        response = self.client.create_registry(
        RegistryName=self.registry_name,
        Description=self.additional_params["registry"]['description'],
        Tags= self.additional_params["registry"]['tags']
        )
        return response
    
    def create_schema(self,schema_info):
        """
        Store a schema in the
        schema registry 
        """    
        response = self.client.create_schema(
        RegistryId={
            'RegistryName': self.registry_name
        },
        SchemaName= self.schema_name,
        DataFormat='JSON',
        Compatibility='FULL',
        Description=self.additional_params['schema']['description'],
        Tags= self.additional_params["schema"]['tags'],
        SchemaDefinition= schema_info
        )
        return response

    def get_schema(self,schema_name='',latest_version=True,version_number=1):
        """
        Get the schema information
        from the schema registry
        using the version and the
        schema name
        """
        if schema_name:
            self.schema_name = schema_name
        # latest_version = self.additional_params['schema']['latest_version']
        if not latest_version:
            version_number = self.additional_params['schema']['version_numb']
            schema_version_info ={'VersionNumber': version_number}
        else:   
            schema_version_info= {'LatestVersion': latest_version}
        
        schema_info = json.loads(self.client.get_schema_version(
        SchemaId={'SchemaName': self.schema_name,
            'RegistryName': self.registry_name
        },
        SchemaVersionNumber=schema_version_info)['SchemaDefinition'])
        return schema_info
    
    def list_schemas(self):
        """
        list_schemas
        """
        response = self.client.list_schemas(
        RegistryId={
            'RegistryName': self.registry_name
            
        }
        )
        return response
    

    def list_schema_info(self):
        schema_info = {schema['SchemaName']: self.get_schema(schema['SchemaName']) for schema in self.list_schemas()['Schemas']}
        return schema_info    

    def return_sources(self,stage=''):
        sources = {'stage_0': self.list_schema_info(),
                'stage_1': self.return_table_info(),'stage_2': self.list_schema_info()}
        if stage:
            return sources[stage]
        else:
            return sources
