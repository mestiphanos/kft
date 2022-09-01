from kft_dp.glue import Glue

class SchemaRegistry(Glue):  
    """
    Inherits from the Glue
    class and contains methods
    related to creating/managing
    schemas found in the schema
    registry
    """
    def __init__(self,dry_run,region_name='eu-west-1',schema_name,registry_name,**kwargs):
        self.schema_name = schema_name
        self.registry_name = registry_name
        self.additional_params = kwargs
        super().__init__(dry_run,region_name):
        
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
        Tags= self.additional_params["schema"]['tags']
        SchemaDefinition= schema_info
        )
        return response

    def get_schema(self):
        """
        Get the schema information
        from the schema registry
        using the version and the
        schema name
        """
        latest_version = self.additional_params['schema']['latest_version']
        if not latest_version:
            version_number = self.additional_params['schema']['version_numb']
            
        schema_version_info= dict('LatestVersion': latest_version,
                                  'VersionNumber': version_number)
        schema_info = json.loads(self.client.get_schema_version(
        SchemaId={'SchemaName': self.schema_name,
            'RegistryName': self.registry_name
        }SchemaVersionNumber=schema_version_info)['SchemaDefinition'])
        return schema_info
