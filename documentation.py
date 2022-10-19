import pandas as pd
import json
import os, sys
from pandas_profiling import ProfileReport

class doc:
    
    def __init__(self):
        print("class uploaded")
        

    
    def make_file(self, csv_file, description):
        """
        creates a documentaion in a json format
        """
        ls = []
        dataset_name = csv_file.split('/')[3]
        df = pd.read_csv(csv_file)
        cols =  df.columns.tolist()
        shape = df.shape
        missing_value_df = self.missing_value(df)
        profile = ProfileReport(df, minimal=True, title=f"{dataset_name}_Report")
        profile.to_file(f"/mnt/kft-lakehouse-staging/html/{dataset_name}.html")
        for i in range(len(cols)):
            tpe = df.dtypes[cols[i]]  
            sample1 = df._get_value(10, cols[i])
            sample2 = df._get_value(30, cols[i])
            ls.append({'name': cols[i], 'type':str(tpe), 'missing_values':float(missing_value_df.round(2).iloc[i]), 'description':cols[i],'ex_col_val':[str(sample1), str(sample2)],'Abnormal_values':''})
        json_obj = 'doc.json'
        with open(json_obj, 'r') as json_file:
            doc = json.load(json_file)

        doc['Dataset_name'] = dataset_name
        doc['Description'] = f'{description}'
        doc['analysis_site'] = f"/mnt/kft-lakehouse-staging/html/{dataset_name}.html"
        doc['Dataset_shape'] = str(shape)
        doc['columns'] = ls
        doc['Path'] = csv_file
        
        if len(dataset_name) > 10:
            path = dataset_name[:10]
        else:
            path = dataset_name
        if os.path.isdir(f"/mnt/kft-lakehouse-staging/json_files/{path}"):
            pass
        else:
            os.mkdir(f"/mnt/kft-lakehouse-staging/json_files/{path}")
        with open(f"/mnt/kft-lakehouse-staging/json_files/{path}/{dataset_name}.json", "w") as write_file:
            json.dump(doc, write_file, indent=4)

    def missing_value(self, df):
        """
        returns a dataframe with each columns missing values percentages
        """
        percent_missing = (df.isnull().sum() * 100 / len(df)) 
        missing_value_df = pd.DataFrame({'percent_missing': percent_missing})
        return missing_value_df
                

