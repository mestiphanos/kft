import pandas as pd
import json
from pandas_profiling import ProfileReport

class doc(csv_file):
    self.dataset_name = csv_file
    self.df = pd.read_csv(csv_file)
    self.cols =  self.df.columns.tolist()
    self.shape = self.df.shape

    
def make_file(self):
    """
    creates a documentaion in a json format
    """
    ls = []
    missing_value_df = missing_value()
    profile = ProfileReport(self.self.df, title=f"{self.dataset_name}_Report")
    profile.to_file(f"{self.dataset_name}.html")
    for i in range(len(self.cols)):
        tpe = self.df.dtypes[self.cols[i]]  
        sample1 = self.df._get_value(10, self.cols[i])
        sample2 = self.df._get_value(30, self.cols[i])
        ls.append({'name': self.cols[i], 'type':str(tpe), 'missing_values':float(missing_value_df.round(2).iloc[i]), 'description':self.cols[i],'ex_col_val':[str(sample1), str(sample2)],'Abnormal_values':''})
    json_obj = 'doc.json'
    with open(json_obj, 'r') as json_file:
        doc = json.load(json_file)

    doc['Dataset_name'] = self.dataset_name
    doc['Description'] = f'The Description of the file'
    doc['analysis_site'] = f"{self.dataset_name}.html"
    doc['Dataset_shape'] = str(self.shape)
    doc['columns'] = ls
    with open(f"{dataset_name}.json", "w") as write_file:
        json.dump(doc, write_file, indent=4)

def missing_value(self):
    """
    returns a dataframe with each columns missing values percentages
    """
    percent_missing = (self.df.isnull().sum() * 100 / len(self.df)) 
    missing_value_df = pd.DataFrame({'percent_missing': percent_missing})
    return missing_value_df
                

