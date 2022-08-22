class Data:
    def __init__(self,filepath):
        self.filepath = filepath
        self.filename = self.filepath.split('/')[-1].replace(".xlsx","")
        self.filelocation = ('/').join(self.filepath.split('/')[0:-1])
        
    def return_sheet_names(self):
        """
        Get the sheet names
        found in the provided
        excel file
        """
        excel = pd.ExcelFile(self.filepath,engine='openpyxl')
        self.sheet_names = excel.sheet_names
        return self.sheet_names
    
    def get_data(self,sheet_name_list=[]):
        """
        Return the data 
        in the excel file
        """
        df_sheets = {}
        if not sheet_name_list:
            sheet_name_list = self.return_sheet_names()
        for s_name in sheet_name_list:
            print(s_name)
            df = return_sheet_data(s_name)
            df_sheets[s_name] = df
        return df_sheets
        
    def return_sheet_data(self,sheet_name):
        return pd.read_excel(self.filepath,engine='openpyxl',header=None,sheet_name=sheet_name)
    
    def return_same_schema_df(self):
        """
        Merges data frames with
        the same columns and return
        dictionary with a schema
        and the merged dataframe
        """
        df_list = self.get_data()
        df_col_list = []
        df_cont_list = {}
        for sheet_name,df in df_list.items():
            if not df.empty:
                df["data_source"] = self.filename+sheet_name
                df_cols = df.columns.tolist()
                if df_cols not in df_col_list:
                    df_col_list.append(df_cols)
                    df_cont_list[df_col_list.index(df_cols)] = [df]
                else:
                    df_cont_list[df_col_list.index(df_cols)].append(df)
        
        df_merged = {cols:pd.concat(df_list) for cols,df in df_cont_list.items()}
        
        return df_merged
        
    def fit(self):
        df_list = self.return_same_schema_df()
        return df_list
            
    
    def transform(self,transform_func):
        """
        Takes in a transform 
        function and applies it 
        to the data 
        """
        df_merged = self.return_same_schema_df()
        df_transformed = []
        sheet_count = 0
        for cols,df in df_merged.items():
            sheet_count += 1
            df_transformed.append(transform_func(df))
            self.write_file(df,filename=self.filename+"_"+str(sheet_count))
        return df_transformed
        
    
    def write_file(self,df,filelocation = "",filename="",file_format='.parquet'):
        """
        Write a dataframe to
        the specified format
        """
        if not filename:
            filename = self.filename
        if not filelocation:
            filelocation= self.filelocation
        df.to_csv(f"{self.filelocation}/{self.filename}{file_format}",index=False)
        print(f"successfully saved {formatted_file}")