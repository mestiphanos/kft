class MergeMultipleHeaders:
    """
    A class that corrects and 
    formats multiple headers
    """
    def __init__(self,df,col_start_idx = 0,col_start_val = 'ተ.ቁ'):
        self.df = df
        self.set_header_idx(col_start_idx,col_start_val)
        
        
    def get_correct_format_columns(self):
        """
        Flatten and join headers
        found on multiple rows
        """
        columns = []
        for column in self.df.columns.tolist():
            columns.append(('.').join([str(item) for item in column if not str(item).startswith("Unnamed") and str(item) != 'nan']))
        return columns

    def get_df(self,filename,s_name,header_start=0,header_end=0):
        """
        Get the dataframe
        """
        if header_start:
            self.header_start = header_start
        if header_end:
            self.header_end = header_end
        

        self.df = pd.read_excel(filename,engine='openpyxl',header=list(range(self.header_start,self.header_end)),sheet_name=s_name)
        
        if self.header_start != self.header_end:
            columns = self.get_correct_format_columns()
            self.df.columns = columns
        # print(columns)
        return self.df

    def set_header_idx(self,col_start_idx,col_start_val):
        """
        set indexes for start
        and end of multiple
        headers
        """
        if not self.df.empty:
            if not self.df[self.df[col_start_idx] == col_start_val].empty:
                # the index where the manually set column name appears
                self.header_start = self.df.index[df[col_start_idx] == col_start_val].tolist()[0]
                # the new dataframe starting from the column name given
                df_new = self.df.loc[self.header_start:,:]
                # check if the second value that is not null from the chosen column
                if df_new.index[~df_new[col_start_idx].isna()].tolist()[1] - self.header_start == 1:
                    print("multiple columns not found")
                    self.header_end = self.header_start
                    
                else:
                    self.header_end = df_new.index[~df_new[0].isna()].tolist()[1]
                    print("Found multiple columns found")
                    print(self.header_start,self.header_end)
                   
            else:
                print("The content start key word given not found. check again using another keyword")
                print(self.df.head())
                exit()
          

