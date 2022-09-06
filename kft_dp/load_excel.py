from kft_dp.helper_methods import *
from kft_dp.merge_multiple_headers import MergeMultipleHeaders

class LoadExcelFile:
    def __init_(self,merged):
        self.filepath = filepath
        self.filename = self.filepath.split('/')[-1].replace(".xlsx","")
        self.filelocation = ('/').join(self.filepath.split('/')[0:-1])
        self.data = {}
        self.display_data()
        self.merged = merged
    
    
    def get_dataframe_dict(self,sheet_name_list=[]):
        """
        Return a dataframe 
        after reading the 
        different sheets 
        found in the excel file
        """
        df_sheets = {}
        if not sheet_name_list:
            sheet_name_list = return_sheet_names(self.filepath)
        for s_name in sheet_name_list:
            df = return_sheet_data(self.filepath,s_name)
            # identify if it has multiple headers
            df_sheets[s_name] = df
        self.data = df_sheets


    def display_data(self,df_dict={}):
        """
        Display sample of the data
        found on each sheet of the
        excel sheet
        """
        dfs_display = self.get_dataframe_dict()
        if df_dict:
            dfs_display = df_dict
        for sheet_name,df in dfs_display:
            print(f"Data on sheet {sheet_name}")
            print("---------------------------------------------")
            display(df.head(10))
            print("")
            
    def return_selected_sheets(self,sheet_list=[]):
        """
        Return dataframes based on
        the provided list of sheet_names
        """
        choosen_dfs = {sheet_name: self.data[sheet_name] for sheet_name in sheet_list}
        return choosen_dfs
    
    def return_same_schema_df(self,df_dict={}):
        """
        Merges data frames with
        the same columns and return
        dictionary with a schema
        and the merged dataframe
        """
        # df_list = self.get_data()
        df_col_list = []
        df_cont_list = {}
        for sheet_name,df in self.formatted_dfs.items():
            if not df.empty:
                df["dataset_name"] = self.filename+sheet_name
                df_cols = df.columns.tolist()
                print(df_cols)
                if df_cols not in df_col_list:
                    df_col_list.append(df_cols)
                    df_cont_list[df_col_list.index(df_cols)] = [df]
                else:
                    df_cont_list[df_col_list.index(df_cols)].append(df)
        
        self.df_merged = [pd.concat(df) for df in df_cont_list.values()]
        self.schema_info = df_col_list
        return self.df_merged
    
    def choose_dataframes(self,choosen_sheet_info={}):
        """
        Accepts the selected sheets
        and additional information
        about each sheet that is going
        to be used for correcting their
        format
        """
        
        # choosen_sheets = list(choosen_sheet_info.keys())
        self.formatted_dfs = {}
        # self.dfs_chosen = {sheet_name:self.df_dict[sheet_name] for sheet_name in choosen_sheets}
        for sheet_name,df in self.df_dict.items():
            if sheet_name in choosen_sheets:
                sheet_info = choosen_sheet_info[sheet_name]
                self.has_multi_header = sheet_info.get("multi_header")
                print(choosen_sheet_info)
                if self.has_multi_header:
                    self.header = None
                    print("has multi header")
                    multi_header_info = sheet_info['merge_headers_param']
                    formatted_df = MergeMultipleHeaders(return_sheet_data(self.filepath,sheet_name),
                                         multi_header_info.get('check_col_idx'),
                                         multi_header_info.get('check_col_val')).get_df(self.filepath,sheet_name)
                    self.formatted_dfs[sheet_name] = formatted_df
        

