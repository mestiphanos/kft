# from scripts.lrw_cache_store import *
from datetime import datetime
from datetime import date
from datetime import timedelta


def return_sheet_names(filepath):
        """
        Get the sheet names
        found in the provided
        excel file
        """
        excel = pd.ExcelFile(filepath,engine='openpyxl')
        sheet_names = excel.sheet_names
        return sheet_names
    
def return_sheet_data(filepath,sheet_name,header=None):
    return pd.read_excel(filepath,engine='openpyxl',sheet_name=sheet_name,header=header)

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
        
def get_datetime_now():
    now = datetime.now()
    today = date.today()
    return now,today

def get_datetime_yesterday():
    _,today = get_datetime_now()
    yesterday = today - timedelta(days=1)
    # yesterday = yesterday.strftime("%Y-%m-%d")
    # date_now = today.strftime("%Y-%m-%d")
    # hour_now = now.hour

    