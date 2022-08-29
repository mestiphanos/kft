def get_unique_counts(df,cols=[]):
    """
    Get unique values for each column
    and their counts
    """
    if not cols:
        cols = df.columns.tolist()
    unique_dict = {col: df[col].value_counts().reset_index().sort_values(by=col,ascending=False) for col in cols} 
    common_values = {col: df[df[col] != 1]  for col,df in unique_dict.items()}
    return unique_dict,common_values

def convert_case(match_obj):
    """
    convert abbrevation
    to numerical
    """
    if match_obj.group(1) and match_obj.group(2) is not None:
        return match_obj.group(1) + match_obj.group(2) + '00000'
    
def transform_capital(df,col):
    """
    Transforming to same type
    for capital related cols
    """
    # cleaning string values from an integer column
    df[col] = df[col].str.replace('\D', '')
    # Cleaning column values using regex
    df[col] = df[col].replace(to_replace ='([0-9]+)(?:[.])([0-9]{3})(?: ሚ)', value = convert_case, regex = True)
    df[col] = df[col].replace(to_replace ='([0-9]+)(?: ሚ)', value = convert_case, regex = True)
    df[col] = df[col].replace(to_replace ='o', value = 0, regex = True)
    return df

def transform_employee_count(df,col):
    """
    Transforming to same type
    for employee counts
    """
    df[col] = df[col].replace(to_replace ='\xa02', value = None, regex = True)

