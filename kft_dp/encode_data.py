def encode_column_values(df,col_names:list):
    # list out unique values for each column
    for I, E in enumerate(col_names):
        # list out sorted unique values for each column
        sor_uniq = sorted(df[E].unique())
        print(E)
        Dict = create_dict(sor_uniq)
        df = replace_values(df,Dict,E)
    return df

def create_dict(val:list):
    # create dictionary for each unique values and encoded values 
    Dict= {}
    for I, E in enumerate(val):
        Dict[E] = I
    print(Dict)
    return Dict

def replace_values(df,Dict:dict,col_name):
    # replace the unique values with encoded values using the dictionary
    df=df.replace({col_name: Dict})
    return df
