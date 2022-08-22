import pandas as pd
import re
from sklearn import preprocessing
from sklearn from sklearn.preprocessing import OneHotEncoder

class Label:
    
    def __init__(self):
        pass
    
    def label_encoder(self, df: DataFrame, col: str):
        """categorical column for a single column
        Parameters
        ----------
        df: Pandas Dataframe
            This is the dataframe containing the features and target variable.
        columns: str (target column to be label encoded)
        Returns
        -------
        The function returns a dataframe with the target variable encoded.
        """
        label_encoder = preprocessing.LabelEncoder() 
        df[col_names]= label_encoder.fit_transform(data[col_names]) 
        return df
    
    def ordinal_encoding(self, df, col, di):
        """ordinal enconding or direct mapping 
        Parameters
        ----------
        df: Pandas Dataframe
            This is the dataframe containing the features and target variable.
        columns: str (target column to be label encoded)
        di: dictionary ( the mapping between the previous and the new value)
        Returns
        -------
        The function returns a dataframe with the target variable encoded.
        """
        df[col] = df[col].map(di)
        return df
    
    def calculate_vif(data):
        """Function to calculate VIF
        used if the one hot encoding is in a dummy varible trap or not
        if the value is above >5 it is and need to drop one column"""
        vif_df = pd.DataFrame(columns = ['Var', 'Vif'])
        x_var_names = data.columns
        for i in range(0, x_var_names.shape[0]):
            y = data[x_var_names[i]]
            x = data[x_var_names.drop([x_var_names[i]])]
            r_squared = sm.OLS(y,x).fit().rsquared
            vif = round(1/(1-r_squared),2)
            vif_df.loc[i] = [x_var_names[i], vif]
        return vif_df.sort_values(by = 'Vif', axis = 0, ascending=False, inplace=False)
    
    def label_encode(df, columns):
        """Label encode the target variable fol a column of list.
        Parameters
        ----------
        df: Pandas Dataframe
            This is the dataframe containing the features and target variable.
        columns: list
        Returns
        -------
        The function returns a dataframe with the target variable encoded.
        """
        # Label Encoding

        label_encoded_columns = []
        # For loop for each columns
        for col in columns:
            # We define new label encoder to each new column
            le = preprocessing.LabelEncoder()
            # Encode our data and create new Dataframe of it,
            # notice that we gave column name in "columns" arguments
            column_dataframe = pd.DataFrame(
                le.fit_transform(df[col]), columns=[col])
            # and add new DataFrame to "label_encoded_columns" list
            label_encoded_columns.append(column_dataframe)

        # Merge all data frames
        df = df.drop(columns, axis=1)
        label_encoded_columns = pd.concat(label_encoded_columns, axis=1)
        label_encoded_columns = pd.concat([df,label_encoded_columns], axis=1)
        return label_encoded_columns
        

    def convert_onehot(self, df, col):
        """one hot enconded columns for a list of columns
        Parameters
        ----------
        df: Pandas Dataframe
            This is the dataframe containing the features and target variable.
        columns: str (target column to be one hot encoded encoded)
        Returns
        -------
        The function returns a dataframe with the target variable encoded.
        """
        
        df1 = pd.get_dummies(df[col], prefix=col, dummy_na=True, drop_first=True)
        df1.loc[df1[f'{col}_nan'] == 1, [df1.columns]] = np.nan 
        del df1[f'{col}_nan']
        df = pd.concat([df, df1], axis = 1)
        df = df.drop(df1, axis=1)
        return df
    
    def multi_hot_encode(self, df, col, di):
        """ordinal enconding or direct mapping 
        Parameters
        ----------
        df: Pandas Dataframe
            This is the dataframe containing the features and target variable.
        columns: str (target column to be label encoded)
        di: dictionary ( the mapping between the previous and the new value)
        Returns
        -------
        The function returns a dataframe with the target variable encoded.
        """
        
        # create a new column with the value to multi hot encoded
        df[f'{col}_new'] = df[col].map(di)
        # iterate and create new 4 columns in each row and add value to their specific mappings
        for index, row in df.iterrows():
            value = row[col]
            li = [*value]
            df.at[index,f'{col}_1'] = int(li[0])
            df.at[index,f'{col}_2'] = int(li[1])
            df.at[index,f'{col}_3'] = int(li[2])
            df.at[index,f'{col}_4'] = int(li[3])
        return df
    
    def literal_translation(self, text: list, di: dict):
        """literal translation from amaharic to english 
        Parameters
        ----------
        text: list 
            list of text needed to be translated
        di: dictionary ( the mapping between the amahric fidel and english characters)
        Returns
        -------
        The function returns a list of translated text 
        """
        
        list_text = []
        for j in range(len(text)):
            ltext = list(text[j])
            for index, i in enumerate(ltext):
                val = di.get(i)
                if(val):
                    ltext[index] = val
                a = "".join(ltext)
            list_text.append(a)
        return list_text