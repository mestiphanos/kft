import pandas as pd
import re
from sklearn import preprocessing
from sklearn from sklearn.preprocessing import OneHotEncoder

class Label:
    
    def __init__(self):
        pass
    
    def label_encoder(self, df: DataFrame, col: str):
        """categorical columns for a list of columns
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
        """Function to calculate VIF"""
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
        """Label encode the target variable.
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
    
    def remove_spaces(self, df, col):
        """remove trailing space from both sides"""
        df[col] = df[col].str.strip()
        return df
    
    def normalize_char_level_missmatch(self, input_token):
        rep1=re.sub('[ሃኅኃሐሓኻ]','ሀ',input_token)
        rep2=re.sub('[ሑኁዅ]','ሁ',rep1)
        rep3=re.sub('[ኂሒኺ]','ሂ',rep2)
        rep4=re.sub('[ኌሔዄ]','ሄ',rep3)
        rep5=re.sub('[ሕኅ]','ህ',rep4)
        rep6=re.sub('[ኆሖኾ]','ሆ',rep5)
        rep7=re.sub('[ሠ]','ሰ',rep6)
        rep8=re.sub('[ሡ]','ሱ',rep7)
        rep9=re.sub('[ሢ]','ሲ',rep8)
        rep10=re.sub('[ሣ]','ሳ',rep9)
        rep11=re.sub('[ሤ]','ሴ',rep10)
        rep12=re.sub('[ሥ]','ስ',rep11)
        rep13=re.sub('[ሦ]','ሶ',rep12)
        rep14=re.sub('[ዓኣዐ]','አ',rep13)
        rep15=re.sub('[ዑ]','ኡ',rep14)
        rep16=re.sub('[ዒ]','ኢ',rep15)
        rep17=re.sub('[ዔ]','ኤ',rep16)
        rep18=re.sub('[ዕ]','እ',rep17)
        rep19=re.sub('[ዖ]','ኦ',rep18)
        rep20=re.sub('[ጸ]','ፀ',rep19)
        rep21=re.sub('[ጹ]','ፁ',rep20)
        rep22=re.sub('[ጺ]','ፂ',rep21)
        rep23=re.sub('[ጻ]','ፃ',rep22)
        rep24=re.sub('[ጼ]','ፄ',rep23)
        rep25=re.sub('[ጽ]','ፅ',rep24)
        rep26=re.sub('[ጾ]','ፆ',rep25)
        #Normalizing words with Labialized Amharic characters such as በልቱዋል or  በልቱአል to  በልቷል  
        rep27=re.sub('(ሉ[ዋአ])','ሏ',rep26)
        rep28=re.sub('(ሙ[ዋአ])','ሟ',rep27)
        rep29=re.sub('(ቱ[ዋአ])','ቷ',rep28)
        rep30=re.sub('(ሩ[ዋአ])','ሯ',rep29)
        rep31=re.sub('(ሱ[ዋአ])','ሷ',rep30)
        rep32=re.sub('(ሹ[ዋአ])','ሿ',rep31)
        rep33=re.sub('(ቁ[ዋአ])','ቋ',rep32)
        rep34=re.sub('(ቡ[ዋአ])','ቧ',rep33)
        rep35=re.sub('(ቹ[ዋአ])','ቿ',rep34)
        rep36=re.sub('(ሁ[ዋአ])','ኋ',rep35)
        rep37=re.sub('(ኑ[ዋአ])','ኗ',rep36)
        rep38=re.sub('(ኙ[ዋአ])','ኟ',rep37)
        rep39=re.sub('(ኩ[ዋአ])','ኳ',rep38)
        rep40=re.sub('(ዙ[ዋአ])','ዟ',rep39)
        rep41=re.sub('(ጉ[ዋአ])','ጓ',rep40)
        rep42=re.sub('(ደ[ዋአ])','ዷ',rep41)
        rep43=re.sub('(ጡ[ዋአ])','ጧ',rep42)
        rep44=re.sub('(ጩ[ዋአ])','ጯ',rep43)
        rep45=re.sub('(ጹ[ዋአ])','ጿ',rep44)
        rep46=re.sub('(ፉ[ዋአ])','ፏ',rep45)
        rep47=re.sub('[ቊ]','ቁ',rep46) #ቁ can be written as ቊ
        rep48=re.sub('[ኵ]','ኩ',rep47) #ኩ can be also written as ኵ  
        
        return rep48