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

def group_sub_sectors(df):
    # group subsectors
    df['የተሰማራበት ን/ዘርፍ'] = df['የተሰማራበት ን/ዘርፍ'].str.replace(r'\እንጨት.*', 'እንጨትና ብረታ ብረት ስራ', regex=True)
    df['የተሰማራበት ን/ዘርፍ'] = df['የተሰማራበት ን/ዘርፍ'].str.replace(r'\አግሮ.*', 'አግሮ ፕሮሰሲንግ', regex=True)
    df['የተሰማራበት ን/ዘርፍ'] = df['የተሰማራበት ን/ዘርፍ'].str.replace(r'\ጨርቃ.*', 'ጨርቃ ጨርቅና ልብስ ስፌት ስራ', regex=True)
    df['የተሰማራበት ን/ዘርፍ'] = df['የተሰማራበት ን/ዘርፍ'].str.replace(r'\ዕጀ.*', 'ዕደ ጥበብ፣ ጌጣጌጥና የማዕድን ውጤቶች ማምረት ስራ', regex=True)
    df['የተሰማራበት ን/ዘርፍ'] = df['የተሰማራበት ን/ዘርፍ'].str.replace(r'\ከማኑፋክቸሪንግ.*', 'ከማኑፋክቸሪንግ ዘርፍ ውጭ', regex=True)
    df['የተሰማራበት ን/ዘርፍ'] = df['የተሰማራበት ን/ዘርፍ'].str.replace(r'\ኬሚካልና.*', 'ኬሚካልና የኬሚካል ወጤቶች ስራ', regex=True)
    df['የተሰማራበት ን/ዘርፍ'] = df['የተሰማራበት ን/ዘርፍ'].str.replace(r'\የኮንስትራክሽን.*', 'የኮንስትራክሽን ግብዓቶች ማምረት ስራ', regex=True)
    df['የተሰማራበት ን/ዘርፍ'] = df['የተሰማራበት ን/ዘርፍ'].str.replace(r'\ አግሮ.*', 'አግሮ ፕሮሰሲንግ', regex=True)
    df['የተሰማራበት ን/ዘርፍ'] = df['የተሰማራበት ን/ዘርፍ'].str.replace(r'\ቆዳና.*', 'ቆዳና የቆዳ ወጤቶች ማምረት ስራ', regex=True)
    df['የተሰማራበት ን/ዘርፍ'] = df['የተሰማራበት ን/ዘርፍ'].str.replace(r'\ቆዳና.*', 'ቆዳና የቆዳ ወጤቶች ማምረት ስራ', regex=True)
    # group association types
    df['የአደረጃጀት ዓይነት'] = df['የአደረጃጀት ዓይነት'].str.replace(r'\በግ.*|በገል.*|ግል.*|ግለ.*|ቨግል|በግለ', 'በግል', regex=True)
    df['የአደረጃጀት ዓይነት'] = df['የአደረጃጀት ዓይነት'].str.replace(r'አክሲዬን|share|የንድ ሽርክና|ኃላፊነቱ.*', 'ማህበር', regex=True)
    df['የአደረጃጀት ዓይነት'] = df['የአደረጃጀት ዓይነት'].str.replace(r'ህብረት ስራ መህብር|ፒኤልሲ|ሼር ካምፓኒ|ፒኤልሲ', 'ማህበር', regex=True)
    df['የአደረጃጀት ዓይነት'] = df['የአደረጃጀት ዓይነት'].str.replace(r'.*ግ.*', 'በግል', regex=True)
    df['የአደረጃጀት ዓይነት'] = df['የአደረጃጀት ዓይነት'].str.replace(r'በገል|በራስ', 'በግል', regex=True)
    df['የአደረጃጀት ዓይነት'] = df['የአደረጃጀት ዓይነት'].str.replace(r'Sol.*', 'private', regex=True)
    df['የአደረጃጀት ዓይነት'] = df['የአደረጃጀት ዓይነት'].str.replace(r'\Ó.*', 'unknown', regex=True)
    df['የአደረጃጀት ዓይነት'] = df['የአደረጃጀት ዓይነት'].str.replace(r'I.*', 'unknown', regex=True)
    df['የአደረጃጀት ዓይነት'] = df['የአደረጃጀት ዓይነት'].str.replace(r'ዘዘ|መብቃት|uunknown', 'unknown', regex=True)
    df['የአደረጃጀት ዓይነት'] = df['የአደረጃጀት ዓይነት'].str.replace(r'ገ/ገ|”unknown', 'unknown', regex=True)
    df['የአደረጃጀት ዓይነት'] = df['የአደረጃጀት ዓይነት'].str.replace(r' አነስተኛ ', 'በግል', regex=True)
    df['የአደረጃጀት ዓይነት'] = df['የአደረጃጀት ዓይነት'].str.replace(r'Axion', 'unknown', regex=True)
    df['የአደረጃጀት ዓይነት'] = df['የአደረጃጀት ዓይነት'].str.replace(r'Uniyeenii|W.*|waldaa|S.*|s.*|E.*', 'ማህበር', regex=True)
    df['የአደረጃጀት ዓይነት'] = df['የአደረጃጀት ዓይነት'].str.replace(r'የለም|››', 'unknown', regex=True)
    df['የአደረጃጀት ዓይነት'] = df['የአደረጃጀት ዓይነት'].str.replace(r'በግል', 'private', regex=True)
    df['የአደረጃጀት ዓይነት'] = df['የአደረጃጀት ዓይነት'].str.replace(r'PLC|Family|ማህበር', 'association', regex=True)
    df['የአደረጃጀት ዓይነት'] = df['የአደረጃጀት ዓይነት'].str.replace(r'ታዳጊ', 'growing', regex=True)
    df['የአደረጃጀት ዓይነት'] = df['የአደረጃጀት ዓይነት'].str.replace(r'ብድር', 'loan', regex=True)
    # group 
    df['የተሰማራበት የስራ መስክ'] = df['የተሰማራበት የስራ መስክ'].str.replace(r'(^.*ወተት.*$)', 'የወተት ተዋፅኦና ውጤቶች', regex=True)
    df['የተሰማራበት የስራ መስክ'] = df['የተሰማራበት የስራ መስክ'].str.replace(r'(^.*ቆዳ.*$)', 'ቆዳና የቆዳ ውጤቶች', regex=True)
    df['የተሰማራበት የስራ መስክ'] = df['የተሰማራበት የስራ መስክ'].str.replace(r'(.*ደቦ.*$)', 'ዳቦ ማምረት', regex=True)
    df['የተሰማራበት የስራ መስክ'] = df['የተሰማራበት የስራ መስክ'].str.replace(r'(.*ሻይ.*$)|(.*ቁርስ.*$)', 'ካፌና ቁርስ ቤት', regex=True)
    df['የተሰማራበት የስራ መስክ'] = df['የተሰማራበት የስራ መስክ'].str.replace(r'(.*ጨርቃ.*$)', 'ጨርቃ ጨርቅና አልባሳት', regex=True)
    df['የተሰማራበት የስራ መስክ'] = df['የተሰማራበት የስራ መስክ'].str.replace(r'(.*ፅዳት.*$)|(.*ጽዳት.*$)', 'ጽዳት እቃዎች ማምርት', regex=True)
    df['የተሰማራበት የስራ መስክ'] = df['የተሰማራበት የስራ መስክ'].str.replace(r'(.*ጨርቃ.*$)', 'ልብስ ስፌት', regex=True)
    df['የተሰማራበት የስራ መስክ'] = df['የተሰማራበት የስራ መስክ'].str.replace(r'(.*ጨርቃ.*$)', 'ልብስ ስፌት', regex=True)
    df['የተሰማራበት የስራ መስክ'] = df['የተሰማራበት የስራ መስክ'].str.replace(r'እ/ብረታ ብረት ስራ', 'ብረታ ብረት ስራ', regex=True)
    df['የተሰማራበት የስራ መስክ'] = df['የተሰማራበት የስራ መስክ'].str.replace(r'ዳቦ.*', 'ዳቦ ማምረት', regex=True)
    df['የተሰማራበት የስራ መስክ'] = df['የተሰማራበት የስራ መስክ'].str.replace(r'^ጨ/ጨርቅ.*', 'ጨርቃ ጨርቅና አልባሳት', regex=True)
    df['የተሰማራበት የስራ መስክ'] = df['የተሰማራበት የስራ መስክ'].str.replace(r'የባልትና.*', 'ባልትና', regex=True)
    df['የተሰማራበት የስራ መስክ'] = df['የተሰማራበት የስራ መስክ'].str.replace(r'የእፅዋ.*', 'የእፅዋት ተዋጽኦ', regex=True)
    df['የተሰማራበት የስራ መስክ'] = df['የተሰማራበት የስራ መስክ'].str.replace(r'የቤት.*', 'የቤትና የቢሮ ዕቃዎች', regex=True)
    df['የተሰማራበት የስራ መስክ'] = df['የተሰማራበት የስራ መስክ'].str.replace(r'^እንጭት ስራ', 'እንጨት ሥራ', regex=True)
    df['የተሰማራበት የስራ መስክ'] = df['የተሰማራበት የስራ መስክ'].str.replace(r'ጥልፍ.*', 'ሽመና', regex=True)
    df['የተሰማራበት የስራ መስክ'] = df['የተሰማራበት የስራ መስክ'].str.replace(r'.*ቆዳ.*', 'ቆዳና የቆዳ ውጤቶች', regex=True)
    df['የተሰማራበት የስራ መስክ'] = df['የተሰማራበት የስራ መስክ'].str.replace(r'^ወፍጮ.*', 'ወፍጮ ቤት', regex=True)
    df['የተሰማራበት የስራ መስክ'] = df['የተሰማራበት የስራ መስክ'].str.replace(r'(^.*ቢሮ.*$)', 'የቤትና የቢሮ ዕቃዎች', regex=True)
    df['የተሰማራበት የስራ መስክ'] = df['የተሰማራበት የስራ መስክ'].str.replace(r'(^.*ዳቦ.*$)', 'ዳቦ ማምረት', regex=True)
