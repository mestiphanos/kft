from kft_dp.run_athena_query import *
from kft_dp.logger import Logger
logger = Logger(__name__).log()


def return_partitions():
    partitions = ['agro_Sheet2',
 'agro_Sheet3',
 'agro_agro',
 'agro_መጠጥ',
 'agro_እንሰሳት',
 'agro_እጸዋት',
 'mule_Garment',
 'mule_Textile',
 'mule_ቆዳ',
 'mule_የጨ',
 'mule_sheet2',
 '42_industry',
 'mule_ልደታ',
 'mule_ቂርቆስ',
 'mule_ቃሊተ1',
 'mule_ቃሊቲ',
 'mule_Arada',
 'mule_Sheet5',
 'mule_ቦሌ',
 'mule_ንፋስ ስልክ',
 'mule_ኮልፌ',
 'mule_የካ',
 'mule_ጉለሌ',
 'mule_addisketema',
 'minilik']
    return partitions

def return_columns(table_name='merged_business_data'):
    table_info = {
        "merged_business_data": 
        ["index",
        "dataset_name",
        "ምርመራ",
        "ስልክ ቁጥር",
        "ተ.ቁ",
        "ተቋሙ የሚጠቀም ግብዓት.አይነት",
        "ተቋሙ የሚጠቀም ግብዓት.አይነት.1",
        "ተቋሙ የሚጠቀም ግብዓት.የሀገር ውስጥ",
        "ተቋሙ የሚጠቀም ግብዓት.የውጭ ሀገር",
        "ተቋሙ የሚጠቀምበት ማሽን.ብዛት",
        "ተቋሙ የሚጠቀምበት ማሽን.አይነት",
        "ንዑስ ዘርፍ",
        "ዋና ዘርፍ",
        "የመስሪያ ቦታ ስፋት በካ.ሜ",
        "የሚያመርቱት የምርት መጠን (በዓመት)",
        "የሚያመርቱት የምርት ዓይነት",
        "የሚያመርቱት የምርት ዓይነት.1",
        "የሚፈልጉት የድጋፍ አይነት",
        "የማምረቻ ቦታ ሁኔታ.በኪራይ",
        "የማምረቻ ቦታ ሁኔታ.በግል",
        "የማምረቻ ቦታ ሁኔታ.ፓርክ",
        "የማምረቻ ቦታ ሁኔታ.ፓርክ.1",
        "የተመሰረተበት ዓ.ም",
        "የተቋሙ አድራሻ.ልዩ ቦታ",
        "የተቋሙ አድራሻ.ክ/ከተማ",
        "የተቋሙ አድራሻ.ወረዳ",
        "የተፈጠረ የስራ ዕድል.ቋሚ.ሴ",
        "የተፈጠረ የስራ ዕድል.ቋሚ.ወ",
        "የተፈጠረ የስራ ዕድል.ቋሚ.ድ",
        "የተፈጠረ የስራ ዕድል.ጊዜያዊ.ሴ",
        "የተፈጠረ የስራ ዕድል.ጊዜያዊ.ወ",
        "የተፈጠረ የስራ ዕድል.ጊዜያዊ.ድ",
        "የአደረጃጀት ዓይነት",
        "የኢንዱስትሪ ስም",
        "የካፒታል ሁኔታ.መነሻ",
        "የካፒታል ሁኔታ.አሁን ያለው",
        "የድርጅቱ አባላት ሁኔታ.ሲቋቋም.ሴ",
        "የድርጅቱ አባላት ሁኔታ.ሲቋቋም.ወ",
        "የድርጅቱ አባላት ሁኔታ.ሲቋቋም.ድ",
        "የድርጅቱ አባላት ሁኔታ.አሁን.ሴ",
        "የድርጅቱ አባላት ሁኔታ.አሁን.ወ",
        "የድርጅቱ አባላት ሁኔታ.አሁን.ድ",
        "የድርጅት ባለቤት መረጃ.ስም",
        "የድርጅት ባለቤት መረጃ.ዕድሜ",
        "የድርጅት ባለቤት መረጃ.ፆታ",
        "የገበያ መዳረሻ.የሀገር ውስጥ",
        "የገበያ መዳረሻ.የውጭ ሀገር",
        "የገብያ ሁኔታ.በሀገር ውስጥ",
        "የገብያ ሁኔታ.በውጭ ሀገር",
        "የድርጅት መረጃ.ክልል",
        "የድርጅት መረጃ.ዞን/ክፍለ ከተማ",
        "የድርጅት መረጃ.ወረዳ",
        "የድርጅት መረጃ.ከተማ",
        "የድርጅት መረጃ.ልዩ ቦታ",
        "ፍቃድ ያገኘበት ዓ.ም",
        "የተሰማራበት የስራ መስክ",
        "የኢንዱስትሪው ዓይነት (በትርጓሜ)",
        "የግብር ከፋይ መለያ ቁጥር",
        "የአባላት (ባለቤት) ብዛት.ወ",
        "የአባላት (ባለቤት) ብዛት.ሴ",
        "የአባላት (ባለቤት) ብዛት.ድ",
        "ከአባላት (ባለቤት) ውጭ የተፈጠረ የስራ ዕድል.ቋሚ.ወ",
        "ከአባላት (ባለቤት) ውጭ የተፈጠረ የስራ ዕድል.ቋሚ.ሴ",
        "ከአባላት (ባለቤት) ውጭ የተፈጠረ የስራ ዕድል.ቋሚ.ድ",
        "ከአባላት (ባለቤት) ውጭ የተፈጠረ የስራ ዕድል.ጊዜያዊ.ወ",
        "ከአባላት (ባለቤት) ውጭ የተፈጠረ የስራ ዕድል.ጊዜያዊ.ሴ",
        "ከአባላት (ባለቤት) ውጭ የተፈጠረ የስራ ዕድል.ጊዜያዊ.ድ",
        "ጠቅላላ የተፈጠ የስራ ዕድል.ቋሚ *.ወ",
        "ጠቅላላ የተፈጠ የስራ ዕድል.ቋሚ *.ሴ",
        "ጠቅላላ የተፈጠ የስራ ዕድል.ቋሚ *.ድ",
        "የኢንዱስትሪው ዓይነት"],
        "merged_business_data_cleaned":
        ['group_name', 
        'mobile_number', 
        'establishment_year', 
        'starting_capital',
       'current_capital', 
       'total_employee', 
       'dataset_name',
       'business_documentation', 
       'business_sector', 
       'business_region',
       'association_type', 
       'level_Startup',
       'level_Startup,\nGrowing,\nGraduated',
       'initial_source_of_capital_Own Family', 
       'woreda_new_1', 
       'woreda_new_2',
       'woreda_new_3', 
       'woreda_new_4']
    }
    return table_info[table_name]

def return_encoding_info(table_name):
    """
    Returns the possible values
    of an encoded column
    """

def return_tables():
    return 'merged_business_data'

def fetch(dry_run = False,cleaned=False,table_name='merged_business_data',cols = [],return_type='dataframe',output_location="kft_query_output",dataset_names=[]):
    if cleaned:
        table_name += "_cleaned"
    logger.info("If you want to fetch cols from the table, you can choose from the following:")
    logger.info(return_columns(table_name))
    logger.info("")
    query = "SELECT "
    selected_cols = "*"
    if cols:
        selected_cols = (" ,").join([f""" "{col}" """ for col in cols])  
    query += selected_cols + f"  FROM {table_name}"
    if dataset_names:
        query += " WHERE "+(" or ").join([f"partition_0 = '{dataset_name}'" for dataset_name in dataset_names])
    else:
        logger.info("Insert data_set name:")
        logger.info("Choose one/more of the following")
        logger.info(return_partitions())
        logger.info("example would be: fetch(dataset_name(['agro_Sheet2','agro_Sheet3'])")
        exit()

    if dry_run:
        return query
    rq = RunAthenaQuery(query,return_type,**{'output_location':output_location})
    df = rq.query_results()

    return df

