import kft_dp

if __name__ == '__main__':
    # for listing all the source file names 
    print(kft_dp.list_sources())

    # get details of each source
    print(kft_dp.get_details('stage_1','payment'))
    print(kft_dp.get_details('stage_0','work_stopped.csv'))
    
    # get documentation for the methods exposed in __init__
    print(help(kft_dp))

    # get data by specifying the stage and source_name 
    print(kft_dp.get_data(stage='stage_0',source_name ='work_stopped.csv'))

