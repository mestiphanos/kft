from kft_dp.fetch_data import *

if __name__ == '__main__':
    fd = FetchData(dry_run=False,table_name='mse_data')
    print(fd.get_detail_info_tables())
    print(fd.get_detail_info_table())

