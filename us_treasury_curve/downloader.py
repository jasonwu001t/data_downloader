from TAI.source import Treasury
from TAI.data import DataMaster
from datetime import datetime
import os
#
print('PROCESS STARTS AT : {}'.format(datetime.now()))
today = datetime.now()
cur_year = today.year

tr = Treasury()
dm = DataMaster()

base_file = 'treasury_yield_all.parquet'

current_dir = dm.get_current_dir()
data_dir_path = os.path.join(current_dir, 'data')
file_path = os.path.join(data_dir_path, base_file)  # .format(year))
json_file_path = os.path.join(
    data_dir_path, 'treasury_yield_all.json')  # .format(year))


def run_historical_data():  # Only need to run the first time
    historical_rates = tr.get_treasury_historical(start_year=1990,
                                                  end_year=2023)
    dm.create_dir()
    dm.save_local(historical_rates, 'data', base_file, delete_local=False)


def daily_refresh():  # update the latest data to the same csv file
    udpated_df = tr.update_yearly_yield(cur_year, base_data_file=base_file)

    # Save the updated parquet to both local and S3
    dm.save_local(udpated_df, 'data', base_file, delete_local=False)
    dm.save_s3(udpated_df,  'jtrade1-dir', 'data/us_treasury_yield',
               base_file, use_polars=False, delete_local=True)
    print('{} Data successfully updated and written to file.'.format(datetime.today()))

    # Save the updated JSON File to both local and S3
    df_dict = udpated_df.to_dict(orient='records')
    dm.save_local(df_dict, 'data', 'treasury_yield_all.json',
                  delete_local=False)
    dm.save_s3(df_dict,  'jtrade1-dir', 'api',
               'treasury_yield_all.json', use_polars=False, delete_local=True)


def print_local():
    df = dm.load_local(data_folder='data',
                       file_name=base_file,
                       use_polars=False,
                       load_all=False,
                       selected_files=[base_file])
    print(df)


run_historical_data()
daily_refresh()
print_local()

print('PROCESS ENDS AT : {}'.format(datetime.now()))
