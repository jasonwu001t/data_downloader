from TAI.source import BLS
from TAI.data import DataMaster
from datetime import datetime


print('PROCESS STARTS AT : {}'.format(datetime.now()))

dm = DataMaster()

# BLS instance with a user-defined lookback period
bls = BLS(lookback_years=30)  # User can set any number of years

# Series information for various datasets
series_info = {
    'nonfarm_payroll': {'series_ids': ["CES0000000001"]},
    'unemployment_rate': {'series_ids': ["LNS14000000"], 'mom_diff': False},
    'us_job_opening': {'series_ids': ["JTS000000000000000JOL"]},
    # 'cps_n_ces': {'series_ids': ["CES0000000001", "LNS12000000"]},
    'us_avg_weekly_hours': {'series_ids': ["CES0500000002"]},
    # 'unemployment_by_demographic': {'series_ids': ["LNS14000009", "LNS14000006", "LNS14000003", "LNS14000000"]}
}

# Directory where data files will be stored
data_dir = "bls_data"

###########################################################################
# Fetch and save historical data (initial fetch or complete refresh)
bls.fetch_and_save_bls_data(series_info, file_format="json", start_year=bls.start_year, end_year=bls.end_year, mode='overwrite')
#############################################################################


# Daily refresh to fetch only the latest data and append to historical data
bls.update_historical_data(series_info, file_format="json")

# Data processing and saving to S3
data_files = ['unemployment_rate', 'nonfarm_payroll', 'us_avg_weekly_hours', 'us_job_opening']
bucket_name = 'jtrade1-dir'
s3_folder = 'api/bls'

for data_file in data_files:
    local_path = f'{data_dir}/{data_file}.json'
    data = dm.load_local(data_dir, f'{data_file}.json', load_all=False)
    dm.save_s3(data, bucket_name, s3_folder, f'{data_file}.json', use_polars=False, delete_local=True)

print('PROCESS ENDS AT : {}'.format(datetime.now()))
