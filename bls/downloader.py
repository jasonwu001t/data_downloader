from TAI.source import BLS
from TAI.data import DataMaster
from datetime import datetime
import json


print('PROCESS STARTS AT : {}'.format(datetime.now()))

dm = DataMaster()

# BLS instance with a user-defined lookback period
bls = BLS(lookback_years=30)  # User can set any number of years

# Series information for various datasets
series_info = {
    'nonfarm_payroll': {'series_ids': ["CES0000000001"], 'name': "Nonfarm Payroll"},
    'unemployment_rate': {'series_ids': ["LNS14000000"], 'mom_diff': False, 'name': "Unemployment Rate"},
    'us_job_opening': {'series_ids': ["JTS000000000000000JOL"], 'name': "US Job Opening"},
    'us_avg_weekly_hours': {'series_ids': ["CES0500000002"], 'name': "US Average Weekly Hours"},
}

# Directory where data files will be stored
data_dir = "bls_data"

# Modify these methods in the BLS class to save data in the new structure
bls.fetch_and_save_bls_data(series_info, file_format="json",
                            start_year=bls.start_year, end_year=bls.end_year, mode='overwrite')

# bls.update_historical_data(series_info, file_format="json")

# Data processing and saving to S3
data_files = ['unemployment_rate', 'nonfarm_payroll',
              'us_avg_weekly_hours', 'us_job_opening']
bucket_name = 'jtrade1-dir'
s3_folder = 'api/bls'


agg_data = []
for idx, data_file in enumerate(data_files, start=1):
    output_data = []
    # print('dddddddd', idx, data_file)
    local_path = f'{data_dir}/{data_file}.json'
    data = dm.load_local(data_dir, f'{data_file}.json', load_all=False)

    # Restructure the data
    chart_data = [{"date": entry["date"], "value": entry["value"]}
                  for entry in data]

    output_entry = {
        "id": str(idx),
        "name": series_info[data_file]['name'],
        "category": "bls",
        "chartType": "line",
        "description": f"{series_info[data_file]['name']} data from BLS.",
        "chartData": chart_data
    }

    output_data.append(output_entry)
    agg_data.append(output_entry)

    dm.save_local(output_data, data_dir, f'{data_file}.json', use_polars=False)
    dm.save_s3(output_data, bucket_name, s3_folder,
               f'{data_file}.json', use_polars=False, delete_local=True)

# # Save the restructured data to S3
# restructured_data = json.dumps(output_data, indent=2)
dm.save_s3(agg_data, bucket_name, s3_folder,
           'bls_data.json', use_polars=False, delete_local=False)

print('PROCESS ENDS AT : {}'.format(datetime.now()))
