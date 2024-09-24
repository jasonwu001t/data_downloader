from TAI.source import BLS

bls = BLS(lookback_years=30)  # User can set any number of years

series_info = {
    'nonfarm_payroll': {'series_ids': ["CES0000000001"]},
    'unemployment_rate': {'series_ids': ["LNS14000000"], 'mom_diff': False},
    'us_job_opening': {'series_ids': ["JTS000000000000000JOL"]},
    'cps_n_ces': {'series_ids': ["CES0000000001", "LNS12000000"]},
    'us_avg_weekly_hours': {'series_ids': ["CES0500000002"]},
    'unemployment_by_demographic': {'series_ids': ["LNS14000009", "LNS14000006", "LNS14000003", "LNS14000000"]}
}

# Directory where data files will be stored
data_dir = "bls_data"

# Fetch and save historical data (initial fetch or complete refresh)
print("Fetching and saving historical data...")
bls.fetch_and_save_bls_data(series_info, file_format="json", start_year=bls.start_year, end_year=bls.end_year, mode='overwrite')

# Daily refresh to fetch only the latest data and append to historical data
print("\nPerforming daily refresh to update historical data with the latest year...")
bls.update_historical_data(series_info, file_format="json")
