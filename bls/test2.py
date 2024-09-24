# from TAI.utils import ConfigLoader
import requests
import json
import pandas as pd
from datetime import datetime
import os

class BLSAuth:
    def __init__(self):
        # config_loader = ConfigLoader()
        self.api_key =''

    def get_api_key(self):
        return self.api_key

class BLS:
    def __init__(self, lookback_years=19, data_directory="bls_data"):
        """
        Initialize the BLS class.

        Parameters:
        - lookback_years (int): Number of years to look back from the current year.
        - data_directory (str): Directory where data files will be stored.
        """
        auth = BLSAuth()
        self.api_key = auth.get_api_key()
        self.lookback_years = lookback_years  # User-defined lookback years
        self.base_url = 'https://api.bls.gov/publicAPI/v2/timeseries/data/'
        self.headers = {'Content-type': 'application/json'}
        self.start_year = datetime.now().year - self.lookback_years
        self.end_year = datetime.now().year
        self.data_directory = data_directory

        # Create the data directory if it doesn't exist
        os.makedirs(self.data_directory, exist_ok=True)

    def fetch_bls_data(self, series_ids, mom_diff=True, start_year=None, end_year=None):
        """
        Fetch BLS data for given series IDs within the specified date range.

        Parameters:
        - series_ids (list): List of BLS series IDs to fetch.
        - mom_diff (bool): Whether to calculate month-over-month differences.
        - start_year (int): Start year for data fetching.
        - end_year (int): End year for data fetching.

        Returns:
        - pd.DataFrame: DataFrame containing the fetched data.
        """
        if start_year is None:
            start_year = self.start_year
        if end_year is None:
            end_year = self.end_year

        max_years_per_request = 20
        total_years = end_year - start_year + 1
        date_ranges = []
        current_start_year = start_year
        while current_start_year <= end_year:
            current_end_year = min(current_start_year + max_years_per_request - 1, end_year)
            date_ranges.append((current_start_year, current_end_year))
            current_start_year = current_end_year + 1

        all_data = []
        for start, end in date_ranges:
            print(f"Fetching data from {start} to {end}")
            payload = {
                "seriesid": series_ids,
                "startyear": str(start),
                "endyear": str(end),
                "registrationkey": self.api_key
            }
            response = requests.post(self.base_url, json=payload, headers=self.headers)
            if response.status_code != 200:
                raise Exception(f"API request failed with status code {response.status_code}: {response.text}")
            json_data = response.json()

            # Check for API errors
            if json_data.get("status") != "REQUEST_SUCCEEDED":
                error_messages = json_data.get("message", [])
                raise Exception(f"BLS API Error: {error_messages}")

            for series in json_data['Results']['series']:
                series_id = series['seriesID']
                for item in series['data']:
                    # Exclude annual averages or non-monthly periods if necessary
                    if item['period'].startswith('M'):
                        all_data.append({
                            "series_id": series_id,
                            "year": int(item['year']),
                            "period": item['period'],
                            "value": float(item['value']),
                        })

        if not all_data:
            print("No data fetched for the given series and date range.")
            return pd.DataFrame()  # Return empty DataFrame if no data

        df = pd.DataFrame(all_data)
        df['month'] = df['period'].str.replace('M', '').astype(int)
        df['date'] = pd.to_datetime(df[['year', 'month']].assign(DAY=1))
        df.sort_values(by=['series_id', 'date'], inplace=True)
        if mom_diff:
            df['mom_diff'] = df.groupby('series_id')['value'].diff()  # Month-over-month difference per series
        return df

    def fetch_and_save_bls_data(self, series_info, file_format="csv", start_year=None, end_year=None):
        """
        Fetch BLS data for multiple series and save them in the specified format.

        Parameters:
        - series_info (dict): Dictionary containing series keys and their respective series IDs and settings.
        - file_format (str): Format to save the data ('csv' or 'parquet').
        - start_year (int): Start year for data fetching.
        - end_year (int): End year for data fetching.
        """
        dataframes = {}
        for key, value in series_info.items():
            print(f"Fetching data for series: {key}")
            dataframes[key] = self.fetch_bls_data(
                series_ids=value['series_ids'],
                mom_diff=value.get('mom_diff', True),
                start_year=start_year,
                end_year=end_year
            )

        self.save_data(dataframes, file_format)

    def save_data(self, dataframes, file_format="csv", mode='overwrite'):
        """
        Save the fetched dataframes to files.

        Parameters:
        - dataframes (dict): Dictionary of DataFrames to save.
        - file_format (str): Format to save the data ('csv' or 'parquet').
        - mode (str): Mode of saving ('overwrite' or 'append').
        """
        for name, df in dataframes.items():
            if df.empty:
                print(f"No data to save for series: {name}")
                continue

            file_path = os.path.join(self.data_directory, f"{name}.{file_format}")
            if file_format == "parquet":
                df.to_parquet(file_path, index=False)
            elif file_format == "csv":
                if mode == 'append' and os.path.exists(file_path):
                    df.to_csv(file_path, mode='a', header=False, index=False)
                else:
                    df.to_csv(file_path, mode='w', header=True, index=False)
            print(f"Data saved to {file_path}")

    def fetch_latest_data(self, series_info, file_format="csv"):
        """
        Fetch the latest year's data for daily refresh.

        Parameters:
        - series_info (dict): Dictionary containing series keys and their respective series IDs and settings.
        - file_format (str): Format to save the data ('csv' or 'parquet').

        Returns:
        - pd.DataFrame: DataFrame containing the latest data.
        """
        latest_year = datetime.now().year
        print(f"Fetching latest data for the year: {latest_year}")
        self.fetch_and_save_bls_data(series_info, file_format=file_format, start_year=latest_year, end_year=latest_year)

    def update_historical_csv(self, series_info, file_format="csv"):
        """
        Append the latest data to the historical CSV files and remove duplicates.

        Parameters:
        - series_info (dict): Dictionary containing series keys and their respective series IDs and settings.
        - file_format (str): Format of the main historical data files ('csv' or 'parquet').
        """
        latest_data = {}
        for key, value in series_info.items():
            print(f"Updating historical data for series: {key}")
            # Fetch latest data for the current series
            df_latest = self.fetch_bls_data(
                series_ids=value['series_ids'],
                mom_diff=value.get('mom_diff', True),
                start_year=datetime.now().year,
                end_year=datetime.now().year
            )

            if df_latest.empty:
                print(f"No new data fetched for series: {key}")
                continue

            # Define file paths
            historical_file = os.path.join(self.data_directory, f"{key}.csv")

            if os.path.exists(historical_file):
                # Load existing historical data
                df_historical = pd.read_csv(historical_file, parse_dates=['date'])
                print(f"Loaded historical data for series: {key} with {len(df_historical)} records.")
            else:
                # If historical file doesn't exist, initialize an empty DataFrame
                df_historical = pd.DataFrame()
                print(f"No existing historical data found for series: {key}. A new file will be created.")

            # Append latest data
            df_combined = pd.concat([df_historical, df_latest], ignore_index=True)

            # Drop duplicates based on 'series_id' and 'date'
            before_dedup = len(df_combined)
            df_combined.drop_duplicates(subset=['series_id', 'date'], inplace=True)
            after_dedup = len(df_combined)
            duplicates_removed = before_dedup - after_dedup
            if duplicates_removed > 0:
                print(f"Removed {duplicates_removed} duplicate records for series: {key}.")

            # Save the updated DataFrame back to CSV
            df_combined.sort_values(by=['date'], inplace=True)
            df_combined.to_csv(historical_file, index=False)
            print(f"Historical data for series: {key} updated. Total records: {len(df_combined)}.")

    def save_data_combined(self, dataframes, file_format="csv"):
        """
        Save combined dataframes to files without duplication logic.

        Parameters:
        - dataframes (dict): Dictionary of DataFrames to save.
        - file_format (str): Format to save the data ('csv' or 'parquet').
        """
        # This method can be used for initial historical data fetching
        self.save_data(dataframes, file_format=file_format, mode='overwrite')

if __name__ == "__main__":
    # Define lookback years when instantiating the class
    bls = BLS(lookback_years=25)  # User can set any number of years

    series_info = {
        # 'nonfarm_payroll': {'series_ids': ["CES0000000001"]},
        'unemployment_rate': {'series_ids': ["LNS14000000"], 'mom_diff': False},
        # 'us_job_opening': {'series_ids': ["JTS000000000000000JOL"]},
        # 'cps_n_ces': {'series_ids': ["CES0000000001", "LNS12000000"]},
        # 'us_avg_weekly_hours': {'series_ids': ["CES0500000002"]},
        # 'unemployment_by_demographic': {'series_ids': ["LNS14000009", "LNS14000006", "LNS14000003", "LNS14000000"]}
    }

    # Directory where data files will be stored
    data_dir = "bls_data"

    # Fetch and save historical data (initial fetch or complete refresh)
    print("Fetching and saving historical data...")
    bls.fetch_and_save_bls_data(series_info, file_format="csv", start_year=bls.start_year, end_year=bls.end_year)

    # Daily refresh to fetch only the latest data and append to historical data
    print("\nPerforming daily refresh to update historical data with the latest year...")
    bls.update_historical_csv(series_info, file_format="csv")
