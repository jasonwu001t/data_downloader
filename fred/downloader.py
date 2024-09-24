import json
import pandas as pd
from TAI.source import Fred  # Assuming the Fred class is saved in a file called FredClass.py
from TAI.data import DataMaster
from datetime import datetime

class FredToJson:
    def __init__(self):
        self.dm = DataMaster()
        self.client = Fred()
        # Combined mapping for chartType and description
        self.series_mapping = {
            'us_30yr_fix_mortgage_rate': {
                'chartType': 'line',
                'description': '30-year fixed mortgage rate in the US.'
            },
            'consumer_price_index': {
                'chartType': 'line',
                'description': 'Consumer Price Index for All Urban Consumers.'
            },
            'federal_funds_rate': {
                'chartType': 'line',
                'description': 'Federal Funds Rate set by the Federal Reserve.'
            },
            'gdp': {
                'chartType': 'line',
                'description': 'Gross Domestic Product (GDP) of the United States.'
            },
            'core_cpi': {
                'chartType': 'line',
                'description': 'Core Consumer Price Index, excluding food and energy.'
            },
            'fed_total_assets': {
                'chartType': 'line',
                'description': 'Total assets held by the Federal Reserve.'
            },
            'm2': {
                'chartType': 'line',
                'description': 'M2 money supply, a measure of the money supply.'
            },
            'unemployment_rate': {
                'chartType': 'line',
                'description': 'Unemployment rate in the US.'
            },
            'sp500': {
                'chartType': 'line',
                'description': 'S&P 500 Index, a measure of the US stock market.'
            },
            'commercial_banks_deposits': {
                'chartType': 'line',
                'description': 'Total deposits at commercial banks.'
            },
            'total_money_market_fund': {
                'chartType': 'line',
                'description': 'Total money market fund assets.'
            },
            'us_producer_price_index': {
                'chartType': 'line',
                'description': 'Producer Price Index for finished goods in the US.'
            }
        }

    def create_json_data(self, item):
        data = self.client.get_latest_release(item)
        json_data = []

        for date, value in data.items():
            # Replace NaN with None (which translates to null in JSON)
            if pd.isna(value):
                value = None
            json_data.append({
                "date": date.strftime('%Y-%m-%d'),  # Convert date to string
                "value": value
            })

        # Get the appropriate chartType and description from series_mapping
        chart_type = self.series_mapping[item]['chartType']
        description = self.series_mapping[item]['description']

        json_structure = [{
            "id": "1",
            "name": item,
            "category": "fred",
            "chartType": chart_type,
            "description": description,
            "chartData": json_data,
        }]
        
        return json_structure

    def save_to_json(self, filename, data):
        self.dm.save_local(data, 'data', filename,
                    delete_local=False)
        self.dm.save_s3(data,  'jtrade1-dir', 'api/fred',
                    filename, use_polars=False, delete_local=True)

        # with open(filename, 'w') as json_file:
        #     json.dump(data, json_file, indent=4)

if __name__ == "__main__":
    print('PROCESS STARTS AT : {}'.format(datetime.now()))
    fred_to_json = FredToJson()
    fred_to_json.dm.create_dir()
    # Loop over all the series in the series_mapping
    for series in fred_to_json.series_mapping.keys():
        json_data = fred_to_json.create_json_data(series)
        filename = f"{series}.json"
        fred_to_json.save_to_json(filename, json_data)
        print(f"{filename} saved successfully.")
    print('PROCESS ENDS AT : {}'.format(datetime.now()))