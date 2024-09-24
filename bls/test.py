"""IF Error, check if added api_key"""

from TAI.utils import ConfigLoader
import requests
import json
import pandas as pd
from datetime import datetime

auth = ''
lookback_years = 21
base_url = 'https://api.bls.gov/publicAPI/v2/timeseries/data/'
headers = {'Content-type': 'application/json'}
start_year = datetime.now().year - lookback_years
end_year = datetime.now().year

print (start_year, end_year)
data = json.dumps({
    "seriesid": ['LNS14000000'],
    "startyear": str(start_year),
    "endyear": str(end_year),
    "registrationkey": auth
})
response = requests.post(base_url, data=data, headers=headers)
json_data = json.loads(response.text)
# print (json_data)
all_data = []
for series in json_data['Results']['series']:
    series_id = series['seriesID']
    for item in series['data']:
        all_data.append({
            "series_id": [series_id],
            "year": item['year'],
            "period": item['period'],
            "value": float(item['value']),
        })
df = pd.DataFrame(all_data)
# print (df)
# df['date'] = pd.to_datetime(df['year'] + df['period'].str.replace('M', '-'), format='%Y-%m')
# df.sort_values(by=['series_id', 'date'], inplace=True)
print (df)
