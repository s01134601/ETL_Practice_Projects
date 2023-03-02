import boto3
import configparser
import requests
import json
import pandas as pd

config = configparser.ConfigParser()
config.read_file(open('dwh.cfg'))

KEY = config.get('AWS', 'KEY')
SECRET = config.get('AWS', 'SECRET')
s3 = boto3.resource('s3', region_name='us-east-1', aws_access_key_id=KEY, aws_secret_access_key=SECRET)

offset = 0
batch_size = 10000
data_merged = pd.DataFrame()
tax_years = [2020, 2021]
for tax_year in tax_years:
    while True:
        url = "https://datacatalog.cookcountyil.gov/resource/7pny-nedm.csv?$offset={}&$limit={}&tax_year={}".format(
            offset, batch_size, tax_year)
        batch_data = pd.read_csv(url)
        data_merged = pd.concat([batch_data, data_merged], axis=0)
        row_count = len(batch_data)
        offset += batch_size
        if row_count < batch_size:
            break
    csv_data = data_merged.to_csv(index=False)
    obj = s3.Object('cook-county-bor-appeals-history', 'bor_{}.csv'.format(tax_year))
    obj.put(Body=csv_data)

# url = "https://datacatalog.cookcountyil.gov/resource/7pny-nedm.json?$limit=10&tax_year=2020"
# small_batch = requests.get(url).json()
# print(small_batch)
# json_string = json.dumps(small_batch)
# obj = s3.Object('cook-county-bor-appeals-history', 'bor_2020_small.json')
# obj.put(Body=json_string)
