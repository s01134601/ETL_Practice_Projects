import boto3
import configparser
import requests
import json

config = configparser.ConfigParser()
config.read_file(open('dwh.cfg'))

KEY = config.get('AWS', 'KEY')
SECRET = config.get('AWS', 'SECRET')
s3 = boto3.resource('s3', region_name='us-east-1', aws_access_key_id=KEY, aws_secret_access_key=SECRET)

offset = 0
batch_size = 10000
data_list = []
tax_years = [2020, 2021]
for tax_year in tax_years:
    while True:
        url = "https://datacatalog.cookcountyil.gov/resource/7pny-nedm.json?$offset={}&$limit={}&tax_year={}".format(
            offset, batch_size, tax_year)
        batch_data = requests.get(url).json()
        data_list.extend(batch_data)
        row_count = len(batch_data)
        offset += batch_size
        if row_count < batch_size:
            break
    json_string = json.dumps(data_list)
    obj = s3.Object('cook-county-bor-appeals-history', 'cook-county-bor-appeals-history'
                                                       '/bor={}'.format(tax_year))
    obj.put(Body=json_string)
