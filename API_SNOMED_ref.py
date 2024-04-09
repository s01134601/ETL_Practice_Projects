import pandas as pd
from bs4 import BeautifulSoup
import requests
import snowflake.connector
import json


with open('./config.json', 'r') as file:
    config = json.load(file)
    db_config = config['database']

try:
    conn = snowflake.connector.connect(
        user = db_config['user'],
        password = db_config['password'],
        account = db_config['account'],
        warehouse = db_config['warehouse'],
        database = db_config['database'],
        schema = db_config['schema'],
        role = db_config['role']
    )
    cursor = conn.cursor()
    # try:
    #     cursor.execute("SELECT CURRENT_VERSION()")
    #     row = cursor.fetchone()
    #     print(f"Successfully connected to Snowflake! Snowflake version: {row[0]}")
    try:
        cursor.execute("SELECT distinct Diagnosis1 FROM SYNTHEA.Stage.CLAIMS")
        results = cursor.fetchall()
        code_list = [code[0] for code in results]
        print(len(code_list))
    finally:
        cursor.close()

finally:
    # Always ensure the connection is closed
    if 'conn' in locals():
        conn.close()

desc={}
for code in code_list:
    url = "https://tx.fhir.org/snomed/554471000005108-20210930/?type=snomed&id={}".format(code)
    html_page = requests.get(url, verify=False).text
    data = BeautifulSoup(html_page, 'html.parser')
    text0 = data.find_all('h1')
    desc_str = text0[0].text
    split_desc = desc_str.split(": ")
    if len(split_desc)>1:
        desc[code] = split_desc[1]
    else:
        desc[code] = desc_str

ref_snomed = pd.DataFrame(desc.items())
print(ref_snomed)
data = list(ref_snomed.itertuples(index=False, name=None))


with open('./config.json', 'r') as file:
    config = json.load(file)
    db_config = config['database2']

try:
    conn = snowflake.connector.connect(
        user = db_config['user'],
        password = db_config['password'],
        account = db_config['account'],
        warehouse = db_config['warehouse'],
        database = db_config['database'],
        schema = db_config['schema'],
        role = db_config['role']
    )
    cursor = conn.cursor()
    try:
        # success, nchunks, nrows, _ = write_pandas(conn=conn, df=ref_snomed, table_name='SNOMED_REF',
        #                                           database='PC_DBT_DB', schema='DBT_BLING')
        # print(success)
        # print(nrows)
        sql = "INSERT INTO SNOMED_REF (SNOMED, DESCRIPTION) VALUES (%s, %s)"

        # Execute SQL Command for Each Row
        for row in data:
            cursor.execute(sql, row)

    finally:
        cursor.close()
finally:
    # Always ensure the connection is closed
    if 'conn' in locals():
        conn.close()