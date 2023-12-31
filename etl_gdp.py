import pandas as pd 
from bs4 import BeautifulSoup
import requests
import sqlite3
from datetime import datetime


def extract(url):
    gdp_df = pd.DataFrame()
    html_page = requests.get(url).text
    data = BeautifulSoup(html_page, 'html.parser')
    tables = data.find_all('tbody')
    rows = tables[2].find_all('tr')
    for row in rows:
        col = row.find_all('td')
        if len(col)!= 0:
            if col[0].find('a') is not None and '-' not in col[2]:
                data_dict = {"Country": col[0].a.contents[0],
            "GDP_USD_Million": col[2].contents[0]}
                # "Year": col[3].contents[0]}
                df1 = pd.DataFrame(data_dict, index=[0])
                gdp_df = pd.concat([gdp_df, df1], ignore_index=True)
    return gdp_df

def transform(gdp_df):
    gdp_df["GDP_USD_Million"] = pd.to_numeric(gdp_df["GDP_USD_Million"].str.replace(',', ''), errors = 'coerce')
    gdp_df["GDP_USD_Billion"] = gdp_df["GDP_USD_Million"].apply(lambda x: x/1000 if pd.notna(x) else x)
    gdp_df_new = gdp_df[["Country" , "GDP_USD_Billion"]]
    return gdp_df_new

def load_csv(df, csv_path): 
    df.to_csv(csv_path)

def load_to_db(df, sql_connection, table_name):
    df.to_sql(table_name, conn, if_exists= 'replace', index=False)

def run_query(query_statement, sql_connection):
    query_output = pd.read_sql(query_statement, conn)
    return query_output

def log_progress(message):
    timestamp_format = '%Y-%h-%d-%H:%M:%S' # Year-Monthname-Day-Hour-Minute-Second 
    now = datetime.now() # get current timestamp 
    timestamp = now.strftime(timestamp_format) 
    with open(log_file,"a") as f: 
        f.write(timestamp + ',' + message + '\n') 
  

log_file = "log_file.txt" 
      
url = 'https://web.archive.org/web/20230902185326/https://en.wikipedia.org/wiki/List_of_countries_by_GDP_%28nominal%29'
conn = sqlite3.connect('World_Economies.db')
csv_path = '/home/project/Countries_by_GDP.csv'
table_name = 'Countries_by_GDP'
attribute_list = ['Country', 'GDP_USD_billion']
query_statement = f"SELECT * FROM {table_name} WHERE GDP_USD_Billion>100"

log_progress("ETL Job Started") 

log_progress("Extract phase Started") 
initial_gdp_df = extract(url)
log_progress("Extract phase Ended") 

log_progress("Transform phase Started")
transformed_gdp_df = transform(initial_gdp_df)
log_progress("Transform phase Ended") 

log_progress("Load phase Started") 
load_csv(transformed_gdp_df, csv_path)
load_to_db(transformed_gdp_df, conn, table_name)
log_progress("Load phase Ended") 

print(query_statement)
output = run_query(query_statement, conn)
print(output)
