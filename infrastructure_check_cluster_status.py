import configparser
import boto3
import psycopg2
from numpy import dtype

config = configparser.ConfigParser()
config.read_file(open('dwh.cfg'))

KEY = config.get('AWS', 'KEY')
SECRET = config.get('AWS', 'SECRET')
DWH_CLUSTER_IDENTIFIER = config.get('DWH', 'DWH_CLUSTER_IDENTIFIER')
DWH_DB = config.get('DWH', 'DWH_DB')
DWH_DB_PASSWORD = config.get('DWH', 'DWH_DB_PASSWORD')
DWH_DB_USER = config.get('DWH', 'DWH_DB_USER')
DWH_PORT = config.get('DWH', 'DWH_PORT')

redshift = boto3.client('redshift', region_name='us-east-2',
                        aws_access_key_id=KEY,
                        aws_secret_access_key=SECRET)
status = redshift.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)['Clusters'][0]
print(status)
DWH_ENDPOINT = status['Endpoint']
DWH_ROLE_ARN = status['IamRoles']

print("DWH_ENDPOINT::", DWH_ENDPOINT['Address'])
print("DWH_ROLE_ARN::", DWH_ROLE_ARN[0]['IamRoleArn'])

ec2 = boto3.resource('ec2',
                     region_name='us-east-2',
                     aws_access_key_id=KEY,
                     aws_secret_access_key=SECRET)

conn_string = "dbname={} user={} password={} host={} port={}".format(DWH_DB, DWH_DB_USER,
                                                                     DWH_DB_PASSWORD,
                                                                     DWH_ENDPOINT['Address'],
                                                                     DWH_PORT)
print(conn_string)
conn = psycopg2.connect(conn_string)
if conn.status == psycopg2.extensions.STATUS_READY:
    print('Connected')
else:
    print('Connection Failed')
conn.close()
