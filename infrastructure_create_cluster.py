import configparser
import pandas as pd
import boto3

from infrastructure_IAM_role import DWH_IAM_ROLE_NAME, roleArn

config = configparser.ConfigParser()
config.read_file(open('dwh.cfg'))

KEY = config.get('AWS', 'KEY')
SECRET = config.get('AWS', 'SECRET')

DWH_CLUSTER_TYPE = config.get('DWH', 'DWH_CLUSTER_TYPE')
DWH_NUM_NODES = config.get('DWH', 'DWH_NUM_NODES')
DWH_NODE_TYPE = config.get('DWH', 'DWH_NODE_TYPE')
DWH_CLUSTER_IDENTIFIER = config.get('DWH', 'DWH_CLUSTER_IDENTIFIER')
DWH_DB = config.get('DWH', 'DWH_DB')
DWH_DB_USER = config.get('DWH', 'DWH_DB_USER')
DWH_DB_PASSWORD = config.get('DWH', 'DWH_DB_PASSWORD')
DWH_PORT = config.get('DWH', 'DWH_PORT')

config_df = pd.DataFrame({'Parameters': ['DWH_CLUSTER_TYPE', 'DWH_NUM_NODES', 'DWH_NODE_TYPE',
                                         'DWH_CLUSTER_IDENTIFIER', 'DWH_DB', 'DWH_DB_USER',
                                         'DWH_DB_PASSWORD', 'DWH_PORT', 'DWH_IAM_ROLE_NAME'],
                          'Value': [DWH_CLUSTER_TYPE, DWH_NUM_NODES, DWH_NODE_TYPE,
                                    DWH_CLUSTER_IDENTIFIER, DWH_DB, DWH_DB_USER,
                                    DWH_DB, DWH_PORT, DWH_IAM_ROLE_NAME]})
print(config_df)

ec2 = boto3.resource('ec2',
                     region_name='us-east-2',
                     aws_access_key_id=KEY,
                     aws_secret_access_key=SECRET)
redshift = boto3.client('redshift', region_name='us-east-2',
                        aws_access_key_id=KEY,
                        aws_secret_access_key=SECRET)
try:
    response = redshift.create_cluster(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER,
                                       ClusterType=DWH_CLUSTER_TYPE,
                                       NodeType=DWH_NODE_TYPE,
                                       DBName=DWH_DB,
                                       NumberOfNodes=int(DWH_NUM_NODES),
                                       MasterUsername=DWH_DB_USER,
                                       MasterUserPassword=DWH_DB_PASSWORD,
                                       IamRoles=[roleArn])
except Exception as e:
    print(e)
status = redshift.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)['Clusters'][0]
