import configparser
import json
import boto3

# Step 1: Read configuration file to get parameters
config = configparser.ConfigParser()
config.read_file(open('dwh.cfg'))

KEY = config.get('AWS', 'KEY')
SECRET = config.get('AWS', 'SECRET')
DWH_IAM_ROLE_NAME = config.get('DWH', 'DWH_IAM_ROLE_NAME')

# Create resource APIs to interact with AWS resources

iam = boto3.client('iam', region_name='us-east-2',
                   aws_access_key_id=KEY,
                   aws_secret_access_key=SECRET)

# Create IAM role.
try:
    print('Creating a new IAM role')
    dwhRole = iam.create_role(Path='/', RoleName=DWH_IAM_ROLE_NAME,
                              Description="Allows Redshift clusters to call AWS services on your behalf.",
                              AssumeRolePolicyDocument=json.dumps(
                                  {'Statement':[{'Action':'sts:AssumeRole',
                                                 'Effect':'Allow',
                                                 'Principal': {'Service':'redshift.amazonaws.com'}}],
                                   'Version': '2012-10-17'}))
except Exception as e:
    print(e)

print('Attaching Policy')
iam.attach_role_policy(RoleName=DWH_IAM_ROLE_NAME,
                       PolicyArn ='arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess')['ResponseMetadata']['HTTPStatusCode']
# iam.attach_role_policy(RoleName=DWH_IAM_ROLE_NAME,
#                        PolicyArn ='arn:aws:iam::aws:policy/AmazonEC2FullAccess')['ResponseMetadata']['HTTPStatusCode']
print('Get the IAM role ARN')
roleArn = iam.get_role(RoleName=DWH_IAM_ROLE_NAME)['Role']['Arn']
print(roleArn)