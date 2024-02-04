# This script creates the Redshift Cluster if none is running
# the code is the original from udacity with some slight additions.
import random
import string
import time
import subprocess
import configparser
import json

import boto3


def get_random_string(length: int = 8) -> str:
    """
    # Sourced original from https://pynative.com/python-generate-random-string/
    Generate a password for redshift.
    According to their api, the master password have to contain:
      * at least one character
      * at least one decimal digit
    :param length: total length of password. Must be even.
    :return:
    """
    lower_random_list = list(random.choice(string.ascii_lowercase) for _i in range(int(length / 2)))
    upper_random_list = list(random.choice(string.ascii_uppercase) for _i in range(int(length / 2)))
    numbers_list = list(map(lambda x: str(x), range(0, 10)))
    total_chars_list = lower_random_list + upper_random_list + numbers_list
    random.shuffle(total_chars_list)
    return ''.join(total_chars_list)


def get_my_ip() -> str:
    """
    Get current public ip address.
    This ip will be allowed as inbound traffic to the redshift instance.
    :return: current public ip.
    """
    return subprocess.check_output("curl -k -L -s ipconfig.me", shell=True, encoding='utf8')


config = configparser.ConfigParser()
config.read_file(open('dwh.cfg'))

# User settings
KEY = config.get('AWS', 'KEY')
SECRET = config.get('AWS', 'SECRET')
MY_IP = get_my_ip()

# aws settings
REGION_NAME = "us-west-2"

# DWH Settings, should not be changed
DWH_IAM_ROLE_NAME = "myDWHrole"
DWH_CLUSTER_TYPE = "single-node"
DWH_NODE_TYPE = "dc2.large"
DWH_NUM_NODES = 1
DWH_DB = "dwh"
DWH_PORT = 5439
DWH_DB_USER = "dwhuser"
DWH_DB_PASSWORD = get_random_string()
DWH_CLUSTER_IDENTIFIER = "dwhCluster"

# Are set automatically via script
DWH_ENDPOINT = ""
DWH_ROLE_ARN = ""

ec2 = boto3.resource('ec2',
                     region_name=REGION_NAME,
                     aws_access_key_id=KEY,
                     aws_secret_access_key=SECRET
                     )

s3 = boto3.resource('s3', region_name=REGION_NAME, aws_access_key_id=KEY, aws_secret_access_key=SECRET)
iam = boto3.client('iam', aws_access_key_id=KEY, aws_secret_access_key=SECRET, region_name='us-west-2')
redshift = boto3.client('redshift', region_name=REGION_NAME, aws_access_key_id=KEY, aws_secret_access_key=SECRET)

try:
    print(f"Checking Role existence {DWH_IAM_ROLE_NAME}")
    dwhRole = iam.get_role(RoleName=DWH_IAM_ROLE_NAME)
except:
    print(f"Creating Role {DWH_IAM_ROLE_NAME}")
    dwhRole = iam.create_role(
        Path='/',
        RoleName=DWH_IAM_ROLE_NAME,
        Description="Allows Redshift clusters to call AWS services on your behalf.",
        AssumeRolePolicyDocument=json.dumps(
            {'Statement': [{'Action': 'sts:AssumeRole',
                            'Effect': 'Allow',
                            'Principal': {'Service': 'redshift.amazonaws.com'}}],
             'Version': '2012-10-17'})
    )

print("Checking policy")
try:
    iam.get_policy(PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess")
except:
    print(f"Creating Role Policy")
    iam.attach_role_policy(RoleName=DWH_IAM_ROLE_NAME, PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess")

print("Get the IAM role ARN")
roleArn = iam.get_role(RoleName=DWH_IAM_ROLE_NAME)['Role']['Arn']

print("Checking Redshift")
# in case we use a cluster that is running, we asume that the credentials
# where already generated in the last call of this script.
new_cluster = False
if len(redshift.describe_clusters()['Clusters']) == 0:
    print("Creating RedShift Cluster")
    response = redshift.create_cluster(
        # HW
        ClusterType=DWH_CLUSTER_TYPE,
        NodeType=DWH_NODE_TYPE,
        NumberOfNodes=int(DWH_NUM_NODES),

        # Identifiers & Credentials
        DBName=DWH_DB,
        ClusterIdentifier=DWH_CLUSTER_IDENTIFIER,
        MasterUsername=DWH_DB_USER,
        MasterUserPassword=DWH_DB_PASSWORD,

        # Roles (for s3 access)
        IamRoles=[roleArn]
    )
    new_cluster = True

print("Checking status of cluster")
cluster_created = False
for i in range(10):
    if not cluster_created:
        for myClusterProps in redshift.describe_clusters()['Clusters']:
            cluster_status = myClusterProps['ClusterStatus']
            if cluster_status.lower() == 'available':
                DWH_ENDPOINT = myClusterProps['Endpoint']['Address']
                DWH_ROLE_ARN = myClusterProps['IamRoles'][0]['IamRoleArn']
                vpc = ec2.Vpc(id=myClusterProps['VpcId'])
                defaultSg = list(vpc.security_groups.all())[0]

                found_ip_permission = False
                print("Checking if IP can access Redshift")
                for ip_permission in defaultSg.ip_permissions:
                    for ip_range in ip_permission['IpRanges']:
                        if MY_IP in ip_range['CidrIp'] and ip_permission['FromPort'] == DWH_PORT and ip_permission['IpProtocol'] == 'tcp':
                            print("Found IP Permission")
                            found_ip_permission = True
                if not found_ip_permission:
                    print("Creating IP access")
                    defaultSg.authorize_ingress(
                        GroupName=defaultSg.group_name,
                        CidrIp=f'{MY_IP}/32',
                        IpProtocol='TCP',
                        FromPort=int(DWH_PORT),
                        ToPort=int(DWH_PORT)
                    )
                cluster_created = True
        if not cluster_created:
            time.sleep(60)

print("Updating dwh.cfg")
old_password = ""
if config.get("CLUSTER", "DB_PASSWORD") is not None:
    old_password = config.get("CLUSTER", "DB_PASSWORD")
config.remove_section("CLUSTER")
config.add_section("CLUSTER")
config.set("CLUSTER", "HOST", DWH_ENDPOINT)
config.set("CLUSTER", "DB_NAME", DWH_DB)
config.set("CLUSTER", "DB_USER", DWH_DB_USER)
config.set("CLUSTER", "ROLE_S3_READ", roleArn)

if new_cluster:
    config.set("CLUSTER", "DB_PASSWORD", DWH_DB_PASSWORD)
else:
    config.set("CLUSTER", "DB_PASSWORD", old_password)
config.set("CLUSTER", "DB_PORT", str(DWH_PORT))
with open('dwh.cfg', 'w') as configfile:
    config.write(configfile)