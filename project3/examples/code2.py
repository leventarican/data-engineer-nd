import configparser
import pandas as pd
import boto3
import json
from botocore.exceptions import ClientError

# create a redshift cluster / delete a redshift cluster
# ##############################################################################

print("# load configuration")
print("#######################################################################")
config = configparser.ConfigParser()
config.read_file(open('ignore-config.cfg'))

KEY                    = config.get('AWS','KEY')
SECRET                 = config.get('AWS','SECRET')

DWH_CLUSTER_TYPE       = config.get("DWH","DWH_CLUSTER_TYPE")
DWH_NUM_NODES          = config.get("DWH","DWH_NUM_NODES")
DWH_NODE_TYPE          = config.get("DWH","DWH_NODE_TYPE")

DWH_CLUSTER_IDENTIFIER = config.get("DWH","DWH_CLUSTER_IDENTIFIER")
DWH_DB                 = config.get("DWH","DWH_DB")
DWH_DB_USER            = config.get("DWH","DWH_DB_USER")
DWH_DB_PASSWORD        = config.get("DWH","DWH_DB_PASSWORD")
DWH_PORT               = config.get("DWH","DWH_PORT")

DWH_IAM_ROLE_NAME      = config.get("DWH", "DWH_IAM_ROLE_NAME")

(DWH_DB_USER, DWH_DB_PASSWORD, DWH_DB)

df = pd.DataFrame(
    {"Param":["DWH_CLUSTER_TYPE", "DWH_NUM_NODES", "DWH_NODE_TYPE", "DWH_CLUSTER_IDENTIFIER", "DWH_DB", "DWH_DB_USER", "DWH_DB_PASSWORD", "DWH_PORT", "DWH_IAM_ROLE_NAME"],
    "Value":[DWH_CLUSTER_TYPE, DWH_NUM_NODES, DWH_NODE_TYPE, DWH_CLUSTER_IDENTIFIER, DWH_DB, DWH_DB_USER, DWH_DB_PASSWORD, DWH_PORT, DWH_IAM_ROLE_NAME]
    })

print(df)

print("# create client: ec2, s3, iam, redshift")
print("#######################################################################")
ec2 = boto3.resource('ec2',
                    region_name="us-west-2",
                    aws_access_key_id=KEY,
                    aws_secret_access_key=SECRET
                    )

s3 = boto3.resource('s3',
                    region_name="us-west-2",
                    aws_access_key_id=KEY,
                    aws_secret_access_key=SECRET
                )

iam = boto3.client('iam',
                region_name='us-west-2', 
                aws_access_key_id=KEY,
                aws_secret_access_key=SECRET,
                )

redshift = boto3.client('redshift',
                    region_name="us-west-2",
                    aws_access_key_id=KEY,
                    aws_secret_access_key=SECRET
                    )

DWH_ENDPOINT = None
DWH_ROLE_ARN = None

def createIAMRole():
    print("# create IAM role")
    print("#######################################################################")
    try:
        dwhRole = iam.create_role(
            Path='/',
            RoleName=DWH_IAM_ROLE_NAME,
            Description = "Allows Redshift clusters to call AWS services on your behalf.",
            AssumeRolePolicyDocument=json.dumps(
                {'Statement': [{'Action': 'sts:AssumeRole',
                'Effect': 'Allow',
                'Principal': {'Service': 'redshift.amazonaws.com'}}],
                'Version': '2012-10-17'})
        )    
    except Exception as e:
        print(e)

    iam.attach_role_policy(RoleName=DWH_IAM_ROLE_NAME, 
        PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess"
        )['ResponseMetadata']['HTTPStatusCode']

    roleArn = iam.get_role(RoleName=DWH_IAM_ROLE_NAME)['Role']['Arn']
    print(roleArn[0:11])

    return roleArn

def createRedshiftCluster(roleArn):
    print("# create redshift cluster")
    print("#######################################################################")
    try:
        response = redshift.create_cluster(        
            # Hardware
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
    except Exception as e:
        print(e)

def prettyRedshiftProps():
    print("# cluster status")
    print("#######################################################################")
    props = redshift.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)['Clusters'][0]
    pd.set_option('display.max_colwidth', None)
    keysToShow = ["ClusterIdentifier", "NodeType", "ClusterStatus", "MasterUsername", "DBName", "Endpoint", "NumberOfNodes", 'VpcId']
    x = [(k, v) for k,v in props.items() if k in keysToShow]
    df = pd.DataFrame(data=x, columns=["Key", "Value"])
    print(df)

def setEndpointARN():
    print("# cluster endpoint and role ARN")
    print("#######################################################################")
    props = redshift.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)['Clusters'][0]
    DWH_ENDPOINT = props['Endpoint']['Address']
    DWH_ROLE_ARN = props['IamRoles'][0]['IamRoleArn']
    print("DWH_ENDPOINT :: ", DWH_ENDPOINT)
    print("DWH_ROLE_ARN :: ", DWH_ROLE_ARN)

def openPort():
    print("# open an incoming TCP port to access the cluster endpoint")
    print('https://us-west-2.console.aws.amazon.com/ec2/home?region=us-west-2#SecurityGroups:')
    print("#######################################################################")
    props = redshift.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)['Clusters'][0]
    try:
        vpc = ec2.Vpc(id=props['VpcId'])
        defaultSg = list(vpc.security_groups.all())[0]
        print(defaultSg)
        defaultSg.authorize_ingress(
            GroupName=defaultSg.group_name,
            CidrIp='0.0.0.0/0',
            IpProtocol='TCP',
            FromPort=int(DWH_PORT),
            ToPort=int(DWH_PORT)
        )
    except Exception as e:
        print(e)

def deleteCluster():
    print("# delete cluster")
    print("#######################################################################")
    redshift.delete_cluster( ClusterIdentifier=DWH_CLUSTER_IDENTIFIER,  SkipFinalClusterSnapshot=True)

def deleteRole():
    print("# delete role")
    print("#######################################################################")
    iam.detach_role_policy(RoleName=DWH_IAM_ROLE_NAME, PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess")
    iam.delete_role(RoleName=DWH_IAM_ROLE_NAME)

if __name__ == "__main__":
    # create cluster
    # ##############
    # roleArn = createIAMRole()
    # createRedshiftCluster(roleArn)

    # run until cluster status becomes Available
    # prettyRedshiftProps()

    # if cluster status Available
    # setEndpointARN()

    # openPort()

    # cleanup / delete cluster
    # ########################
    # deleteCluster()
    # run until cluster status becomes Deleted
    # prettyRedshiftProps()
    # deleteRole()
