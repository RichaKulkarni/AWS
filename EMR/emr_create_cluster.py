#!/usr/bin/python
import datetime
import requests
import sys
import json
import os
import logging, logging.handlers
from subprocess import call
import pdb
import argparse
import boto3
from botocore.exceptions import ClientError
import json

connection = boto3.client(
    'emr',
    region_name='us-east-1',
    # aws_access_key_id='<Your AWS Access Key>',
    # aws_secret_access_key='<Your AWS Secret Key>',
)

parser = argparse.ArgumentParser(
    formatter_class=argparse.RawDescriptionHelpFormatter,
    description='''Creates EMR Cluster''')

parser.add_argument('--region', default="us-east-1", help='AWS regions to user.')
parser.add_argument('--profile', default="default", help='profile to use, aws cli ~/.aws/credentials.')
parser.add_argument('--cluster_name', default="emr_cluster", help='AWS regions to user.')
#parser.add_argument('--profile', default="aws-dev", help='profile to use, aws cli ~/.aws/credentials.')
#master node parameters
parser.add_argument('--master_market', default="SPOT", choices=['SPOT', 'ON_DEMAND'], help='Which stack, a or b')
parser.add_argument('--master_type', default="m4.large", help='Instance type. Default is m4.large.')
parser.add_argument('--master_bid', default="0.032", help='Bid price to provide for spot instances.')
#core node parameters
parser.add_argument('--core_market', default="SPOT", choices=['SPOT', 'ON_DEMAND'], help='Which stack, a or b')
parser.add_argument('--core_type', default="m4.large", help='Instance type. Default is m4.large.')
parser.add_argument('--core_bid', default="0.032", help='Bid price to provide for spot instances.')
parser.add_argument('--core_count', default=1, help='Number of core instances you need.')
#task node parameters
parser.add_argument('--task_market', default="SPOT", choices=['SPOT', 'ON_DEMAND'], help='Which stack, a or b')
parser.add_argument('--task_type', default="m4.large", help='Instance type. Default is m4.large.')
parser.add_argument('--task_bid', default="0.032", help='Bid price to provide for spot instances.')
parser.add_argument('--task_count', default=1, help='Number of task instances you need.')


args = parser.parse_args()

awsProfile = "--profile " + args.profile
print "AWS Profile " + awsProfile

#convert core_count to string for future use in Step
str_core_count = str(args.core_count)

#grab json config file from s3 bucket/folder
#read it (as string) then load as json object
#pass to configuration when creating emr cluster
s3 = boto3.resource('s3')
bucket = s3.Bucket('S3_BUCKET_EMR')
for obj in bucket.objects.filter(Prefix='scripts/modified_config.json'):
    s3_json_config = json.loads(obj.get()['Body'].read().decode())
    print "DATA = ", s3_json_config
    print " "

#create emr cluster
cluster_id = connection.run_job_flow(
    Name=args.cluster_name,
    LogUri='s3://S3_BUCKET_EMR/',
    ReleaseLabel='emr-5.12.0',
    Instances={
        'InstanceGroups': [
            {
                'Name': "Master nodes",
                #'Market': 'ON_DEMAND',
                'Market': args.master_market,
                'InstanceRole': 'MASTER',
                'InstanceType': args.master_type,
                'InstanceCount': 1,
                'BidPrice': args.master_bid,
            },
            {
                'Name': "Core nodes",
                'Market': args.core_market,
                'InstanceRole': 'CORE',
                'InstanceType': args.core_type,
                'InstanceCount': args.core_count,
                'BidPrice': args.core_bid,
            },
            {
                'Name': "Task nodes",
                'Market': args.task_market,
                'InstanceRole': 'TASK',
                'InstanceType': args.task_type,
                'InstanceCount': args.task_count,
                'BidPrice': args.task_bid,
            }
        ],
        'Ec2KeyName': 'EC2_KEY_FOR_SSH_CONNECTIONS',
        'KeepJobFlowAliveWhenNoSteps': True,
        #'TerminationProtected': False,
        'Ec2SubnetId': 'EC2_SUBNET_IDS',
    },
    ServiceRole='EMR_Role',
    JobFlowRole='EMR_EC2_Role',
    Tags=[
        {
            'Key': 'emr_cluster_name',
            'Value': args.cluster_name,
        },
    ],
    BootstrapActions=[
        {
            'Name': 'AWS provided bootstrap test',
            'ScriptBootstrapAction': {
                'Path':'s3://S3_BUCKET_EMR/scripts/download.sh',
            }
        },
        {
            'Name': 'JQ Installation',
            'ScriptBootstrapAction': {
                'Path':'s3://S3_BUCKET_EMR/scripts/config.sh',
            }
        },
        {
            'Name': 'Copy S3 scripts to local filesystem to run during Steps',
            'ScriptBootstrapAction': {
                'Path':'s3://S3_BUCKET_EMR/scripts/scriptCopy.sh'
            }
        }
    ],
    Steps=[
        {
            'Name': 'Download pem key for SSH',
            'ActionOnFailure': 'CONTINUE',
            'HadoopJarStep': {
                'Jar': 'command-runner.jar',
                'Args': [
                    '/bin/bash',
                    '/scripts/downloadPemSSH.bash'
                ]
            }
        },
        {
            'Name': 'TEZ Jar Load',
            'ActionOnFailure': 'CONTINUE',
            'HadoopJarStep': {
                'Jar': 'command-runner.jar',
                'Args': [
                    's3-dist-cp',
                    '--src=s3://S3_BUCKET_EMR/tez/tez-0.9.0.tar.gz',
                    '--dest=hdfs:///apps/tez/x/'
                ]
            }
        },
        {
            'Name': 'TEZ UI Modify',
            'ActionOnFailure': 'CONTINUE',
            'HadoopJarStep': {
                'Jar': 'command-runner.jar',
                'Args': [
                    '/bin/bash',
                    '/scripts/tezUI.sh'
                ]
            }
        },
        {
            'Name': 'Create Node Labels',
            'ActionOnFailure': 'CONTINUE',
            'HadoopJarStep': {
                'Jar': 'command-runner.jar',
                'Args': [
                    '/bin/bash',
                    '/scripts/nodeLabels.sh'
                ]
            }
        },
        {
            'Name': 'Attach Node Labels',
            'ActionOnFailure': 'CONTINUE',
            'HadoopJarStep': {
                'Jar': 'command-runner.jar',
                'Args': [
                    '/bin/bash',
                    '/scripts/attachNodeLabels.bash',
                    args.cluster_name,
                    str_core_count
                ]
            }
        }
    ],
    Applications=[{'Name':'Hadoop'}, {'Name':'Tez'}],
    Configurations=s3_json_config,
    VisibleToAllUsers=True,
)
print (cluster_id['JobFlowId'])#print emr cluster job flow id
job_flow_id = cluster_id['JobFlowId']
print "job_flow_id = ",job_flow_id
print "Starting Cluster ", cluster_id

waiter = connection.get_waiter('cluster_running')
waiter.wait(
    ClusterId = job_flow_id,
    WaiterConfig={
        'Delay':123,
        'MaxAttempts': 123
    }
)

response = connection.list_instances(
    ClusterId = job_flow_id,
    InstanceGroupTypes=['MASTER'],
)
    
