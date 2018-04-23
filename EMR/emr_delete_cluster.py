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

connection = boto3.client(
    'emr',
    region_name='us-east-1',
    # aws_access_key_id='<Your AWS Access Key>',
    # aws_secret_access_key='<Your AWS Secret Key>',
)

parser = argparse.ArgumentParser(
    formatter_class=argparse.RawDescriptionHelpFormatter,
    description='''Terminates EMR Cluster''')

parser.add_argument('--jobFlow', default="none", help='JobFlowIds for clusters to terminate')
parser.add_argument('--profile', default="default", help='profile to use, aws cli ~/.aws/credentials.')

args = parser.parse_args()

awsProfile = "--profile " + args.profile
print "AWS Profile " + awsProfile

jobFlow = "--jobFlow " + args.jobFlow
print "JobFlowIds " + jobFlow


#delete cluster
if args.jobFlow == "none":
	print "Please provide a valid jobFlowId of the cluster you want to terminate."
else:
	response = connection.terminate_job_flows(
		JobFlowIds=[args.jobFlow]
		)
	print response

waiter = connection.get_waiter('cluster_terminated')
waiter.wait(
    ClusterId = args.jobFlow,
    WaiterConfig={
        'Delay':123,
        'MaxAttempts': 123
    }
)
