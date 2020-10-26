#!/usr/bin/env python3

import paramiko 
import pysftp
import os
import boto3
import pyodbc
from paramiko import RSAKey
from base64 import decodebytes
from datetime import date

sft_public_key = os.environ.get('SFT_PUBLIC_KEY')
sft_host = os.environ.get('SFT_HOST')
sft_username = os.environ.get('SFT_USERNAME')
sft_password = os.environ.get('SFT_PASSWORD')

s3_region_name = os.environ.get('S3_REGION_NAME')
s3_aws_access_key_id = os.environ.get('S3_AWS_ACCESS_KEY_ID')
s3_aws_secret_access_key = os.environ.get('S3_AWS_SECRET_ACCESS_KEY')

keydata = bytes(sft_public_key, 'utf-8')
key = paramiko.RSAKey(data=decodebytes(keydata))
cnopts = pysftp.CnOpts()
cnopts.hostkeys.add(sft_host, 'ssh-rsa', key)

sftp_connection = pysftp.Connection(
  host=sft_host, 
  username=sft_username, 
  password=sft_password,
  cnopts=cnopts
)

def get_files_from_sft(conn, remotedir, localdir):
    return conn.get_d(remotedir, localdir)

dir_location = '{}/{}'.format(os.curdir, 'data/')

get_files_from_sft(sftp_connection, 'POC_data_out', dir_location)

s3_client = boto3.client(
    service_name='s3',
    region_name=s3_region_name,
    aws_access_key_id=s3_aws_access_key_id,
    aws_secret_access_key=s3_aws_secret_access_key
)

current_date = date.today()
file_name = 'ABUSE_TYPE_DIM.TXT'

files = os.listdir(dir_location)

for file in files:
  print('{}/{}'.format(current_date, file))
  s3_client.upload_file('./data/{}'.format(file), 'dcyf-data-extracts', '{}/{}'.format(current_date, file))