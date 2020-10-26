#!/usr/bin/env python3

import paramiko 
import pysftp
import os
import glob
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
    if os.path.isdir(localdir) is False:
      os.mkdir(localdir)

    dir_location = '{}/{}'.format(os.curdir, localdir)

    return conn.get_d(remotedir, localdir)

get_files_from_sft(sftp_connection, 'POC_data_out/test/', 'data/')

s3_client = boto3.client(
    service_name='s3',
    region_name=s3_region_name,
    aws_access_key_id=s3_aws_access_key_id,
    aws_secret_access_key=s3_aws_secret_access_key
)

def put_files_s3(conn, localdir):
  dir_location = '{}/{}'.format(os.curdir, localdir)
  files = glob.glob(dir_location + "*.zip")
  remotedir = date.today()

  for file in files:
    conn.upload_file(file, 'dcyf-data-extracts', '{}/{}'.format(remotedir, file.replace(localdir, '')))

put_files_s3(s3_client, 'data/')