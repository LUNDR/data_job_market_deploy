# -*- coding: utf-8 -*-
"""
Created on Sat May  9 22:18:45 2020

@author: lundr
"""


import boto3
import pandas as pd
from io import StringIO
import numpy as np
from datetime import time
from datetime import datetime
import matplotlib.pyplot as plt

from os import listdir
from os.path import isfile, join

from config import ACCESS_KEY,SECRET_KEY

def ListFiles(client,_BUCKET_NAME,_PREFIX):
    
    """List files in specific S3 URL"""
    
    response = client.list_objects(Bucket=_BUCKET_NAME, Prefix=_PREFIX)
    for content in response.get('Contents', []):
        yield content.get('Key')
        
   
        

def get_all_s3_keys(bucket):
    """Get a list of all keys in an S3 bucket."""
    keys = []

    kwargs = {'Bucket': bucket}
    while True:
        resp = s3.list_objects_v2(**kwargs)
        for obj in resp['Contents']:
            keys.append(obj['Key'])

        try:
            kwargs['ContinuationToken'] = resp['NextContinuationToken']
        except KeyError:
            break

    return keys

_BUCKET_NAME = 'jobadscrape'
_PREFIX = ''       
  
s3 = boto3.client(
"s3",
aws_access_key_id=ACCESS_KEY,
aws_secret_access_key=SECRET_KEY
)


file_list = get_all_s3_keys(_BUCKET_NAME )


files={}
for f in file_list:
    client = boto3.client(
    "s3",
    aws_access_key_id=ACCESS_KEY,
    aws_secret_access_key=SECRET_KEY
    )
    response = client.head_object(
    Bucket=_BUCKET_NAME,
    Key=f
    )
    files[f] = response['ContentLength']


files_keep=[]
for k in files:
    if files[k] >155000:
        files_keep.append(k.split('_')[-2])
#files = [f for f in files if '.txt' in f]


with open('C:/Users/lundr/jobs_data/new_list.txt','wb') as f:
        pickle.dump(files_keep)



s3 = boto3.client(
"s3",
aws_access_key_id=ACCESS_KEY,
aws_secret_access_key=SECRET_KEY
)


