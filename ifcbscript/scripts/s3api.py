import boto3
import botocore
import os
import configparser

def load_s3_config(path):
    credentials = {}
    config = configparser.ConfigParser()
    config.read(path)
    credentials['host_base'] = config['default']['host_base']
    credentials['secret_key'] = config['default']['secret_key']
    credentials['access_key'] = config['default']['access_key']
    return credentials

class s3API:
    def __init__(self, config_file_path):
        config = load_s3_config(config_file_path)
        self.client = boto3.client('s3', use_ssl=False,
                               endpoint_url="http://" + config['host_base'],
                               aws_access_key_id=config['access_key'],
                               aws_secret_access_key=config['secret_key'],                    
                               region_name="US",)
    
    def list_s3_objects(self, host_bucket, prefix = ''):
        # Returns a list of s3 keys
        keys = []
        try:
            paginator = self.client.get_paginator('list_objects')
            operation_parameters = {'Bucket': host_bucket,
                                    'Prefix': prefix}
            page_iterator = paginator.paginate(**operation_parameters)
            for page in page_iterator:
                if 'Contents' in page:
                    for item in page['Contents']:
                        keys.append(item['Key'])
        except Exception as e:
            print("List Error: ", str(e))
            raise e
        return keys
    
    def get_s3_object(self, host_bucket, s3_key, destination_file):
        # destination_file => os path to file + file_name
        try:
            self.client.download_file(host_bucket, s3_key, destination_file)
            print("Downloaded File as: ", destination_file)
        except Exception as e:
            print("Download Error: ", str(e))
            raise e
    
    def download_folder(self, host_bucket, s3_prefix, local_dest):
        keys = self.list_s3_objects(self.client, host_bucket, s3_prefix)
        
        for key in keys:
            dest = key if local_dest is None \
                else os.path.join(local_dest, os.path.relpath(key, s3_prefix))
            if not os.path.exists(os.path.dirname(dest)):
                os.makedirs(os.path.dirname(dest))
            if key[-1] == '/':
                continue
            
            self.client.download_file(host_bucket, key, dest)
        

# USAGE OF THE S3 API
# s3_obj = s3API('/Users/shravani.nagala/dev/IFCB/shravaninagala.s3cfg')
# print(s3_obj.list_s3_objects("appdev1", '789'))