import os
import re
import logging
import boto3
from tqdm import tqdm
import requests
import shutil
from datetime import datetime
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import dask.dataframe as dd
import xarray as xr
import pandas as pd
import dask_geopandas as dgpd
import s3fs
import urllib
from urllib.parse import urlparse, parse_qs
from pytz import utc
import json
import boto3 
import requests
import dotenv
import time

logger = logging.getLogger(__name__)

class S3Utils:
    def __init__(self):
        pass
    
    
    @staticmethod
    def is_subsettable_arco_asset(url):
        """
        Check if the given URL is a subsettable ARCO asset (either Zarr or Parquet).
        :param url: The URL to check.
        :type url: str
        :return: True if the URL is a subsettable ARCO asset, False otherwise.
        :rtype: bool
        """
        if S3Utils.is_zarr_asset(url):
            return S3Utils.test_subset_zarr_asset(url)
        elif S3Utils.is_parquet_asset(url):
            return S3Utils.test_subset_parquet_asset(url)
        return False
    
    @staticmethod
    def is_zarr_asset(url):
        """
        :param url: The URL to check.
        :type url: str
        :return: True if the URL is a Zarr asset, False otherwise.
        :rtype: bool
        """
        zarr_pattern = re.compile(r'\.zarr/?$')
        return bool(zarr_pattern.search(url))

    @staticmethod
    def is_parquet_asset(url):
        """
        :param url: The URL to check.
        :type url: str
        :return: True if the URL is a Parquet asset, False otherwise.
        :rtype: bool
        """
        parquet_pattern = re.compile(r'\.(parquet|geoparquet)/?$')
        return bool(parquet_pattern.search(url))

    def is_geoparquet_asset(url):
        """
        :param url: The URL to check.
        :type url: str
        :return: True if the URL is a GeoParquet asset, False otherwise.
        :rtype: bool
        """
        geoparquet_pattern = re.compile(r'\.parquet/?$')
        if not geoparquet_pattern.search(url):
            return False
        try:
            dgdf = S3Utils.read_geoparquet_s3(url)
            if dgdf is not None:
                return True
        except Exception as e:
            logger.info(f"parquet is not geoparquet: {e}")
            return False
        return False

    @staticmethod
    def test_subset_zarr_asset(url):
        """
        :param url: The URL to the Zarr asset.
        :type url: str
        :return: True if a small subset is successfully retrieved, False otherwise.
        :rtype: bool
        """
        try:
            ds = xr.open_zarr(url)
            data_var = list(ds.data_vars)[0]
            subset = ds[data_var].isel({dim: 0 for dim in ds.dims})
            return subset is not None
        except Exception as e:
            print(f"Error subsetting Zarr asset: {e}")
            return False

    @staticmethod
    def test_subset_parquet_asset(url):
        """
        :param url: The URL to the Parquet asset.
        :type url: str
        :return: True if a small subset is successfully retrieved, False otherwise.
        :rtype: bool
        """
        try:
            df = S3Utils.read_parquet_from_s3(url)
            subset = df.head(10)
            return subset is not None
        except Exception as e:
            print(f"Error subsetting Parquet asset: {e}")
            return False

    @staticmethod
    def check_for_existing_objects(s3_client, bucket, prefix):
        """
        Check if there are any objects under the given prefix in the S3 bucket.
        :param s3_client: The boto3 S3 client instance.
        :param bucket: Name of the S3 bucket.
        :param prefix: The S3 prefix to check for objects.
        :return: True if objects are found, False otherwise.
        """
        response = s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix)
        if response.get('Contents'):
            return True
        return False

    @staticmethod
    def check_for_object(s3_client, bucket, key):
        """
        Check if an object exists in the S3 bucket.
        :param s3_client: The boto3 S3 client instance.
        :param bucket: Name of the S3 bucket.
        :param key: The S3 key of the object.
        :return: True if the object exists, False otherwise.
        """
        try:
            s3_client.head_object(Bucket=bucket, Key=key)
            return True
        except Exception as e:
            return False


    @staticmethod
    def upload_to_s3(s3_client, bucket: str, host: str, path: str, s3_loc: str):
        """
        :param s3_client: The Boto3 S3 client.
        :type s3_client: boto3.client
        :param bucket_name: The name of the S3 bucket.
        :type bucket_name: str
        :param host: The S3 host URL.
        :type host: str
        :param path: The local file or directory path to upload.
        :type path: str
        :param s3_loc: The destination location in the S3 bucket.
        :type s3_loc: str
        :return: The S3 URL of the uploaded file or None if the upload fails.
        :rtype: str
        """
        # check if the path exists at the s3_loc
        base_name = os.path.basename(path)
        asset_s3_url = f"{host}/{bucket}/{s3_loc}/{base_name}"
        
        # check if path is dir, walk through and upload all files using relative path, keep dir structure
        if os.path.isdir(path):
            total_size = os.path.getsize(path)
            for root, dirs, files in os.walk(path):
                for file in files:
                    local_path = os.path.join(root, file)
                    relative_path = os.path.relpath(local_path, path)
                    s3_key = f"{s3_loc}/{os.path.basename(path)}/{relative_path}"
                    try:
                        with tqdm(total=os.path.getsize(local_path), unit='B', unit_scale=True, desc=base_name, ncols=80) as pbar:
                            s3_client.upload_file(local_path, bucket, s3_key, Callback=lambda bytes_sent: pbar.update(bytes_sent))
                    except Exception as e:
                        logger.error(f"Failed to upload {base_name} to S3: {str(e)}")
                        return None
        # upload single file
        else:
            s3_key = f"{s3_loc}/{base_name}"
            total_size = os.path.getsize(path)
            with tqdm(total=total_size, unit='B', unit_scale=True, desc=path, ncols=80) as pbar:
                try:
                    s3_client.upload_file(path, bucket, s3_key, Callback=lambda bytes_sent: pbar.update(bytes_sent))
                    logger.info(f"Successfully uploaded {path} to S3 as {s3_key}")
                except Exception as e:
                    logger.error(f"Failed to upload asset_url to S3: {str(e)}")
                    return None

        logger.info(f"Successfully transferred to S3: {s3_key}, link: {asset_s3_url}")
        return asset_s3_url


    def delete_from_s3_recursive(s3_client, bucket, prefix):
        """
        Delete all objects under a specific prefix, including the prefix directory itself if it exists.
        :param s3_client: The Boto3 S3 client.
        :type s3_client: boto3.client
        :param bucket: The name of the S3 bucket.
        :type bucket: str
        :param prefix: The prefix of the objects to delete.
        :type prefix: str
        """
        try:
            # Check if the prefix is a single file
            response = s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix)
            if 'Contents' in response:
                if len(response['Contents']) == 1 and response['Contents'][0]['Key'] == prefix:
                    # It's a single file, delete it directly
                    s3_client.delete_object(Bucket=bucket, Key=prefix)
                    logger.info(f"Deleted file: {prefix}")
                else:
                    # It's a directory, delete all contents recursively
                    for obj in response['Contents']:
                        s3_client.delete_object(Bucket=bucket, Key=obj['Key'])
                    logger.info(f"Deleted all objects under directory: {prefix}")

            else:
                logger.info(f"No objects found under {prefix}")
        except Exception as e:
            logger.error(f"Error checking or deleting objects under {prefix}: {str(e)}")


    @staticmethod
    def list_s3_files(s3_client, bucket, host, prefix):
        """
        List all files in the S3 bucket under the given prefix.
        :param s3_client: The boto3 S3 client instance.
        :param bucket: Name of the S3 bucket.
        :param prefix: The S3 prefix to list files under.
        :return: A dictionary where keys are S3 keys and values are the last modified timestamps.
        """
        s3_files = {}
        paginator = s3_client.get_paginator('list_objects_v2')
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
            if 'Contents' in page:
                for obj in page['Contents']:
                    s3_files[obj['Key']] = obj['LastModified']
        return s3_files


    @staticmethod
    def sync_to_s3(s3_client, bucket, host, path, s3_loc):
        """
        Sync a local directory or file with a remote S3 location using timestamps to determine if the file
        needs to be uploaded. Also deletes objects in S3 that are not present locally.
        
        :param s3_client: The boto3 S3 client instance.
        :param bucket: Name of the S3 bucket.
        :param host: Host URL of the S3 bucket.
        :param path: The local file or directory to sync.
        :param s3_loc: The S3 location (prefix) to sync with.
        :return: The S3 URL after successful sync, or None on failure.
        """
        # List all S3 objects in the target prefix
        s3_files = S3Utils.list_s3_files(s3_client, bucket, host, s3_loc)

        local_files = set()
        if os.path.isdir(path):
            for root, dirs, files in os.walk(path):
                for file in files:
                    local_path = os.path.join(root, file)
                    relative_path = os.path.relpath(local_path, path)
                    s3_key = f"{s3_loc}/{relative_path}"
                    local_files.add(s3_key)
                    if s3_key not in s3_files or os.path.getmtime(local_path) > s3_files[s3_key].timestamp():
                        try:
                            with tqdm(total=os.path.getsize(local_path), unit='B', unit_scale=True, desc=file, ncols=80) as pbar:
                                s3_client.upload_file(local_path, bucket, s3_key, Callback=lambda bytes_sent: pbar.update(bytes_sent))
                                logger.info(f"Successfully uploaded {local_path} to S3 as {s3_key}")
                        except Exception as e:
                            logger.error(f"Failed to upload {file} to S3: {str(e)}")
                            return None
        else:
            file_name = os.path.basename(path)
            s3_key = f"{s3_loc}/{file_name}"
            local_files.add(s3_key)
            if s3_key not in s3_files or os.path.getmtime(path) > s3_files[s3_key].timestamp():
                total_size = os.path.getsize(path)
                with tqdm(total=total_size, unit='B', unit_scale=True, desc=path, ncols=80) as pbar:
                    try:
                        s3_client.upload_file(path, bucket, s3_key, Callback=lambda bytes_sent: pbar.update(bytes_sent))
                        logger.info(f"Successfully uploaded {path} to S3 as {s3_key}")
                    except Exception as e:
                        logger.error(f"Failed to upload {path} to S3: {str(e)}")
                        return None

        # Identify objects to delete in S3 (present in S3 but not locally)
        s3_keys = set(s3_files.keys())
        to_delete = s3_keys - local_files

        if to_delete:
            logger.info(f"Deleting {len(to_delete)} unmatched objects from S3...")
            delete_objects = [{"Key": key} for key in to_delete]
            for i in range(0, len(delete_objects), 1000): 
                s3_client.delete_objects(
                    Bucket=bucket,
                    Delete={"Objects": delete_objects[i:i + 1000]}
                )
        else:
            logger.info("No unmatched objects found to delete in S3.")

        asset_s3_url = f"{host}/{bucket}/{s3_loc}"
        logger.info(f"Successfully transferred to S3: {s3_loc}, link: {asset_s3_url}")
        return asset_s3_url


    @staticmethod
    def move_s3_objects(s3_client, bucket, host, source_loc, new_loc):
        """
        :param source_loc: The source S3 location (prefix).
        :type source_loc: str
        :param new_loc: The destination S3 location (prefix).
        :type new_loc: str
        :return: True if the move was successful, False otherwise.
        :rtype: bool
        """
        
        try:
            # Ensure the prefix matches only the desired directory
            source_loc = source_loc.rstrip('/') + '/'
            new_loc = new_loc.rstrip('/') + '/'
        
            source_objects = s3_client.list_objects_v2(Bucket=bucket, Prefix=source_loc).get('Contents', [])
            for obj in source_objects:
                source_key = obj['Key']
                new_key = source_key.replace(source_loc, new_loc, 1)
                # Copy each object to the backup location
                copy_source = {'Bucket': bucket, 'Key': source_key}
                s3_client.copy_object(Bucket=bucket, CopySource=copy_source, Key=new_key)
                logging.info(f"Successfully backed up {source_key} to {new_key}")
                s3_client.delete_object(Bucket=bucket, Key=source_key)
                logger.info(f"Successfully deleted {source_key}")
            return True
        except Exception as e:
            logging.error(f"Failed to move objects from {source_loc} to {new_loc}: {str(e)}")
            return False

    @staticmethod
    def copy_s3_objects(s3_client, bucket, host, source_loc, new_loc):
        """
        :param source_loc: The source S3 location (prefix).
        :type source_loc: str
        :param new_loc: The destination S3 location (prefix).
        :type new_loc: str
        :return: True if the copy was successful, False otherwise.
        :rtype: bool
        """
        
        try:
        
            source_objects = s3_client.list_objects_v2(Bucket=bucket, Prefix=source_loc).get('Contents', [])
            for obj in source_objects:
            
                source_key = obj['Key']
                new_key = source_key.replace(source_loc, new_loc, 1)
                # Copy each object to the backup location
                copy_source = {'Bucket': bucket, 'Key': source_key}
                s3_client.copy_object(Bucket=bucket, CopySource=copy_source, Key=new_key)
                logging.info(f"Successfully copied {source_key} to {new_key}")
            return True
        except Exception as e:
            logging.error(f"Failed to copy objects from {source_loc} to {new_loc}: {str(e)}")
            return False

    @staticmethod
    def read_parquet_from_s3(url):
        """
        Read a Parquet file from s3 url in the format 'https://s3.region.host/bucket/key.parquet'.

        :param download_url: The URL of the S3 asset to read.
        :type download_url: str
        :return: A Dask dataframe containing the Parquet data.
        :rtype: dask.dataframe.DataFrame
        """
        #check url in the format 'https://s3.region.host/bucket/key.parquet'
        if not url.startswith('https://s3.'):
            logger.error(f'cant read parquet from non generic s3 url {url}')
        parsed_url = urllib.parse.urlparse(url)
        net_loc = parsed_url.netloc
        object_key = parsed_url.path.lstrip('/')
        s3_uri = f's3://{object_key}'
        endpoint_url = f'https://{net_loc}'
        fs = s3fs.S3FileSystem(anon=True, use_ssl=True, client_kwargs={'endpoint_url': endpoint_url})
        ddf = dd.read_parquet(
            s3_uri,
            storage_options={'anon': True, 'client_kwargs' : {'endpoint_url': endpoint_url}},
            engine='pyarrow'
        )
        return ddf
    
    
    @staticmethod
    def read_geoparquet_s3(download_url):
        """
        Read a GeoParquet file from an s3 url in the format 'https://s3.region.host/bucket/key.parquet'.

        :param download_url: The URL of the S3 asset to read.
        :type download_url: str
        :return: A Dask GeoDataFrame containing the GeoParquet data.
        :rtype: dask_geopandas.GeoDataFrame
        """
        if not download_url.startswith('https://s3.'):
            logger.error(f'cant read geoparquet from non generic s3 url {download_url}')
        parsed_url = urllib.parse.urlparse(download_url)
        net_loc = parsed_url.netloc
        object_key = parsed_url.path.lstrip('/')
        s3_uri = f's3://{object_key}'
        endpoint_url = f'https://{net_loc}'
        fs = s3fs.S3FileSystem(anon=True, use_ssl=True, client_kwargs={'endpoint_url': endpoint_url})
        ddf = dgpd.read_parquet(
            s3_uri,
            storage_options={'anon': True, 'client_kwargs' : {'endpoint_url': endpoint_url}}
        )
        return ddf
    
    
    @staticmethod
    def list_arco_dataset_variables(layer_metadata):
        """
        List variables from converted ARCO datasets that are found in the layer metadata. 

        :param layer_metadata: The metadata of the layer, including assets.
        :type layer_metadata: dict
        :return: A list of unique variable names found in the dataset.
        :rtype: list[str]
        """
        arco_variables = []
        logger.info(f"looking for variables in {layer_metadata['converted_arco_assets']}")
        for asset in layer_metadata['converted_arco_assets']:
            if asset.endswith('.zarr') or '.zarr/' in asset:
                logger.info(f"reading zarr {asset}")
                ds = xr.open_dataset(asset, engine='zarr')
                for var in ds.data_vars:
                    if var not in arco_variables:
                        arco_variables.append(var)
            elif asset.endswith('.parquet') or '.parquet/' in asset:
                logger.info(f"reading parquet {asset}")
                df = S3Utils.read_parquet_from_s3(asset)
                for column in df.columns:
                    if column in ['geometry', 'datetime'] or column in arco_variables:
                        continue
                    arco_variables.append(column)
        logger.info(f"found variables {arco_variables}")
        return arco_variables

class EMODnetS3:
    """
    This class provides methods to interact with the EMODnet S3 bucket. Creates boto3client, and provides attributes to 
    upload, delete, move, sync files to the S3 bucket.
    Loads credentials from 'emods3.env' file in the 'data/creds' directory. 
    Contact admin for access to the 'emodaws.env' file.
    """
    def __init__(self, pipeline_config: dict):
        """
        :param pipeline_config: The pipeline configuration dictionary.
        :type pipeline_config: dict
        """
        datadir = pipeline_config['datadir']
        dotenv_path = f'{datadir}/creds/emods3.env'
        if dotenv.load_dotenv(dotenv_path):
            logger.debug(f"Loaded .env file from {dotenv_path}")
        else:
            logger.error(f"Failed to load .env file from {dotenv_path}")
        self.access_key = os.getenv('EMOD_ACCESS_KEY')
        self.secret_key = os.getenv('EMOD_SECRET_KEY')
        self.bucket = 'emodnet'
        self.host = 'https://s3.waw3-1.cloudferro.com'
        self.botoclient = boto3.client('s3', 
                                aws_access_key_id=self.access_key, 
                                aws_secret_access_key=self.secret_key, 
                                endpoint_url=self.host
                                )
