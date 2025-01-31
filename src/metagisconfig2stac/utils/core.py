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
import s3fs
import urllib
from urllib.parse import urlparse, parse_qs
from pytz import utc
import json
import requests
import yaml
import sys
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from metagisconfig2stac.layer import Layer
from datetime import datetime

logger = logging.getLogger(__name__)
wkdir = os.path.dirname(os.path.realpath(__file__))
today = datetime.now().strftime('%Y-%m-%d')
class CoreUtils:
    def __init__(self):
        pass

    @staticmethod
    def get_logger(    
            LOG_FORMAT     = '%(asctime)s %(name)-12s %(levelname)-8s %(message)s script:%(filename)s line:%(lineno)d',
            LOG_NAME       = '',
            LOG_DIRECTORY  = f'{wkdir}/../../../data/logs/',
            append_logs    = False):
        """
        Configures and returns a logger with handlers for info, error, and warning levels.

        :param LOG_FORMAT: Format string for log messages. Defaults to a custom format with time, level, and message.
        :type LOG_FORMAT: str, optional
        :param LOG_NAME: Name of the logger. Defaults to an empty string.
        :type LOG_NAME: str, optional
        :param LOG_DIRECTORY: Directory where log files will be saved.'.
        :type LOG_DIRECTORY: str, optional
        :param append_logs: If True, logs will be appended to existing files, otherwise overwritten. Defaults to False.
        :type append_logs: bool, optional
        :return: Configured logger instance.
        :rtype: logging.Logger
        """
        mode = 'a' if append_logs else 'w'
        
        os.makedirs(LOG_DIRECTORY, exist_ok=True)
        log_file_info = os.path.join(LOG_DIRECTORY, "info_log.txt")
        log_file_error = os.path.join(LOG_DIRECTORY, "error_log.txt")
        log_file_warning = os.path.join(LOG_DIRECTORY, "warning_log.txt")

        log           = logging.getLogger(LOG_NAME)
        log_formatter = logging.Formatter(LOG_FORMAT)
        
        log_handler_info = logging.FileHandler(log_file_info, mode=mode)
        log_handler_info.setFormatter(log_formatter)
        log_handler_info.setLevel(logging.INFO)
        log.addHandler(log_handler_info)

        error_log_formatter = logging.Formatter('%(asctime)s - %(filename)s - %(message)s')
        log_handler_error = logging.FileHandler(log_file_error, mode=mode)
        log_handler_error.setFormatter(error_log_formatter)
        log_handler_error.setLevel(logging.ERROR)
        log.addHandler(log_handler_error)

        log_handler_warning = logging.FileHandler(log_file_warning, mode=mode)
        log_handler_warning.setFormatter(log_formatter)
        log_handler_warning.setLevel(logging.WARNING)
        log.addHandler(log_handler_warning)

        log.setLevel(logging.INFO)

        return log

    @staticmethod
    def get_session(total_retries=3, backoff_factor=1, status_forcelist=(500, 502, 503, 504), timeout=120):
        """
        Configures and returns an HTTP session with retry strategy.

        :param total_retries: Maximum number of retries for failed requests. Defaults to 3.
        :type total_retries: int, optional
        :param backoff_factor: Factor by which the delay between retries increases. Defaults to 1.
        :type backoff_factor: int, optional
        :param status_forcelist: List of HTTP status codes to trigger a retry. Defaults to (500, 502, 503, 504).
        :type status_forcelist: tuple, optional
        :param timeout: Timeout for requests in seconds. Defaults to 120.
        :type timeout: int, optional
        :return: Configured requests session.
        :rtype: requests.Session
        """
        
        session = requests.Session()
        retry = Retry(
            total=total_retries,  
            backoff_factor=backoff_factor, 
            status_forcelist=status_forcelist
        )
        adapter = HTTPAdapter(max_retries=retry)
        session.mount('http://', adapter)
        session.mount('https://', adapter)
        return session

    # Custom request function that uses the provided timeout
    @staticmethod
    def custom_request(session, method, url, timeout=120, **kwargs):

        """
        Makes an HTTP request using a custom session.

        :param session: Configured requests session.
        :type session: requests.Session
        :param method: HTTP method to use (GET, POST, etc.).
        :type method: str
        :param url: URL to send the request to.
        :type url: str
        :param timeout: Timeout for the request. Defaults to 120 seconds.
        :type timeout: int, optional
        :param kwargs: Additional parameters for the request.
        :return: HTTP response object.
        :rtype: requests.Response
        """

        if 'timeout' not in kwargs:
            kwargs['timeout'] = timeout
        return session.request(method, url, **kwargs)

    @staticmethod
    def fetch_json_data(url):
        """
        Fetches JSON data from a given URL using a retry-enabled session.

        :param url: URL from which to fetch the JSON data.
        :type url: str
        :return: Parsed JSON data if the request is successful, or None if it fails.
        :rtype: dict or None
        """
        # Get a configured session
        session = CoreUtils.get_session(total_retries=3, timeout=120)

        try:
            # Determine whether to verify SSL certificates
            verify = True
            if url == 'https://emodnet.development.ec.europa.eu/geoviewer/edito.php':
                verify = False
            
            # Make the request with the custom session
            response = CoreUtils.custom_request(session, 'GET', url, timeout=120, verify=verify)
            response.raise_for_status()  # Check if the request was successful
            
            return response.json()
        except requests.RequestException as e:
            print(f"Request failed: {e}")
            return None

    @staticmethod
    def empty_directory(directory_path):
        """
        Empties the contents of a specified directory. Deletes all files and directories within it.

        :param directory_path: Path to the directory to be emptied.
        :type directory_path: str
        """
        try:
            # Verify that the directory exists
            if os.path.exists(directory_path):
                # Iterate over all the files and directories inside the given directory
                for entry in os.listdir(directory_path):
                    entry_path = os.path.join(directory_path, entry)
                    # If the entry is a file, delete it
                    if os.path.isfile(entry_path):
                        os.remove(entry_path)
                    # If the entry is a directory, delete it recursively
                    elif os.path.isdir(entry_path):
                        shutil.rmtree(entry_path)
            else:
                logger.warning(f"The directory '{directory_path}' does not exist.")
        except Exception as e:
            logger.warning(f"An error occurred while cleaning up the directory: {e}")


    def read_secrets(creds_file):
        """
        Reads AWS credentials from a specified file.

        :param creds_file: Path to the file containing AWS credentials.
        :type creds_file: str
        :return: Tuple containing the AWS access key ID and secret access key.
        :rtype: tuple(str, str)
        """
        with open(creds_file, 'r') as creds:
            aws_credentials = {line.split('=')[0].strip(): line.split('=')[1].strip().strip("'") for line in creds}

        access_key_id = aws_credentials.get('AWS_ACCESS_KEY_ID')
        secret_access_key = aws_credentials.get('AWS_SECRET_ACCESS_KEY')

        return access_key_id, secret_access_key

    def is_valid_download_type(download_url):
        """
        Checks whether the given URL is a valid download type based on file extension or URL patterns.

        :param download_url: URL to check.
        :type download_url: str
        :return: The URL if it's valid, or None otherwise.
        :rtype: str or None
        """
        valid_endings = ['.zip', '.nc', '.parquet', '.csv', '.parquet/', '.geoparquet/']
        valid_patterns = ['mda.vliz.be/download.php?file=VLIZ', 'parquet/', 'geoparquet/']

        if any(download_url.endswith(pattern) for pattern in valid_endings):
            return download_url
        if any(pattern in download_url for pattern in valid_patterns):
            return download_url


    def test_download_url(url):
        """
        Tests if a download URL is working by making a GET request.

        :param url: The URL to test.
        :type url: str
        :return: True if the URL is valid, False otherwise.
        :rtype: bool
        """
        try:
            response = requests.get(url, stream=True)
            if response.status_code == 200:
                logger.info(f"{url} is working.")
                return True
            else:
                logger.info(f"{url} is not working. Status code:", response.status_code)
        except requests.exceptions.RequestException as e:
            logger.info("Error testing URL:", e)


    def find_possible_stac_extensions(item):
        """
        Identify possible STAC extensions based on the item properties and assets.

        :param item: The STAC item as a dictionary.
        :type item: dict
        :return: None. Appends appropriate STAC extension URLs to a list.
        :rtype: None
        """
        extensions = []
        if 'properties' in item:
            if 'proj:epsg' in item['properties']:
                extensions.append('https://stac-extensions.github.io/projection/v1.1.0/schema.json')
            if 'zarr' in item['assets']:
                extensions.append('https://stac-extensions.github.io/datacube/v1.0.0/schema.json')


    def custom_slugify(text, separator='_'):
        """
        Create a slugified version of a text string.

        :param text: The input text to be slugified.
        :type text: str
        :param separator: The separator to replace spaces and special characters.
        :type separator: str, optional
        :return: The slugified text.
        :rtype: str
        """
        # Remove leading dots and spaces
        text = re.sub(r'^[.\s]+', '', text)

        # remove trailing dots and spaces
        text = re.sub(r'[.\s]+$', '', text)
        #specifically replace / with underscore
        text = re.sub(r'/', '_', text)

        # Replace special characters with separator
        text = re.sub(r'[-\s]+', separator, text)

        # Replace multiple separator with separator
        text = re.sub(r'__', separator, text)

        # Replace dots followed by a digit with separator
        text = re.sub(r'\.(\d)', r'.\1', text)

        # Remove other non-alphanumeric characters
        text = re.sub(r'[^\w\s.-]', '', text)

        #remove exponents
        text = re.sub(r'²', '_2', text)
        text = re.sub(r'³', '_3', text)

        return text.lower()

    def get_download_content_disposition(response):
        """
        Extract the filename from the Content-Disposition header of an HTTP response.

        :param response: The HTTP response object.
        :type response: requests.Response
        :return: The filename from the Content-Disposition header, or None if not found.
        :rtype: str or None
        """
        headers = response.headers
        content_disposition = response.headers.get('Content-Disposition')
        if content_disposition:
            filename = re.findall('filename="(.+)"', content_disposition)
            if filename:
                return filename[0]
        return None


    def download_asset(download_url, temp_dir):
        """
        Download an asset from a URL to a local temporary directory.

        :param download_url: The URL of the asset to download.
        :type download_url: str
        :param temp_dir: The local directory where the file will be saved.
        :type temp_dir: str
        :return: The local path to the downloaded file, or None on failure.
        :rtype: str
        """
        try:
            response = requests.get(download_url, timeout=30, stream=True)
            response.raise_for_status()
        except requests.RequestException as e:
            logger.warning(f"{download_url} Failed {e}")
            return None
        
        # try to get the filename from the Content-Disposition header
        file_name = CoreUtils.get_download_content_disposition(response)
        if not file_name:
            file_name = os.path.basename(download_url)  # Fallback to URL-derived filename

        os.makedirs(temp_dir, exist_ok=True)
        temp_file_path = os.path.join(temp_dir, file_name)
        total_size = int(response.headers.get('Content-Length', 0))
        # download with progress bar
        try:
            with tqdm(total=total_size, unit='B', unit_scale=True, desc=file_name, ncols=80) as pbar:
                with open(temp_file_path, 'wb') as temp_file:
                    for chunk in response.iter_content(chunk_size=15192):
                        if chunk:
                            temp_file.write(chunk)
                            pbar.update(len(chunk))
        except Exception as e:
            logger.error(f"Failed to download {file_name} from {download_url}: {str(e)}")
            return
        logger.info(f"Downloaded {file_name} from {download_url}")
        return temp_file_path


    def get_asset_url_name(download_url):
        """
        Generate the appropriate asset URL name based on the URL or specific download conditions.

        :param download_url: The URL of the asset to be downloaded.
        :type download_url: str
        :return: The name of the asset, usually derived from the URL or a custom condition.
        :rtype: str
        """
        parsed_url = urlparse(download_url)
        if "outputFormat=SHAPE-ZIP" in download_url:
            layer_name = CoreUtils.get_layer_name_from_wfs_url(download_url)
            return f"{layer_name}.zip"
        return os.path.basename(parsed_url.path)


    def get_layer_name_from_wfs_url(wfs_url):
        """
        Extract the layer name from a WFS URL.

        :param wfs_url: The WFS URL.
        :type wfs_url: str
        :return: The extracted layer name.
        :rtype: str
        """
        parsed_url = urlparse(wfs_url)
        query_params = parse_qs(parsed_url.query)
        layer_name = query_params.get('typeName', [None])[0]
        if layer_name:
            layer_name = layer_name.split(':')[-1]
        return layer_name


    @staticmethod
    def get_arco_assets_variables(layermetadata: dict):
        """
        List variables from converted ARCO datasets that are found in the layer metadata

        :param layer_metadata: The metadata of the layer, including assets.
        :type layer_metadata: dict
        :return: A list of unique variable names found in the dataset.
        :rtype: list[str]
        """
        from metagisconfig2stac.utils.s3 import S3Utils
        arco_variables = []
        for asset in layermetadata['converted_arco_assets']:
            if '.parquet' in asset or '.parquet/' in asset:
                logging.info(f'reading parquet from {asset} using CoreUtils')
                ddf = S3Utils.read_parquet_from_s3(asset)
                for column in ddf.columns:
                    if column in ['geometry', 'datetime'] or column in arco_variables:
                        continue
                    arco_variables.append(column)
            
            elif '.zarr' in asset or '.zarr/' in asset:
                logging.info(f'reading parquet from {asset} using xarray')
                ds = xr.open_dataset(asset, engine='zarr')
                for var in ds.data_vars:
                    if var not in arco_variables:
                        arco_variables.append(var)
                if 'categorical_encoding' in ds.attrs and ds.attrs['categorical_encoding']:
                    layermetadata['categorical_encoding'] = ds.attrs['categorical_encoding']
        return arco_variables

    def get_arco_variable_encoding(layer_metadata, arco_variable):
        """
        Get the encoding of an ARCO variable from the layer metadata.

        :param layer_metadata: The metadata of the layer, including the ARCO variable encoding.
        :type layer_metadata: dict
        :param arco_variable: The ARCO variable to get the encoding for.
        :type arco_variable: str
        :return: The encoding of the ARCO variable.
        :rtype: dict
        """
        return layer_metadata['arco_variable_encoding'].get(arco_variable, {})

    @staticmethod
    def load_local_json(filepath):
        """
        Load a JSON file from the local filesystem.

        :param filepath: The path to the JSON file.
        :type filepath: str
        :return: The contents of the JSON file as a dictionary, or None if an error occurs.
        :rtype: dict or None
        """
        try:
            with open(filepath, 'r') as f:
                try:
                    logger.info(f"Reading file content{filepath}")
                    file_content = f.read()
                    try:
                        dict = json.loads(file_content)
                        return dict
                    except Exception as e:
                        logger.error(f"Error decoding JSON from file {filepath}: {e}")    
                except Exception as e:
                    logger.error(f"Error reading file {filepath}: {e}")
        except json.JSONDecodeError as e:
            logger.error(f"Error decoding JSON from file {filepath}: {e}")
        except FileNotFoundError as e:
            logger.error(f"File not found: {e}")
        except IOError as e:
            logger.error(f"IO error when handling file {filepath}: {e}")
        return None


    @staticmethod
    def validate_config(config):
        """
        Validate the configuration dictionary for the pipeline. Ensure that all required parameters are present and
        have the correct types. Add default values for optional parameters if they are not present.

        :param config: The configuration dictionary to validate.
        :type config: dict

        :return: The validated configuration dictionary.
        :rtype: dict
        """
        # Define required parameters and their types
        REQUIRED_PARAMS = {
            'layer_collection': str,
            'layer_collection_config': str,
            'stac_title': str,
        }

        # Define default values for optional parameters
        DEFAULTS = {
            'stac_s3': f"teststac_{today}",
            'previous_layer_catalog': '',
            'thematic_lots_to_process': [],
            'transfer_native_to_s3': False,
            'convert_arco': False,
            'create_backup_stac': False,
            'stac_s3_backup': '',
            'cp_transformation_table': False,
            'central_portal_php_config': '',
            'attributes_table': False,
            'attribute_s3': '',
            'push_to_resto': False,
            'resto_instance': ''
        }
        for key, expected_type in REQUIRED_PARAMS.items():
            if key not in config:
                raise ValueError(f"Missing required parameter: {key}")
            if not isinstance(config[key], expected_type):
                raise TypeError(f"Parameter '{key}' must be of type {expected_type.__name__}")

        # Add defaults for optional parameters if not present
        for key, default_value in DEFAULTS.items():
            config.setdefault(key, default_value)

        return config

    @staticmethod
    def load_yaml(config_file_path):
        """Load and validate YAML configuration file.
        
        :param config_file_path: Path to the YAML configuration file.
        :type config_file_path: str
        :return: The configuration dictionary.
        :rtype: dict
        """
        try:
            with open(f"{config_file_path}", 'r') as file:
                config = yaml.safe_load(file)
                if not isinstance(config, dict):
                    raise ValueError("Invalid YAML format. Expected a dictionary at the root level.")
                return config
        except FileNotFoundError:
            sys.exit(f"Error: Configuration file f'{config_file_path}' not found.")
        except yaml.YAMLError as e:
            sys.exit(f"Error parsing YAML file: {e}")
        except (ValueError, TypeError) as e:
            sys.exit(f"Error in configuration: {e}")
