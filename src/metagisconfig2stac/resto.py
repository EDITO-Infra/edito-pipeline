import argparse
import requests
import json
import pystac
import os
import pandas as pd
from datetime import datetime
import logging
import dotenv

today = datetime.now().strftime("%Y-%m-%d")

logger = logging.getLogger(__name__)
class RestoClient:
    """
    Class to interact with the RESTO API. 
    """
    def __init__(self, resto_instance: str, credentials_path: str):
        """
        :param resto_instance: The RESTO instance to interact with.
        :type resto_instance: str
        :param credentials_path: The path to the credentials file.
        :type credentials_path: str
        """
        self.resto_instance = resto_instance
        self.credentials_path = credentials_path
        self.load_credentials()
        self.get_initial_access_token()

    def load_credentials(self):
        """
        Load the RESTO credentials from the credentials file.  Update the class attributes with the credentials.
        """
        dotenv.load_dotenv(self.credentials_path)
        self.resto_user = os.getenv('RESTO_USERNAME')
        self.resto_password = os.getenv('RESTO_PASSWORD')
        logger.info(f"Loaded credentials for {self.resto_user}")

    def get_initial_access_token(self):
        """
        Get the initial access token from the RESTO API.  Update the class attribute with the initial access token.
        """
        url = f"https://auth.lab.{self.resto_instance}.edito.eu/auth/realms/datalab/protocol/openid-connect/token"
        response = requests.post(url, data={
            'Content-Type': 'application/x-www-form-urlencoded',
            'client_id': "edito",
            'username': self.resto_user,
            'password': self.resto_password,
            'grant_type': "password",
            'scope': "openid"
        })
        self.token = response.json()['access_token']
        logger.info(f"Initial token: {self.token}")

    def log_api_request(self, method, url, headers, data=None):
        logger.info(f"{method} request to {url}")
        logger.info(f"Headers: {json.dumps(headers, indent=2)}")
        if data:
            logger.info(f"Data: {json.dumps(data, indent=2)}")
        else:
            logger.info(f"No data, {method} request to {url}")
            
    def make_api_request(self, method, url, headers, data=None):
        try:
            if method == 'POST':
                response = requests.post(url, headers=headers, json=data)
            elif method == 'PUT':
                response = requests.put(url, headers=headers, json=data)
            elif method == 'DELETE':
                response = requests.delete(url, headers=headers)
            elif method == 'GET':
                response = requests.get(url, headers=headers)
            else:
                raise ValueError(f"Unsupported method: {method}")

            logger.info(f"Response from {url}: {response.json()}")
            return response
        except Exception as e:
            logger.error(f"Failed to {method} data to {url}: {e}")
            return None

    def get_headers(self):
        return {
            'Accept': 'application/json',
            'Content-Type': 'application/json',
            'Authorization': f'Bearer {self.token}'
        }

    def post_data(self, url, data, update=True):
        headers = self.get_headers()
        self.log_api_request('POST', url, headers, data)
        response = self.make_api_request('POST', url, headers, data)
        if response is None:
            return None

        if response.status_code == 401:
            logger.info(f"Token expired, refreshing.")
            self.get_initial_access_token()
            return self.post_data(url, data, update)
        
        if response.status_code == 409 and update:
            logger.info(f"Feature already exists, updating.")
            return self.update_data(url, data)

        return response.json()


    def update_data(self, url, data):
        headers = self.get_headers()
        self.log_api_request('PUT', f"{url}{data['id']}", headers, data)
        response = self.make_api_request('PUT', f"{url}{data['id']}", headers, data)
        if response is None:
            return None

        if response.status_code == 401:
            logger.info(f"Token expired, refreshing.")
            self.get_initial_access_token()
            return self.update_data(url, data)
        return response.json()


    def delete_data(self, url):
        headers = self.get_headers()
        self.log_api_request('DELETE', url, headers)
        response = self.make_api_request('DELETE', url, headers)
        if response is None:
            return None
        return response.json()

    def get_data(self, url):
        headers = self.get_headers()
        self.log_api_request('GET', url, headers)
        response = self.make_api_request('GET', url, headers)
        if response is None or response.status_code != 200:
            logger.error(f"Failed to retrieve data from {url}")
            return None
        return response.json()

    def get_all_user_features(self):
        """
        Get the user features from the RESTO API.  Get the user features and save as csv file in the RESTO logs directory.

        :param user: The user to get the features for.
        :type user: str
        """
        
        limit = 100

        init_search_url = f"https://api.{self.resto_instance}.edito.eu/data/search?owner={self.resto_user}&limit={limit}"
        first_response = requests.get(init_search_url)
        if first_response.status_code != 200:
            logger.error(f"Failed to retrieve initial search results from {self.resto_instance}")
            return

        number_matched = first_response.json().get('numberMatched')
        logger.info(f"Total matches: {number_matched}")
        

        my_features_df = pd.DataFrame(columns=["resto_id", "product_id", "collection"])
        self.myfeatures = {}
        
        resplinks = first_response.json().get('links', [{}])
        nextlink = None
        for link in resplinks:
            if link['rel']  == 'next':
                nextlink = link['href']

        while nextlink:
            next_response = requests.get(nextlink)

            if next_response.status_code != 200:
                logger.error(f"Failed to retrieve next search results from {self.resto_instance}")
                return
            for feature in next_response.json().get('features', []):
                self.myfeatures[feature.get('id')] = {
                    "product_id": feature.get('properties', {}).get('productIdentifier'),
                    "collection": feature.get('collection')
                }
                my_features_df = pd.concat([my_features_df, pd.DataFrame([{
                    "resto_id": feature.get('id'),
                    "product_id": feature.get('properties', {}).get('productIdentifier'),
                    "collection": feature.get('collection')
                }])], ignore_index=True)
            resplinks = next_response.json().get('links', [{}])
            nextlink = None
            for link in resplinks:
                if link['rel']  == 'next':
                    nextlink = link['href']

        if nextlink is None:
            logger.info(f"retrieved {len(my_features_df)} features")
            return my_features_df

class STACRestoManager:
    """
    This class is responsible for posting STAC data to a specified Resto instance('staging' or 'dive').
    First initialize the class with the pipeline configuration. Generate the initial and final access tokens using the credentials
    stored in the credentials file in 
    Then post the STAC data to the Resto database. The class also has methods for deleting features from the Resto database.

    If you don't have credentials to ingest features onto Resto, ask the Resto admin to provide you with the credentials.
    """
    def __init__(self, pipeline_config: dict, local_stac: pystac.Catalog):
        """
        Initialize the class passing it the local STAC, and the resto instance to ingest on.  
        Generate the initial and final access tokens using the credentials stored in the credentials file.
        Add attributes to the class for the initial and final access tokens, the local STAC, 
        the local STAC directory, the RESTO logs directory, and the RESTO instance.
        
        :param pipeline_config: The pipeline configuration dictionary.
        :type pipeline_config: dict
        
        """
        # Set the resto instance, default to staging
        self.resto_instance = pipeline_config.get('resto_instance', 'staging')
        self.pipeline_config = pipeline_config
        self.wkdir = os.path.dirname(os.path.abspath(__file__))
        self.local_stac = local_stac
        self.restologs = f'{self.pipeline_config['datadir']}/restologs'
        os.makedirs(self.restologs, exist_ok=True)
        self.client = RestoClient(self.resto_instance, f'{self.pipeline_config['datadir']}/creds/resto.env')
        self.delete_log = {}
        self.myfeatures = {}
        self.posted_logs = {}


    def backup_resto_logs(self, pipeline_config):
        """
        Backup the RESTO logs to the S3 bucket.  Get the list of RESTO logs and upload them to the S
        3 bucket specified in the pipeline configuration.
        """
        from metagisconfig2stac.utils.s3 import S3Utils, EMODnetS3
        logfiles = os.listdir(self.restologs)
        for csv in logfiles:
            if csv.endswith('.csv'):
                filepath = os.path.join(self.restologs, csv)
                s3_loc = self.pipeline_config['stac_s3']
                emod_s3 = EMODnetS3(self.pipeline_config)
                botoclient = emod_s3.botoclient
                bucket = emod_s3.bucket
                host = emod_s3.host
                s3_url = S3Utils.upload_to_s3(botoclient, bucket, host, filepath, s3_loc)
                logger.info(f"Uploaded {csv} to {s3_url}")
        return

    def post_local_stac(self) -> None:
        """
        Post the local STAC to the RESTO API.  Post root catalog, then variable family catalogs, then collections, then items.
        Record the responses in the RESTO logs, save in csv format in 'data/restologs'
        """
        cat_posts = []
        vfc_posts = []
        coll_posts = []
        item_posts = []

        catalog_data = self.local_stac.to_dict()

        logger.info(f"posting {catalog_data['id']} catalog")
        # here we post the root catalog
        cat_post_resp = self.post_catalog_edito(catalog_data)
        cat_post_resp['catalog_id'] = catalog_data['id']
        cat_posts.append(cat_post_resp)
        
        vfc_count = 0
        for vfc in self.local_stac.get_children():
            # if vfc_count >= 10:
            #     break
            vfc_data = vfc.to_dict()

            # here we post the variable family catalog to the variable family catalogs
            vfc_resp = self.post_to_child_catalog(vfc_data, f"variable_families")
            
            vfc_resp2 = self.post_to_child_catalog(vfc_data, catalog_data['id'])

            for resp in [vfc_resp, vfc_resp2]:
                if resp is None:
                    resp = {}
                    resp['error'] = "Failed to post variable family catalog"
                    resp['variable_family_catalog'] = vfc.id
                    resp['catalog_id'] = vfc_data['id']
                resp['catalog_id'] = vfc_data['id']
                vfc_posts.append(resp)
            
            coll_count = 0
            for coll in vfc.get_children():
                # if coll_count >= 3:
                #     break
                
                coll_data = coll.to_dict()
                logger.info(f"posting collection {coll_data['id']} to /collections")
                 # post the collection to the collections
                coll_resp1 = self.post_collection(coll_data)

                #post to the variable family catalog
                coll_resp2 = self.post_collection_to_child_catalog(coll_data, vfc_data['id'])

                # post to the root catalog
                coll_resp3 = self.post_collection_to_child_catalog(coll_data, f"{catalog_data['id']}/{vfc_data['id']}")

                # log the responses
                for resp in [coll_resp1, coll_resp2, coll_resp3]:
                    if resp is None:
                        resp = {}
                        resp['error'] = "Failed to post collection"
                        resp['variable_family_catalog'] = vfc.id
                        resp['collection_id'] = coll_data['id']
                        
                    resp['collection_id'] = coll_data['id']
                    coll_posts.append(resp)
                
                item_count = 0
                for item in coll.get_all_items():
                    # if item_count >= 10:
                    #     break
                    item_data = item.to_dict()

                    logger.info(f"posting item {item_data['id']} to collection {item_data['collection']}")
                    item_resp = self.post_item(item_data)
                    if item_resp is None:
                        item_resp = {}
                        item_resp['error'] = "Failed to post item"
                        item_resp['variable_family_catalog'] = vfc.id
                        item_resp['item_id'] = item_data['id']
                        item_resp['collection_id'] = item_data['collection']
                        item_posts.append(item_resp)
                    
                    item_resp['variable_family_catalog'] = vfc.id
                    item_resp['collection_id'] = item_data['collection']
                    item_resp['item_id'] = item_data['id']
                    item_posts.append(item_resp)
                    item_count += 1
                coll_count += 1
            vfc_count += 1
        
        # Log and save response
        cat_post_df = pd.DataFrame(cat_posts).to_csv(f"{self.restologs}/{self.resto_instance}_posted_catalogs_{today}.csv", index=False)
        #vfc_post_df = pd.DataFrame(vfc_posts).to_csv(f"{self.restologs}/posted_variable_families_{today}.csv", index=False)
        coll_post_df = pd.DataFrame(coll_posts).to_csv(f"{self.restologs}/{self.resto_instance}_posted_collections_{today}.csv", index=False)
        item_post_df = pd.DataFrame(item_posts).to_csv(f"{self.restologs}/{self.resto_instance}_posted_items_{today}.csv", index=False)
        logger.info(f"Posted catalogs, variable families, collections, and items to RESTO")


    def post_catalog_edito(self, catalog_data):
        """
        Post the a Catalog to the root catalog in EDITO, ex a variable family catalog. Set data['links] parent link to root catalog.
        
        :param catalog_data: The catalog data to post.
        :type catalog_data: dict

        :return: The response from the RESTO API.
        :rtype: dict
        """

        post_url = f"https://api.{self.resto_instance}.edito.eu/data/catalogs/"
        catalog_data['links'] = self.cleanup_links(catalog_data['links'], 'root', f'https://api.{self.resto_instance}.edito.eu/data/catalogs/')
        return self.client.post_data(post_url, catalog_data)

    def post_to_child_catalog(self, data, child_catalog):
        """
        Post the data to the child catalog. Set data['links'] parent link to the child catalog.
        
        :param data: The data to post.
        :type data: dict
        :param child_catalog: The child catalog to post the data to.
        :type child_catalog: str
        
        :return: The response from the RESTO API.
        :rtype: dict
        """
        post_url = f"https://api.{self.resto_instance}.edito.eu/data/catalogs/{child_catalog}/"
        data['links'] = []
        return self.client.post_data(post_url, data)

    def post_collection(self, collection_data):
        """
        Post collection to 'data/collections'. Set collection_data['links'] parent link to '/collections'.

        :param collection_data: The collection data to post.
        :type collection_data: dict

        :return: The response from the RESTO API.
        :rtype: dict
        """
        post_url = f"https://api.{self.resto_instance}.edito.eu/data/collections/"
        collection_data['links'] = self.cleanup_links(collection_data['links'], 'parent', f'https://api.{self.resto_instance}.edito.eu/data/collections/')
        return self.client.post_data(post_url, collection_data)

    def post_collection_to_child_catalog(self, collection_data, child_catalog):
        """
        Post collection to a child catalog. Set collection_data['links'] parent link to the child catalog.

        :param collection_data: The collection data to post.
        :type collection_data: dict
        :param child_catalog: The child catalog to post the collection to.
        :type child_catalog: str

        :return: The response from the RESTO API.
        :rtype: dict
        """
        post_url = f"https://api.{self.resto_instance}.edito.eu/data/catalogs/{child_catalog}/"
        collection_data['links'] = self.cleanup_links(collection_data['links'], 'parent', f'https://api.{self.resto_instance}.edito.eu/data/catalogs/{child_catalog}')
        return self.client.post_data(post_url, collection_data)
    
    def post_item(self, item_data):
        """
        Post item to its collection. Set item_data['links'] parent link to '/collections/{collection}/items/'.

        :param item_data: The item data to post.
        :type item_data: dict

        :return: The response from the RESTO API.
        :rtype: dict
        """
        post_url = f"https://api.{self.resto_instance}.edito.eu/data/collections/{item_data['collection']}/items/"
        item_data['links'] = self.cleanup_links(item_data['links'], 'parent', f'https://api.{self.resto_instance}.edito.eu/data/collections/{item_data['collection']}/items/')
        return self.client.post_data(post_url, item_data)


    def cleanup_links(self, links, rel_to_replace, new_href):
        """
        Removes unnecessary links from the data and replaces the href of the link with the new href.
        Ex variable family catalog links should have a parent link to the root catalog. Collections should have a parent link to '/collections'.
        Items should have a parent link to '/collections/{collection}/items/'.
        """
        new_links = []
        for link in links:
            if link['rel'] == rel_to_replace:
                link['href'] = new_href
                new_links.append(link)
        return new_links


    def delete_catalog(self, catalogid):
        """
        Delete catalog from the RESTO API.  Note you must be the owner of the catalog to delete it.

        :param catalogid: The catalog to delete.
        :type catalogid: str
        """
        post_url = f"https://api.{self.resto_instance}.edito.eu/data/catalogs/{catalogid}"

        response = self.client.delete_data(post_url)
        if response is None:
            logger.error(f"Failed to delete catalog {catalogid}")
            return
        return response

    def delete_collection_from_root_catalog(self, collectionid):
        """
        Delete collection from the RESTO API.  Note you must be the owner of the collection to delete it.

        :param collectionid: The collection to delete.
        :type collectionid: str
        """
        post_url = f"https://api.{self.resto_instance}.edito.eu/data/collections/{collectionid}"
        response = self.client.delete_data(post_url)
        if response is None:
            logger.error(f"Failed to delete collection {collectionid}")
            return
        return response


    def delete_collection_from_catalog(self, collectionid, catalogid):
        """
        Delete a collection reference from within a catalog in the RESTO API.  Note this deletes only the collection reference not the collection itself.
        Note you must be the owner of the collection and catalog to delete the reference.
        
        :param collection: The collection to delete.
        :type collection: str
        :param catalog: The catalog to delete the collection from.
        :type catalog: str
        
        :return: The response from the RESTO API.
        :rtype: dict
        """
        post_url = f"https://api.{self.resto_instance}.edito.eu/data/catalogs/{catalogid}/{collectionid}"
        response = self.client.delete_data(post_url)
        if response is None:
            logger.error(f"Failed to delete collection {collectionid} from catalog {catalogid}")
            return
        return response
    

    def delete_single_feature(self, product_id, collectionid, resto_id):
        """
        Delete a single feature from the RESTO API.  Use the product_id, collection, and resto_id to delete the feature.
        
        :param product_id: The product_id of the feature to delete.
        :type product_id: str
        :param collection: The collection of the feature to delete.
        :type collection: str
        :param resto_id: The resto_id of the feature to delete.
        :type resto_id: str
        
        :return: The response from the RESTO API.
        :rtype: dict
        """

        post_url = f"https://api.{self.resto_instance}.edito.eu/data/collections/{collectionid}/items/{resto_id}"
        response = self.client.delete_data(post_url)
        if response is None:
            logger.error(f"Failed to delete product_id {product_id} from collection {collectionid}")
            return
        return response


    def get_local_stac_features(self):
        """
        Get the local STAC features from the local STAC directory.  Get the collections and features from the local STAC
        directory and update the class attribute with the local features.
        
        :return: The local STAC features.
        :rtype: list
        """
        logger.info(f"Getting local STAC features from {self.local_stac}/catalog.json")
        collections = pystac.Catalog.from_file(f"{self.local_stac}/catalog.json").get_all_collections()
        self.local_features = []
        for collection in collections:
            for feature in collection.get_all_items():
                self.local_features.append(feature)
        logger.info(self.local_features)
        return self.local_features


    def get_catalog(self, catalog_id):
        logger.info(f"Getting catalog {catalog_id}")
        post_url = f"https://api.{self.resto_instance}.edito.eu/data/catalogs/{catalog_id}"
        response = self.client.get_data(post_url)
        if response is None:
            logger.error(f"Failed to get catalog {catalog_id}")
            return
        return response
    
    
    def get_collection(self, collection_id):
        logger.info(f"Getting collection {collection_id}")
        post_url = f"https://api.{self.resto_instance}.edito.eu/data/collections/{collection_id}"
        response = self.client.get_data(post_url)
        if response is None:
            logger.error(f"Failed to get collection {collection_id}")
            return
        return response
    
    def get_item(self, collection_id, item_id):
        logger.info(f"Getting item {item_id} from collection {collection_id}")
        post_url = f"https://api.{self.resto_instance}.edito.eu/data/collections/{collection_id}/items/{item_id}"
        response = self.client.get_data(post_url)
        if response is None:
            logger.error(f"Failed to get item {item_id} from collection {collection_id}")
            return
        return response
    

    def match_features(self):
        matched_features = []
        resto_features_df = self.client.get_all_user_features()
        for feature in self.local_features:
            localid = feature.id
            user_features_ids = list(resto_features_df['product_id'])
            # Check if the feature is in the user features
            if localid in user_features_ids:
                resto_feature = resto_features_df[resto_features_df['product_id'] == localid]
                matched_features.append(resto_feature)
        self.matched_features = matched_features
        return matched_features