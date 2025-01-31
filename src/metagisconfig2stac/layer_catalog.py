import requests
import json
import os
from datetime import datetime
import logging
from typing import Any, Dict, List
from metagisconfig2stac.utils.core import CoreUtils
from metagisconfig2stac.layer import Layer
logger = logging.getLogger(__name__)

today = datetime.today().strftime('%Y-%m-%d')
wkdir = os.path.dirname(os.path.abspath(__file__))

class LayerCatalog:
    """"
    The LayerCatalog class represents a collection of layers in the Metagis catalog.
    Creating the layer catalog involves fetching the layer collection data from the Metagis database from either a config endpoint
    or a previously saved json file. The layer catalog is then created by iterating through the layer collection data and creating
    a Layer instance for each layer in the collection. Adding subtheme and subsubtheme to the layers metadata is done by iterating
    through the layer collection data and adding the subtheme and subsubtheme to the layer metadata. The layer catalog is then
    serialized to a json file.  The layer catalog can be reduced by layers to process or thematic lot to process by filtering the
    layers in the layer catalog by the layers to process or thematic lot to process specified in the pipeline config file.

    """
    def __init__(self, pipeline_config : dict):
        """
        Initializes a LayerCatalog instance using the provided pipeline config dictionary. Loads the previous layer catalog if specified
        in the pipeline config. Creates the layer catalog by fetching the layer collection data from the Metagis database and creating
        a Layer instance for each layer in the collection. The layer catalog is then serialized to a json file.
        """
        self.layer_catalog_dict = {}
        self.layer_count = 0
        self.pipeline_config = pipeline_config
        
    def create_layer_catalog_load_select(self, pipeline_config: dict):
        """
        Create a full layer catalog by fetching the layer collection data from the Metagis database and creating a Layer instance for
        each layer in the collection. Add the subtheme and subsubtheme to the layer metadata by iterating through the layer collection
        data. Serialize the layer catalog to a json file. Select layers if 'select' used in pipeline config.

        :param pipeline_config: The pipeline config dictionary.
        :type pipeline_config: dict

        :return: The LayerCatalog instance with the full layer catalog created.
        :rtype: LayerCatalog
        """
        layer_collection_data = self.get_layer_collection_data(pipeline_config)
        self.find_layer_catalog_themes(layer_collection_data)
        self.load_previous_layer_catalog(pipeline_config)
        self.select_layers(pipeline_config)
        logger.info(f"Layer catalog created with {self.layer_count} layers.")
        return self
    
    def regenerate_layer_catalog(self, layer_catalog_file: str):
        """
        Load a previously saved layer catalog (not layer collection config) and create a new layer catalog by loading the json file.

        :param pipeline_config: The pipeline config dictionary.
        :type pipeline_config: dict
        :param layer_catalog_file: The layer catalog file path.
        :type layer_catalog_file: str

        :return: The LayerCatalog instance with the layer catalog loaded.
        :rtype: LayerCatalog
        """

        with open(f"{self.pipeline_config['datadir']}/{layer_catalog_file}") as file:
            layer_catalog_data = json.load(file)
            logger.info(f"Layer catalog loaded from: {layer_catalog_file}")
        
        if isinstance(layer_catalog_data, list):
            logger.info("Creating layer catalog from list")
            for layer in layer_catalog_data:
                self.layer_catalog_dict[layer['id']] = layer
                self.layer_count += 1
        
        elif isinstance(layer_catalog_data, dict):
            logger.info("Creating layer catalog from dict")
            for id, layer in layer_catalog_data.items():
                self.layer_catalog_dict[id] = layer
                self.layer_count += 1

        logger.info(f"Layer catalog created with {self.layer_count} layers.")
        return self

    def layer_coll_config_to_dict(self, pipeline_config: dict, layer_collection_config: str):
        """
        Fetch the layer collection data from the layer collection config URL, or load from a local file (ex oustide the VPN on EDITO). 
        Returns layer collection config data for further processing to layer catalog.

        :param layer_collection_config: The layer collection URL or file path.
        :type layer_collection_config: str

        :return: The layer collection data.
        :rtype: dict
        """
        if layer_collection_config.endswith('.json') and 'dev' in layer_collection_config:
            logger.info(f"Avoiding Dev endpoint, Reading layer catalog from local file: {layer_collection_config}")
            with open(f"{self.pipeline_config['datadir']}/{layer_collection_config}") as file:
                layer_collection_data = json.load(file)
                logger.info(f"Layer catalog loaded from: {layer_collection_config}")
        
        elif 'dev.' in layer_collection_config:
            logger.info(f"Fetching layer catalog from: {layer_collection_config}")
            layer_collection_data = CoreUtils.fetch_json_data(layer_collection_config)
            logger.info(f"Layer catalog fetched from: {layer_collection_config}")
            with open(f"{self.pipeline_config['datadir']}/layer_collection_config/dev_{pipeline_config['layer_collection']}_{today}.json", 'w') as file:
                json.dump(layer_collection_data, file, indent=4)
                logger.info(f"Layer catalog saved to: {layer_collection_config}")
        else:
            logger.info(f"Fetching layer catalog from: {layer_collection_config}")
            layer_collection_data = CoreUtils.fetch_json_data(layer_collection_config)

        return layer_collection_data

    def add_layer_to_catalog(self, layer: dict, thematic_lot_name: str, subtheme_name="", subsubtheme_name=""):
        """
        Add a layer to the layer catalog by creating a Layer instance for the layer and adding the layer to the layer catalog dictionary.
        Provide the subtheme and subsubtheme names to the layer metadata that is obtained from the layer collection data. And the recursive
        method to find all themes in the layer collection data. If the layer collection URL is the central portal, ignore layers without
        subtheme or subsubtheme. Otherwise log an error if the layer does not have a subtheme or subsubtheme, since this is critical for
        the EDITO STAC variable family and collection.

        :param layer_collection_url: The layer collection URL.
        :type layer_collection_url: str
        :param layer: The layer data.
        :type layer: dict
        :param thematic_lot_name: The thematic lot name of the layer.
        :type thematic_lot_name: str
        :param subtheme_name: The subtheme name of the layer.
        :type subtheme_name: str

        :return: None
        :rtype: None
        """
        layer_collection_config = self.pipeline_config['layer_collection_config']
        if 'id' in layer:
            layer_id = layer['id']
            layer['thematic_lot'] = thematic_lot_name
            layer['subtheme'] = subtheme_name
            layer['subsubtheme'] = subsubtheme_name
            # ignore layers without subtheme or subsubtheme for central portal
            if layer_collection_config == "https://emodnet.ec.europa.eu/geoviewer/config.php":
                if subtheme_name == "":
                    logger.warning(f"Layer {layer_id} does not have a subtheme")
                if subsubtheme_name == "":
                    logger.warning(f"Layer {layer_id} does not have a subsubtheme")
            else:
                if subtheme_name == "":
                    logger.error(f"Layer {layer_id} does not have a subtheme, no variable family")
                if subsubtheme_name == "":
                    logger.error(f"Layer {layer_id} does not have a subsubtheme, no edito collection")
            self.layer_catalog_dict[layer_id] = layer
            self.layer_count += 1
            logger.info(f"Layer {layer_id} added to layer catalog.")
        else:
            logger.error(f"Layer does not have an ID: {layer}")
        return None


    def find_all_themes(self, children: List, thematic_lot_name: str, depth=1, subtheme_name="", subsubtheme_name=""):
        """
        Finds all themes in the layer collection data by iterating through the children of the layer collection data. If the child has
        children, recursively call the find_all_themes method, adding subtheme or subsubtheme based on level. 
        If the child does not have children, then it is at the layer level and it is added to the layer catalog 
        using the add_layer_to_catalog method with the subtheme and subsubtheme names.

        :param children: The children of the layer collection data.
        :type children: List
        :param thematic_lot_name: The thematic lot name of the layer.
        :type thematic_lot_name: str
        :param layer_collection_config: The layer collection URL or file path.
        :type layer_collection_config: str
        :param depth: The depth of the layer in the layer collection data.
        :type depth: int
        :param subtheme_name: The subtheme name of the layer.
        :type subtheme_name: str
        :param subsubtheme_name: The subsubtheme name of the layer.
        :type subsubtheme_name: str

        :return: None
        :rtype: None
        """
        for child in children:
            if depth == 1:
                current_subtheme_name = child.get('displayName', '')
                current_subsubtheme_name = ""
            elif depth == 2:
                current_subtheme_name = subtheme_name
                current_subsubtheme_name = child.get('displayName', '')
            elif depth == 3:
                current_subtheme_name = subtheme_name
                current_subsubtheme_name = subsubtheme_name
            if 'children' in child:
                self.find_all_themes(child['children'], thematic_lot_name, depth + 1, current_subtheme_name, current_subsubtheme_name)
            else:
                self.add_layer_to_catalog(child, thematic_lot_name, current_subtheme_name, current_subsubtheme_name)


    def find_layer_catalog_themes(self, data: dict):
        """
        Find all themes in the layer collection data by iterating through the layer collection data. If the thematic lot has children,
        recursively call the find_all_themes method. If the thematic lot does not have children, add the layer to the layer catalog using
        the add_layer_to_catalog method.

        :param data: The layer collection data.
        :type data: dict
        :param layer_collection_url: The layer collection URL.
        :type layer_collection_url:

        :return: None
        :rtype: None
        """
        data = data['layerCatalog']
        for thematic_lot in data.get('children', []):
            thematic_lot_name = thematic_lot.get('name', '')
            if 'children' in thematic_lot:
                self.find_all_themes(thematic_lot['children'], thematic_lot_name)


    def get_layer(self, layer_id):
        """Retrieve a layer by its ID.
        
        :param layer_id: The ID of the layer to retrieve.
        :type layer_id: int

        :return: The layer with the specified ID.
        :rtype: Layer
        """
        for id, layer in self.layer_catalog_dict.items():
            if id == layer_id:
                return layer
        raise ValueError(f"Layer with ID {layer_id} not found")


    def update_layer_catalog_dict(self, layer_id, updated_layer):
        """Update the layer catalog dictionary with the updated layer.
        
        :param layer_id: The ID of the layer to update.
        :type layer_id: int
        :param updated_layer: The updated layer.
        :type updated_layer: dict

        :return: None
        :rtype: None
        """
        self.layer_catalog_dict[layer_id] = updated_layer


    def serialize_layer_catalog_to_json(self, layer_catalog_outfile: str):
        """
        Serialize the layer_catalog_dict to a json file.

        :param layer_catalog_outfile: The output file path for the serialized layer catalog.
        :type layer_catalog_outfile: str

        :return: None
        :rtype: None
        """

        with open(layer_catalog_outfile, 'w') as file:
            json.dump(self.layer_catalog_dict, file, indent=4)


    def remove_layer(self, layer_id: int):
        """
        Remove a layer from the layer catalog by its ID. Update the layer catalog dictionary, 
        layer objects, and layer count.

        :param layer_id: The ID of the layer to remove.
        :type layer_id: int

        :return: None
        :rtype: None
        """
        new_layer_catalog_dict = {}
        for id, layer in self.layer_catalog_dict.items():
            if id != layer_id:
                new_layer_catalog_dict[id] = layer
        self.layer_catalog_dict = new_layer_catalog_dict
        self.layer_count = len(self.layer_catalog_dict)
        logger.info(f"Layer {layer_id} removed from layer catalog.")


    def get_layer_collection_data(self, pipeline_config: dict):
        """
        Fetch the layer collection the layer collection config URL, or from a local file. Load the layer catalog data as json.
        If 'dev.' is in the layer collection URL, fetch the layer collection data from the dev endpoint and save to a json file.
        Can be used outside the VPN (ex on EDITO). If a json file (saved from a dev endpoint) is specified, 
        read the layer catalog data from the file.

        :param pipeline_config: The pipeline config dictionary.
        :type pipeline_config: dict

        :return: The layer collection data.
        :rtype: dict
        """
        layer_collection_config = pipeline_config['layer_collection_config']

        # Check if layer_collection_config is local .json, to run pipeline off of the VLIZ VPN (ex on EDITO, or for faster networking speed)
        if layer_collection_config.endswith('.json'):
            logger.info(f"Avoiding Dev endpoint, Reading layer catalog from local file: {layer_collection_config}")
            with open(f"{self.pipeline_config['datadir']}/{layer_collection_config}") as file:
                layer_catalog_data = json.load(file)

        # Check if layer collection URL is a dev endpoint, save directly to json. 
        # Can be read outside VPN layer_collection_config (ex on EDITO, or for faster networking speed)
        # Need to be on VLIZ VPN to access dev endpoint
        elif 'dev.' in layer_collection_config:
            logger.info(f"Fetching layer catalog from: {layer_collection_config}")
            layer_catalog_data = CoreUtils.fetch_json_data(layer_collection_config)
            with open(f"{self.pipeline_config['datadir']}/layer_collection_config/dev_{pipeline_config['layer_collection']}_{today}.json", 'w') as file:
                json.dump(layer_catalog_data, file, indent=4)

        # for standard case of fetching a layer catalog from a URL
        else:
            logger.info(f"Fetching layer catalog from: {layer_collection_config}")
            layer_catalog_data = CoreUtils.fetch_json_data(layer_collection_config)
        
        return layer_catalog_data


    def load_previous_layer_catalog(self, pipeline_config):
        """
        Load a 'previous_layer_catalog' json file if specified in the pipeline config. 
        Add the previous layer catalog to the LayerCatalog instance.
        If a list of layer ids are in 'layers_to_update' in the config, 
        delete those layers from the previous layer catalog. 

        :param pipeline_config: The pipeline config dictionary.
        :type pipeline_config: dict

        :return: The LayerCatalog instance with the previous layer catalog loaded.
        :rtype: LayerCatalog
        """
        # Load previous layer catalog if specified in pipeline config
        if not 'previous_layer_catalog' in pipeline_config:
            logger.info("No previous layer catalog specified, skipping")
            return self
        layer_catalog_file = f"{pipeline_config['datadir']}/{pipeline_config['previous_layer_catalog']}"

        if not os.path.exists(layer_catalog_file):
            logger.error(f"Previous layer catalog file not found: {layer_catalog_file}")
            return self
        try:
            with open(layer_catalog_file) as file:
                self.previous_layer_catalog = json.load(file)
        except Exception as e:
            logger.error(f"Failed to load previous layer catalog {layer_catalog_file}: {e}")
            return self
        
        # Add layer_catalog and layer_count attributes
        if self.previous_layer_catalog is not None:
            if isinstance(self.previous_layer_catalog, list):
                self.previous_layer_catalog = {layer['id']: layer for layer in self.previous_layer_catalog}
                for id, layer in self.layer_catalog_dict.items():
                    if id in self.previous_layer_catalog:
                        layer['previous_layer'] = self.previous_layer_catalog[id]

            elif isinstance(self.previous_layer_catalog, dict):
                self.previous_layer_catalog = {id: layer for id, layer in self.previous_layer_catalog.items()}
                for id, layer in self.layer_catalog_dict.items():
                    if id in self.previous_layer_catalog:
                        layer['previous_layer'] = self.previous_layer_catalog[id]
                        
            self.previous_layer_count = len(self.previous_layer_catalog)
            logger.info(f"Loaded previous layer catalog with {self.previous_layer_count} layers.")
        
        # if layers to update are specified in the pipeline config, delete from previous layer catalog
        if 'layers_to_update' in pipeline_config:
            new_previous_layer_catalog = {}
            for id, layer in self.previous_layer_catalog.items():
                if id not in pipeline_config['layers_to_update']:
                    new_previous_layer_catalog[id] = layer
            self.previous_layer_catalog = new_previous_layer_catalog
        return self
    

    def select_layers(self, pipeline_config: dict):
        """
        General method to reduce the layer catalog by any trait and a list of valid values for that trait.
        
        :param trait: The trait to filter layers by.
        :type trait: str
        :param valid_values: The list of valid values for the trait.
        :type valid_values: List[Any]

        :return: The LayerCatalog instance with the layers reduced by the specified trait and valid values.
        :rtype: LayerCatalog
        """
            
        if 'select' not in pipeline_config:
            logger.info("No selection criteria")
            return self
        traitslist = pipeline_config['select']
        for traitdict in traitslist:
            for trait, valid_values in traitdict.items():
                self.layer_catalog_dict = {id: layer for id, layer in self.layer_catalog_dict.items() if layer.get(trait) in valid_values}
                self.layer_count = len(self.layer_catalog_dict)
                logger.info(f"Layer catalog reduced by {trait} with {self.layer_count} layers remaining.")
        return self
        
