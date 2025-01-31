
import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../src')))

import logging
import requests
from metagisconfig2stac.layer import Layer
from metagisconfig2stac.layer_catalog import LayerCatalog
from metagisconfig2stac.pipeline import Pipeline
logging.basicConfig(level=logging.INFO)


pipeline_config = {
    "layer_collection": "edito",
    "layer_collection_config": "https://emodnet.ec.europa.eu/geoviewer/edito.php",
    "previous_layer_catalog" : "data/previous_state_files/previous_edito_state_2024-09-04.json",
    "layers_to_process": [
        13662
    ],
    "transfer_native_to_s3": "True",
    "convert_arco" : "True",
    "stac_title" : "EMODnet",
    "stac_s3" : "test_stac_26112024",
    "cp_transformation_table" : "True",
    "central_portal_php_config" : "https://emodnet.ec.europa.eu/geoviewer/config.php",
    "push_to_resto" : "False",
    "resto_instance" : "staging"
}

def test_layers_to_process(pipeline_config):

    layers_to_process = pipeline_config['layers_to_process']
    pipeline = Pipeline(pipeline_config)
    pipeline.create_layer_catalog()
    layer_catalog = pipeline.layer_catalog
    
    for id, layer in layer_catalog.layer_catalog_dict.items():
        assert layer['id'] in layers_to_process, "Layer ID should be in layers to process"
    
    return layer_catalog
    

def test_transfer_native_assets_to_s3(layer: Layer, layer_catalog_dict: dict) -> None:
    """
    Test transfer native asset to S3
    :param layer: Layer object
    :return: None
    """
    try:
        layer.update_basic_metadata()
        layer.transfer_native_asset_to_s3(layer_catalog.layer_catalog_dict)
        assert layer.metadata['native_asset'], "native asset should exist"
        # see if the native asset is downloadable
        response = requests.head(layer.metadata['native_asset'], stream=True, allow_redirects=True)
        assert response.status_code == 200, "Native asset should be downloadable"
        # check basename download url and native asset
        native_asset = os.path.basename(layer.metadata['native_asset'])
        download_product = os.path.basename(layer.metadata['download_url'])
        assert native_asset == download_product, "Native asset should match download product"
    except Exception as e:
        print(e)
        assert False, "Failed to transfer native assets to S3"

if __name__ == "__main__":
    layer_catalog = test_layers_to_process(pipeline_config)
    for id, layerdata in layer_catalog.layer_catalog_dict.items():
        print(layerdata['id'])
        layer = Layer(pipeline_config, layerdata)
        test_transfer_native_assets_to_s3(layer, layer_catalog.layer_catalog_dict)
    print("Tests passed")