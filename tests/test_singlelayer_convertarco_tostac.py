
import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../src')))


import logging

from metagisconfig2stac.pipeline import Pipeline
from metagisconfig2stac.layer_catalog import LayerCatalog
from metagisconfig2stac.layer import Layer
from metagisconfig2stac.utils.core import CoreUtils
from metagisconfig2stac.utils.s3 import S3Utils

data_dir = os.path.join(os.path.dirname(__file__), '../data')
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', 
                    filename=f'{data_dir}/testarcosinglelayer.log')

pipeline_config = {
    "layer_collection": "edito",
    "layer_collection_config": "https://emodnet.ec.europa.eu/geoviewer/edito.php",
    "stac_title" : "EMODnet",
    "stac_s3" : "teststac/arco1layer/",
    "create_backup_stac": False,
    "stac_s3_backup" : "backup_stac/backup_edito_stac",
    "select": [
        {"id": [13151]}
    ],
    "transfer_native_to_s3": True,
    "convert_arco" : True,
    "cp_transformation_table" : "True",
    "central_portal_php_config" : "https://emodnet.ec.europa.eu/geoviewer/config.php",
    "attributes_table" : "True",
    "attribute_s3": "attributes_tables/attributes_table",
    "push_to_resto" : "True",
    "resto_instance" : "staging"
}
def test_layers_to_process(pipeline_config):

    layers_to_process = pipeline_config['select'][0]['id']
    pipeline = Pipeline(pipeline_config)
    pipeline.create_layer_catalog()
    layer_catalog = pipeline.layer_catalog
    
    for id, layer in layer_catalog.layer_catalog_dict.items():
        assert layer['id'] in layers_to_process, "Layer ID should be in layers to process"
    
    return layer_catalog

def test_update_single_layer_metadata(layer: ('Layer')):
    layer.update_basic_metadata()
    assert layer.metadata['id'] == layer.metadata['id'], "Layer ID should match"
    assert layer.metadata['download_url']
    print("download_url", layer.metadata['download_url'])
    assert layer.metadata['geonetwork_uri']
    print("geonetwork_uri", layer.metadata['geonetwork_uri'])

def test_update_temporal_extent(layer: ('Layer')):
    layer.update_temporal_extent()
    assert layer.metadata['temporal_extent']
    print("temporal_extent", layer.metadata['temporal_extent'])

def test_update_spatial_extent(layer: ('Layer')):
    layer.update_geographic_extent()
    assert layer.metadata['geographic_extent']
    print("geographic_extent", layer.metadata['geographic_extent'])

def test_update_provider(layer: ('Layer')):
    layer.update_providers_data_rights()
    assert layer.metadata['provider']
    print("provider", layer.metadata['provider'])

def test_transfer_native_asset(layer: ('Layer'), layer_catalog_dict: ('dict')):
    layer.transfer_native_asset_to_s3(layer_catalog_dict)
    assert layer.metadata['native_asset'], "native asset should exist"
    # see if the native asset is downloadable or accessible on s3
    import requests
    response = requests.head(layer.metadata['native_asset'], stream=True, allow_redirects=True)
    if response.status_code == 200:
        print("native asset is downloadable")
    elif S3Utils.is_subsettable_arco_asset(layer.metadata['native_asset']):
        print("native asset is not downloadable but should be on s3")

def test_convert_arco(layer: ('Layer')):
    if 'attributes' in layer.metadata:
        layer.convert_native_to_arco()
        assert layer.metadata['converted_arco_assets'], "converted arco assets should exist"
    else:
        print("no attributes to use for arco conversion")

if __name__ == "__main__":

    layer_catalog = test_layers_to_process(pipeline_config)
    for id, layer in layer_catalog.layer_catalog_dict.items():
        print(f"processing {layer['id']}")
        layer = Layer(pipeline_config, layer)
        test_update_single_layer_metadata(layer)
        test_update_temporal_extent(layer)
        test_update_spatial_extent(layer)
        test_update_provider(layer)
        #layer.metadata['native_asset'] = "https://s3.waw3-1.cloudferro.com/emodnet/emodnet_native/emodnet_geology/seabed_substrate/all_resolutions/EMODnet_GEO_Seabed_Substrate_All_Res.zip"
        #layer.metadata['native_asset'] = "https://s3.waw3-1.cloudferro.com/emodnet/emodnet_seabed_habitats/12549/EUSeaMap_2023.zip"
        layer.metadata['native_asset'] = "https://s3.waw3-1.cloudferro.com/emodnet/emodnet_seabed_habitats/13151/EUSeaMap_2023_CaspianSea.zip"
        #test_transfer_native_asset(layer, layer_catalog.layer_catalog_dict)
        test_convert_arco(layer)
        layer.add_to_stac()

