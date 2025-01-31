import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../src')))
import logging
import json
from metagisconfig2stac.layer import Layer
from metagisconfig2stac.layer_catalog import LayerCatalog
from metagisconfig2stac.pipeline import Pipeline

# Simulate making just a single layer from metadata, adding to a layer catalog, 
# and using this layer catalog in the pipeline.  
# Does use valid download url, edito info, and thematic lot, subtheme, and subsubtheme

logging.basicConfig(level=logging.INFO)

# Define a test layer
test_layer_data = {
    
    'id': "190000",
    'name': 'My Test Layer',
    'metadataSources': [
        {
            'metadata_type': 'download_url',
            'metadata_value': 'https://ows.emodnet-humanactivities.eu/geonetwork/srv/api/records/8201070b-4b0b-4d54-8910-abcea5dce57f/attachments/EMODnet_HA_Energy_WindFarms_20240508.zip'
        },
        {
            'metadata_type': 'geonetwork_uri',
            'metadata_value': '8201070b-4b0b-4d54-8910-abcea5dce57f'
        },
        {
            'metadata_type': 'edito_info',
            'metadata_value': 'EMODnet_HA_Energy_WindFarms_pg_20240508.shp'
        }
    ],
    "attributes": {
            "POWER_MW": {
                "id": 22212,
                "gislayer_fk": 10178,
                "name": "POWER_MW",
                "name_custom": "wind_farm_power_mw",
                "description": "Power in MW",
                "show": 0,
                "geoviewer_is_filter_param": 0,
                "geoviewer_filter_type": "choice",
                "geoviewer_filter_choice_values_by_query": 0,
                "geoviewer_filter_default_value": "null",
                "missing_on_geoserver": "null",
                "lifewatch_dataformat_column_fk": "null",
                "specifications": "null",
                "hyperlink": "null",
                "lifewatch": 0,
                "geoviewer_filter_choice_values_by_query_db": "null",
                "geoviewer_filter_choice_values_by_query_table": "null",
                "geoviewer_filter_choice_values_by_query_field": "null",
                "geoviewer_get_feature_info_show_as_title": 0,
                "geoviewer_get_feature_info_show_value_as_link": 0,
                "geoviewer_filter_operator": "=",
                "attribute_order_nr": 10,
                "ogc_attribute": 0,
                "export_stac": 1
            },
    },
    'bbox': [{'west': -180, 'south': -90, 'east': 180, 'north': 90}],
    'responsibleParties': [{'name': 'test_provider', 'role': 'provider'}],
    'thematic_lot': 'test_provider',
    'temporal_extent': {'start': '2021-01-01', 'end': '2021-12-31'},
    'temporalDatesExtent': '2021-01-01/2021-12-31',
    'data_rights': 'test_data_rights',
    'license': 'test_license',
    'subtheme': 'Human Marine Activities', # use an existing subtheme (variable family)
    'subsubtheme': 'Energy', # use an existing subsubtheme(collection)
    
}

layer_catalog = {test_layer_data['id']: test_layer_data}

# add layer to layer_catalog and serialize to json in the data directory
datadir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'data'))


with open(f"{datadir}/layer_catalogs/single_layer_catalog.json", 'w') as f:
    json.dump(layer_catalog, f, indent=4)

# put the layer_catalog in a pipeline config

pipeline_config = {
    'layer_collection': 'test_collection',
    'layer_collection_config': 'single_layer_catalog.json',
    'stac_title': 'test_title',
    'stac_s3': 'test_single_stac',
    'create_backup_stac': 'False',
}

# create a pipeline with the pipeline config, 
pipeline = Pipeline(pipeline_config)

# create a layer catalog by "regenerating" a layer catalog, created from the single layer layer catalog json
layer_catalog = LayerCatalog(pipeline.pipeline_config)
layer_catalog.regenerate_layer_catalog(f"layer_catalogs/single_layer_layer_catalog.json")
# create the test_layer
test_layer = Layer(pipeline.pipeline_config, layer_catalog.layer_catalog_dict[test_layer_data['id']])


def test_update_basic_metadata(layer: ('Layer')):
    layer.update_basic_metadata()
    assert layer.metadata['id'] == test_layer_data['id'], "Layer ID should match"
    assert layer.metadata['download_url'] == test_layer_data['metadataSources'][0]['metadata_value']
    assert layer.metadata['geonetwork_uri'] == test_layer_data['metadataSources'][1]['metadata_value']
    assert layer.metadata['native_data_product'] == test_layer_data['metadataSources'][2]['metadata_value']
    print("Metadata after update_basic_metadata:", test_layer.metadata)
# Test add_metadataSources method

def test_update_geographic_extent(layer: ('Layer')):
    layer.update_geographic_extent()
    assert layer.metadata['geographic_extent']
    print("Metadata after update_geographic_extent:", test_layer.metadata)

def test_update_temporal_extent(layer: ('Layer')):
    layer.update_temporal_extent()
    assert layer.metadata['temporal_extent']
    print("Metadata after update_temporal_extent:", test_layer.metadata)


def test_transfer_native_asset_to_s3(layer: ('Layer')):
    layer.transfer_native_asset_to_s3(layer_catalog.layer_catalog_dict)
    # check that the native asset was downloaded and transferred to S3
    assert layer.metadata['native_asset']

    # check that the native asset was downloaded and is in the temp_assets directory
    native_asset = os.path.basename(layer.metadata['native_asset'])
    downloaded_assets = os.listdir(f"{pipeline.pipeline_config['datadir']}/temp_assets")
    assert native_asset in downloaded_assets
    print("Metadata after transfer_native_to_s3:", test_layer.metadata)

def test_update_providers_data_rights(layer: ('Layer')):
    layer.update_providers_data_rights()
    assert layer.metadata['provider']
    print("Metadata after update_providers_data_rights:", test_layer.metadata)

def test_arco_conversion(layer: ('Layer')):
    layer.convert_native_to_arco()
    assert layer.metadata['converted_arco_assets']

    print("Metadata after arco_conversion:", test_layer.metadata)

def test_add_to_stac(layer: ('Layer')):
    layer.add_to_stac()
    assert layer.metadata['in_stac']

    print("Metadata after add_to_stac:", test_layer.metadata)

if __name__ == "__main__":
    test_update_basic_metadata(test_layer)
    test_update_geographic_extent(test_layer)
    test_update_temporal_extent(test_layer)
    test_transfer_native_asset_to_s3(test_layer)
    test_update_providers_data_rights(test_layer)
    test_arco_conversion(test_layer)
    test_add_to_stac(test_layer)