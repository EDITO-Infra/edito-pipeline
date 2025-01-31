import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../src')))

import logging

# test the creation of a layer catalog using a given pipeline config
datadir = os.path.abspath(os.path.join(os.path.dirname(__file__), '../data'))
logger = logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                    filename=f'{datadir}/logs/testlayercatalog.log',)

single_layer_config = {
    "layer_collection": "edito",
    "layer_collection_config": "https://emodnet.ec.europa.eu/geoviewer/edito.php",
    "previous_layer_catalog" : "layer_catalogs/progress_edito_2024-10-27.json",
    "stac_title" : "EMODnet_test",
    "stac_s3" : "stac_test_4dec2024",
    "create_backup_stac": "False",
    "stac_s3_backup" : "backup_stac/backup_test_stac",
    "select": [
        {'id': [13662]}
        ],
    "transfer_native_to_s3": "True",
    "convert_arco" : "True",
    "datadir" : datadir
}

thematic_lot_config = {
    "layer_collection": "edito",
    "layer_collection_config": "https://emodnet.ec.europa.eu/geoviewer/edito.php",
    "previous_layer_catalog" : "layer_catalogs/progress_edito_2024-10-27.json",
    "stac_title" : "EMODnet_test",
    "stac_s3" : "stac_test_4dec2024",
    "create_backup_stac": "False",
    "stac_s3_backup" : "backup_stac/backup_test_stac",
    "select": [
        {'thematic_lot': ['EMODnet Seabed Habitats', 'EMODnet Chemistry']}
        ],
    "transfer_native_to_s3": "True",
    "convert_arco" : "True",
    "datadir" : datadir
}

from datetime import datetime, date
from metagisconfig2stac.layer_catalog import LayerCatalog
from metagisconfig2stac.pipeline import Pipeline

output_dir = "unshared_data/layer_catalog_states/"

# test the creation of a layer catalog using the pipeline config that selects a single layer
def test_reduce_layer_catalog_by_layers_to_process(pipeline_config):
    layers_to_process = pipeline_config['select'][0]['id']
    reduced_layer_catalog = LayerCatalog(pipeline_config)
    reduced_layer_catalog.create_layer_catalog_load_select(pipeline_config)

    assert len(reduced_layer_catalog.layer_catalog_dict) == len(layers_to_process), f"layer count should match {len(layers_to_process)}"
    assert reduced_layer_catalog.layer_count == len(layers_to_process), f"Layer count should match {len(layers_to_process)}"
    for id, layermetadata in reduced_layer_catalog.layer_catalog_dict.items():
        assert layermetadata['id'] in layers_to_process, "layer_id not in layers_to_process"
        reduced_layer_catalog.serialize_layer_catalog_to_json(f"{datadir}current_{pipeline_config['layer_collection']}_state_{datetime.today().strftime('%Y-%m-%d')}_{layermetadata['id']}.json")
    logger.info(f"Reduced layer catalog has {len(reduced_layer_catalog.layer_catalog_dict)} layers")
    logger.info(f"test passed")
    return reduced_layer_catalog

# test the creation of a layer catalog using the pipeline config that selects layers by thematic lot
def test_reduce_layer_catalog_by_thematic_lot(pipeline_config):
    
    thematic_lots = pipeline_config['select'][0]['thematic_lot']
    thematic_lot_catalog = LayerCatalog(pipeline_config)
    thematic_lot_catalog.create_layer_catalog_load_select(pipeline_config)

    assert len(thematic_lot_catalog.layer_catalog_dict) > 0, "Single thematic lot catalog should have layers"
    for id, layermetadata in thematic_lot_catalog.layer_catalog_dict.items():
        assert layermetadata['thematic_lot'] in thematic_lots, f"Thematic lot should be in {thematic_lots}"
        thematic_lot_catalog.serialize_layer_catalog_to_json(f"{datadir}current_{pipeline_config['layer_collection']}_state_{datetime.today().strftime('%Y-%m-%d')}_testthematiclot.json")
    logger.info(f" thematic lot catalog has {len(thematic_lot_catalog.layer_catalog_dict)} layers from lots {thematic_lots}")
    logger.info(f"test passed")
    return thematic_lot_catalog

# test the loading of a previous layer catalog state
def test_load_previous_layer_catalog_state(pipeline_config):
    layer_catalog = LayerCatalog(pipeline_config)
    layer_catalog.create_layer_catalog_load_select(pipeline_config)

    assert layer_catalog.previous_layer_catalog is not None, "Previous layer catalog state should be loaded"
    logger.info(f"Previous layer catalog state loaded with {len(layer_catalog.previous_layer_catalog)} layers")
    logger.info(f"test passed")


if __name__ == '__main__':
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)
    logger.addHandler(logging.StreamHandler())
    logger.info("Running layer catalog tests")

    logger.info("testing single layer config")
    test_reduce_layer_catalog_by_layers_to_process(single_layer_config)

    logger.info("testing thematic lot config")
    test_reduce_layer_catalog_by_thematic_lot(thematic_lot_config)

    logger.info("testing loading previous layer catalog state")
    test_load_previous_layer_catalog_state(single_layer_config)
    
    logger.info("Layer catalog tests complete")
