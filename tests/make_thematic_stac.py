
import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../src')))

import logging

from datetime import datetime


from metagisconfig2stac.layer_catalog import LayerCatalog
from metagisconfig2stac.layer import Layer
from metagisconfig2stac.pipeline_results import PipelineResults
from metagisconfig2stac.utils.core import CoreUtils

# test the creation of a layer catalog using a given pipeline config


datadir = os.path.abspath(os.path.join(os.path.dirname(__file__), '../data'))
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                    filename=f'{datadir}/logs/thematicstac.log',)


pipeline_config = {
    "layer_collection": "dev_edito",
    "layer_collection_config": "http://dev.emodnet.eu/geoviewer/edito.php",
    "previous_layer_catalog" : "layer_catalogs/progress_thematicstac_dev_edito_EMODnet Geology_2024-12-18.json",
    "thematic_lots_to_process" : ["EMODnet Geology"],
    "stac_title" : "EMODnet Geology Test",
    "stac_s3" : "teststac_geology_dec2024",
    "create_backup_stac" : "False",
    "stac_s3_backup" : "backup_stac/backup_edito_stac_test",
    "transfer_native_to_s3": "True",
    "convert_arco" : "True",
    "cp_transformation_table" : "True",
}

output_dir = f"{datadir}/layer_catalogs"
pipeline_config['datadir'] = datadir

def create_thematic_layer_catalog(pipeline_config):
    layer_catalog = LayerCatalog(pipeline_config)
    thematic_lot = pipeline_config['thematic_lots_to_process'][0]
    layer_catalog_dict = layer_catalog.layer_coll_config_to_dict(pipeline_config, pipeline_config['layer_collection_config'])
    layer_catalog.find_layer_catalog_themes(layer_catalog_dict)
    layer_catalog.reduce_layer_catalog('thematic_lot', pipeline_config['thematic_lots_to_process'])
    layer_catalog.load_previous_layer_catalog(pipeline_config)
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    from metagisconfig2stac.utils.core import CoreUtils
    layer_catalog.serialize_layer_catalog_to_json(f"{output_dir}/test_current_{pipeline_config['layer_collection']}_{CoreUtils.custom_slugify(thematic_lot)}_{datetime.today().strftime('%Y-%m-%d')}.json")
    assert len(layer_catalog.layer_catalog_dict) > 0, "Layer catalog should have layers"
    assert layer_catalog.layer_count == len(layer_catalog.layer_catalog_dict), "Layer count should match the number of layers"
    for id, layermetadata in layer_catalog.layer_catalog_dict.items():
        assert layermetadata['thematic_lot'] == thematic_lot, f"Thematic lot should be {thematic_lot}"
        return layer_catalog


def test_pipeline(pipeline_config):
    logger = logging.getLogger(__name__)
    logger.info('\nSTARTING TEST')
    layer_catalog = create_thematic_layer_catalog(pipeline_config)
    for id, metadata in layer_catalog.layer_catalog_dict.items():
        layer = Layer(pipeline_config, metadata)
        print(layer.metadata['id'])
        layer.update_basic_metadata()
        layer.update_temporal_extent()
        layer.update_providers_data_rights()
        layer.update_geographic_extent()
        layer.transfer_native_asset_to_s3(layer_catalog.layer_catalog_dict)
        layer.convert_native_to_arco()
        layer.add_to_stac()
        layer_catalog.update_layer_catalog_dict(id, layer.metadata)
        layer_catalog.serialize_layer_catalog_to_json(f"{output_dir}/progress_thematicstac_{pipeline_config['layer_collection']}_{layer.metadata['thematic_lot']}_{datetime.today().strftime('%Y-%m-%d')}.json")
    
    for id, layermetadata in layer_catalog.layer_catalog_dict.items():
        print(layermetadata['id'], layermetadata['in_stac'])
        if layermetadata['in_stac'] == 'no':
            print(f"Layer {layermetadata['id']} failed to transfer native assets to s3")
    pipelineresults = PipelineResults(pipeline_config, layer_catalog)
    # pipelineresults.make_cp_transformation_table()
    pipelineresults.logs_to_local_stac()
    pipelineresults.push_local_stac_to_s3()

if __name__ == '__main__':
    test_pipeline(pipeline_config)