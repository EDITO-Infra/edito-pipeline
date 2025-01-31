import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../src')))

import logging
from typing import TYPE_CHECKING, List, Tuple
from datetime import datetime, date
import json
import pystac
import pandas as pd
# test the creation of a layer catalog using a given pipeline config

from metagisconfig2stac.resto import STACRestoManager
from metagisconfig2stac.layer_catalog import LayerCatalog
from metagisconfig2stac.pipeline_results import PipelineResults

data_dir = os.path.join(os.path.dirname(__file__), '../data')
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', filename=f'{data_dir}/push_to_resto.log')

targetstac = 'stac/catalog.json'
pipeline_config = {
                "layer_collection": "edito",
                "layer_collection_config": "https://emodnet.ec.europa.eu/geoviewer/edito.php",
                "previous_layer_catalog": "layer_catalogs/progress_edito_2024-10-27.json",
                "transfer_native_to_s3": False,
                "convert_arco": False,
                "create_backup_stac": False,
                "cp_transformation_table": True,
                "attributes_table": True,
                "push_to_resto": False,
                "resto_instance": "staging",
            }
pipeline_config['datadir'] = data_dir
stac_loc = os.path.join(data_dir, targetstac)
local_stac = pystac.Catalog.from_file(stac_loc)

resto_instance = pipeline_config['resto_instance']
resto = STACRestoManager(pipeline_config, local_stac=local_stac)

# make first layer catalog
layer_catalog = LayerCatalog(pipeline_config)
layer_catalog = layer_catalog.create_layer_catalog_load_select(pipeline_config)

# compare with central portal 

pipelineresults = PipelineResults(pipeline_config, layer_catalog, local_stac)

# compare with central portal
pipelineresults.make_cp_transformation_table()