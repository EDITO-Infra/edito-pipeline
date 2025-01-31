
import logging
import json
import os
from datetime import datetime, date
import pandas as pd
import csv
import shutil
import pystac
import xarray as xr
import yaml
from metagisconfig2stac.utils import CoreUtils
from metagisconfig2stac import LayerCatalog
from metagisconfig2stac import Layer
from metagisconfig2stac import STACBuilder
from metagisconfig2stac.pipeline_results import PipelineResults

logger = logging.getLogger(__name__)

today = datetime.today().strftime('%Y-%m-%d')
wkdir = os.path.dirname(os.path.realpath(__file__))
datadir = os.path.abspath(os.path.join(wkdir, '..', '..', 'data'))

class Pipeline:
    """
    Main class that directs the main steps of the pipeline. The pipeline is run by creating the LayerCatalog object,
    processing each of the layers into STAC items, and creating the PipelineResults object to finish the pipeline.

    See layer_catalog.py, layer.py, and pipeline_results.py for more information on the LayerCatalog, Layer, and
    PipelineResults classes.
    """
    def __init__(self, config: dict) -> None:
        """

        :param config_path: path to the pipeline config file
        :type config_path: str
        """
        self.pipeline_config = config
        self.layer_collection = self.pipeline_config['layer_collection']
        self.pipeline_config['datadir'] = datadir
        stacbuilder = STACBuilder(self.pipeline_config)
        self.local_stac = stacbuilder.local_stac

    def finish_previous_layer_catalog(self, layercatalogfile) -> None:
        """
        Rerun the pipeline on a layer catalog where the pipeline processing was interrupted.  
        This method is used to regenerate a serialized layer catalog file.  This can only be used
        if the local stac is reflected in the layer catalog file.
        """

        self.layer_catalog = LayerCatalog(self.pipeline_config)
        self.layer_catalog = self.layer_catalog.regenerate_layer_catalog(layer_catalog_file=layercatalogfile)
        self.layer_catalog.select_layers(self.pipeline_config)
        
        self.processed_layers = 0
        for id, layermetadata in self.layer_catalog.layer_catalog_dict.items():
            if 'in_stac' in layermetadata and layermetadata['in_stac'] == 'yes':
                logger.info(f"Layer {layermetadata['id']} already in STAC. Skipping.")
                continue
            
            layer = Layer(self.pipeline_config, layermetadata)
            layer.update_basic_metadata()

            layer.transfer_native_asset_to_s3(self.layer_catalog.layer_catalog_dict)
            layer.convert_native_to_arco()
            layer.update_providers_data_rights()
            layer.update_temporal_extent()
            layer.update_geographic_extent()
            layer.add_to_stac()
            self.processed_layers += 1
            CoreUtils.empty_directory(layer.temp_assets)
            self.layer_catalog.update_layer_catalog_dict(id, layer.metadata)
            outfile = (
                f"{datadir}/layer_catalogs/{self.layer_collection}_{today}_"
                f"progress_{self.layer_catalog.layer_count}.json"
            )
            self.layer_catalog.serialize_layer_catalog_to_json(outfile)
        
        logger.info(f'processed {self.processed_layers} layers')
        self.create_pipeline_results()
        return


    def create_layer_catalog(self) -> None:
        """
        Creates the LayerCatalog object using the pipeline config file.  The LayerCatalog object is created with the
        layers specified in the pipeline config file.  If no layers are specified, all layers are included.

        :return: None
        """
        self.layer_catalog = LayerCatalog(self.pipeline_config)
        layer_coll_data = self.layer_catalog.get_layer_collection_data(self.pipeline_config)
        self.layer_catalog.find_layer_catalog_themes(layer_coll_data)
        self.layer_catalog.load_previous_layer_catalog(self.pipeline_config)
        self.layer_catalog.select_layers(self.pipeline_config)
        outfile = f"{datadir}/layer_catalogs/{self.layer_collection}_{today}_start.json"
        self.layer_catalog.serialize_layer_catalog_to_json(outfile)
        
        logger.info(f'processing {len(self.layer_catalog.layer_catalog_dict)} layers in Pipeline')
        return
    
    def process_layers(self) -> None:
        """
        Processes each layer in the LayerCatalog object.  The pipeline processes each layer using the Layer methods
        to update metadata, transfer assets to S3, convert native assets to ARCO, and add the layer to the STAC.

        :return: None
        """
        self.processed_layers = self.success_count = 0
        self.failed_layers = []
        self.failed_count = 0
        self.success_layers = []
        self.success_count = 0

        for id, layermetadata in self.layer_catalog.layer_catalog_dict.items():
            print(layermetadata['id'])
            if 'in_stac' in layermetadata and layermetadata['in_stac'] == 'yes':
                logger.info(f"Layer {layermetadata['id']} already in STAC. Skipping.")
                continue
            layer = Layer(self.pipeline_config, layermetadata)
            layer.update_basic_metadata()

            layer.transfer_native_asset_to_s3(self.layer_catalog.layer_catalog_dict)
            layer.convert_native_to_arco()
            layer.update_providers_data_rights()
            layer.update_temporal_extent()
            layer.update_geographic_extent()
            layer.add_to_stac()
            self.processed_layers += 1
            CoreUtils.empty_directory(layer.temp_assets)
            self.layer_catalog.update_layer_catalog_dict(id, layer.metadata)
            outfile = (
                f"{datadir}/layer_catalogs/{self.layer_collection}_{today}_"
                f"progress_{self.layer_catalog.layer_count}.json"
            )
            self.layer_catalog.serialize_layer_catalog_to_json(outfile)
        return

    def create_pipeline_results(self) -> None:
        """
        Creates the PipelineResults object, to finish the last steps after processing all layers

        :return: None
        """
        self.pipeline_results = PipelineResults(self.pipeline_config, self.layer_catalog, self.local_stac)
        self.pipeline_results.finish_pipeline()
        return


    def run_full_pipeline(self) -> None:
        """
        Runs the pipeline.  The pipeline is run by creating the LayerCatalog object, processing the layers, and
        creating the pipeline results.

        :return: None
        """
        self.create_layer_catalog()
        self.process_layers()
        self.create_pipeline_results()
        return
