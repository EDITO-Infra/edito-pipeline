
import os
import logging
import pandas as pd
import shutil
from datetime import datetime, date
import csv
from .utils.s3 import S3Utils, EMODnetS3
from typing import TYPE_CHECKING
import pystac
import xarray as xr

if TYPE_CHECKING:
    from layer_catalog import LayerCatalog
    from metagisconfig2stac.pipeline import Pipeline

logger = logging.getLogger(__name__)
datadir = os.path.join(os.path.dirname(__file__), '../data')
today = datetime.today().strftime('%Y-%m-%d')

class PipelineResults:
    """
    Class that performs the last steps necessary to finish the pipeline such as transferring the STAC
    to an S3 bucket, pushing the STAC to the Resto database, or creating a transformation or attributes table.
    """
    def __init__(self, pipeline_config: dict, layer_catalog: ('LayerCatalog'), local_stac: pystac.Catalog):
        """
        :param pipeline_config: The pipeline configuration dictionary
        :type pipeline_config: dict
        :param layer_catalog: The layer catalog object created by the pipeline
        :type layer_catalog: LayerCatalog
        """
        self.layer_catalog = layer_catalog
        self.local_stac = local_stac
        self.pipeline_config = pipeline_config
        self.results_complete = False
        self.emod_s3 = EMODnetS3(pipeline_config)
        self.botoclient = self.emod_s3.botoclient
        self.bucket = self.emod_s3.bucket
        self.host = self.emod_s3.host

    def finish_pipeline(self):
        """
        Main method to finish the pipeline.  Pushes STAC to S3, backs up an existing STAC, pushes STAC to Resto, creates transformation table,
        creates attributes table

        :return: The status of the pipeline results, 'complete' if all successive steps ran.
        :rtype: str
        """
        logger.info("Creating pipeline results")
        pipeline_results = 'incomplete'
        logger.info("Creating transformation table")
        if not self.make_cp_transformation_table():
            return
        logger.info("Creating attributes table")    
        if not self.make_attributes_table():
            return
        logger.info("Creating back up STAC on S3")
        if not self.create_backup_stac_s3():
            return
        logger.info("Pushing logs to local STAC")
        if not self.logs_to_local_stac():
            return
        logger.info("Pushing local STAC to S3")
        if not self.push_local_stac_to_s3():
            return
        logger.info("Pushing to resto")
        if not self.push_to_resto():
            return
        
        pipeline_results = 'complete'
        outfile = f"{datadir}/layer_catalog/{self.pipeline_config['layer_collection']}_{today}_end.json"
        self.layer_catalog.serialize_layer_catalog_to_json(outfile)
        return pipeline_results


    def create_backup_stac_s3(self):
        """
        Will create a backup of the STAC catalog on S3.  The backup will be stored in a new directory with
        the current date appended to the end of the directory name.

        :return: True if the backup was successful, False otherwise
        :rtype: bool
        """
        if 'create_backup_stac' in self.pipeline_config and self.pipeline_config['create_backup_stac'] == "False":
            logger.info("create_backup_stac == False, skipping")
            return True
        today_date = datetime.now().strftime("%Y-%m-%d")
        stac_dir = f"{self.pipeline_config['stac_s3']}"
        backup_dir = f"{self.pipeline_config['stac_s3_backup']}_{today_date}"
        if S3Utils.move_s3_objects(self.botoclient, self.bucket, self.host, stac_dir, backup_dir):
            logger.info(f"-{stac_dir} backed up to {backup_dir}")
            return True
        logger.error(f"back up {stac_dir} to {backup_dir} failed")
        return False

    
    def logs_to_local_stac(self):
        """
        Transfers the logs from the data/logs directory to the data/stac directory. This is done to ensure that the logs are included
        in the STAC catalog that is pushed to the S3 bucket.

        :return: True if the logs were successfully added to the STAC catalog, False otherwise
        :rtype: bool
        """
        datadir = self.pipeline_config['datadir']
        try:
            for file in os.listdir(f'{datadir}/logs'):
                if file.endswith('.txt'):
                    shutil.copy(f'{datadir}/logs/{file}', f'{datadir}/stac')
            logger.info("Logs added to STAC")
            return True
        except Exception as e:
            logger.error(f"Failed to push logs to {self.pipeline_config['stac_s3']}: {e}")
            return False
    

    def push_local_stac_to_s3(self):
        """
        Push the locally created STAC catalog to the S3 bucket to the location specified in the pipeline_config.

        :return: True if the STAC catalog was successfully pushed to the S3 bucket, False otherwise
        :rtype: bool
        """
        if 'push_to_s3' in self.pipeline_config and self.pipeline_config['push_to_s3'] == "False":
            logger.info("push_to_s3 == False, skipping")
            return True
        try:

            datadir = self.pipeline_config['datadir']
            local_stac = f"{datadir}/stac"
            S3Utils.sync_to_s3(self.botoclient, self.bucket, self.host, local_stac, self.pipeline_config['stac_s3'])
            logger.info(f"STAC catalog pushed to {self.pipeline_config['stac_s3']}")
            return True
        except Exception as e:
            logger.info(f"Failed to push STAC catalog to {self.pipeline_config['stac_s3']}: {e}")
            return False
   
        
    def push_to_resto(self):
        """
        Create a RestoIngest object and push the STAC catalog to the Resto database.

        Credentials are needed to establish a connection to the Resto database 'data/creds/resto.env" file.

        :return: True if the STAC catalog was successfully pushed to the Resto database, False otherwise
        :rtype: bool
        """
        if 'push_to_resto' in self.pipeline_config and self.pipeline_config['push_to_resto'] == False:
            logger.info("Pushing to resto == False, skipping and finishing pipeline")
            return True
        from metagisconfig2stac.resto import STACRestoManager
        resto = STACRestoManager(self.pipeline_config, self.local_stac)
        resto.post_local_stac()
        resto.backup_resto_logs()
        logger.info("STAC catalog pushed to Resto")

        return True

    def make_cp_transformation_table(self):
        """
        If necessary, makes a transformation table of the Central Portal layers in EDITO STAC. Will be saved in local stac and transferred to S3.
        Is visible https://edito-infra.dev.emodnet.eu/emodnettransformationtable/

        :return: True if the transformation table was successfully created, False otherwise
        :rtype: bool
        """
        if 'cp_transformation_table' in self.pipeline_config and self.pipeline_config['cp_transformation_table'] == True:
            logger.info("Creating CP transformation table")
            transform_table_maker = CPTransformationTable(self.pipeline_config, self.layer_catalog)
            transform_table_df = transform_table_maker.make_cp_transformation_table()
            if transform_table_df.shape[0] == 0:
                return False
        elif 'cp_transformation_table' in self.pipeline_config and self.pipeline_config['cp_transformation_table'] == False:
            logger.info("CP transformation table == False, skipping")
        
        return True

    def make_attributes_table(self):
        """
        If necessary, a table is created of the attributes from the layers in the layer collection that are in the STAC catalog.
        The attributes table is added to the local STAC catalog, which is then pushed to the S3 bucket.  This is done to ensure that
        attributes that have been updated in Metagis, and integrated into the STAC catalog are available and can be viewed
        from the S3 bucket.
        ex. https://edito-infra.dev.emodnet.eu/attributestable/
        :return: True if the attributes table was successfully created, False otherwise
        :rtype: bool
        """
        if 'attributes_table' in self.pipeline_config and self.pipeline_config['attributes_table'] == True:
            logger.info("Creating attributes table")
            attributes_table_maker = AttributesTable(self.pipeline_config, self.layer_catalog, )
            attributes_table_df = attributes_table_maker.create_attributes_table()
            if attributes_table_df.shape[0] == 0:
                return False
        elif 'attributes_table' in self.pipeline_config and self.pipeline_config['attributes_table'] == False:
            logger.info("Attributes table == False, skipping")
        
        return True

class CPTransformationTable:
    """
    Class to create the transformation table of the Central Portal layers in STAC.  This class creates
    a layer catalog object for the central portal and removes admin units, PACE and Ingestion layers. Then
    adds layer catalog themes from the pipeline layer catalog to the CP dataframe to create the transformation table
    with the necessary information to compare what data from EMODnet Central Portal is in the pipeline layer catalog
    and has been integrated into the STAC catalog.

    """

    def __init__(self, pipeline_config, pipeline_layer_catalog: ('LayerCatalog')):
        """
        Initializes the CPTransformationTable object with the pipeline layer catalog and pipeline config.
        """
        # pipeline layer catalog is the layer catalog created by the pipeline
        self.pipeline_layer_catalog = pipeline_layer_catalog
        self.pipeline_config = pipeline_config

    def make_cp_transformation_table(self):
        """
        Main function to create the transformation table of the Central Portal layers in STAC. 
        Creates a layer catalog object for the central portal and removes admin units, PACE and Ingestion layers. Then 
        adds layer catalog themes from the pipeline layer catalog to the CP dataframe to create the transformation table
        :return:
        """
        cp_df = self._make_cp_catalog()
        if cp_df is None or cp_df.empty:
            logger.error("Failed to create CP transformation table")
            return None
            
        transform_table = self._add_layercatalog_themes(cp_df)
        if transform_table is None or transform_table.empty:
            logger.error("Failed to add layer catalog themes to CP dataframe")
            return None

        return transform_table

    def _make_cp_catalog(self):
        """
        :return: The CP dataframe with layer catalog themes added.
        :rtype: pd.DataFrame
        """
        # create CP layer catalog, to compare with the layer catalog created by the pipeline
        cp_config = {
            "layer_collection_config": "https://emodnet.ec.europa.eu/geoviewer/config.php",
            "layer_collection": "central_portal",
            "previous_layer_catalog" : "data/previous_state_files/previous_edito_state_2024-09-04.json",
        }
        cp_config['datadir'] = self.pipeline_config['datadir']
        from metagisconfig2stac.layer_catalog import LayerCatalog
        from metagisconfig2stac.layer import Layer, LayerMetadataUpdater
        cp_catalog = LayerCatalog(cp_config)
        cp_catalog = cp_catalog.create_layer_catalog_load_select(cp_config)
        if cp_catalog is None:
            logger.error("Failed to create CP layer catalog")
            return None
        logger.info("getting CP layers")
        
        
        # rebuild the CP layer catalog without Admin units, PACE and Ingestion layers
        thematic_lots_to_omit = ['Administrative units', 'EU-China EMOD-PACE project', 'EMODnet Ingestion']
        cp_layers = {k: v for k, v in cp_catalog.layer_catalog_dict.items() if v['thematic_lot'] not in thematic_lots_to_omit}
        
        cp_layer_list = []

        for id, cp_layer_metadata in cp_layers.items():

            cp_layer = Layer(self.pipeline_config, cp_layer_metadata)
            updater = LayerMetadataUpdater(cp_layer)
            # update basic metadata for each layer to get the download url, if available
            updater.add_metadataSources()

            cp_layer_list.append({
                'id': cp_layer.metadata.get('id', ''),
                'thematic_lot': cp_layer.metadata.get('thematic_lot', ''),
                'title': cp_layer.metadata.get('defaultTitle', ''),
                'subtheme': cp_layer.metadata.get('subtheme', ''),
                'subsubtheme': cp_layer.metadata.get('subsubtheme', ''),
                'download_url': cp_layer.metadata.get('download_url', ''),
            })

        cp_df = pd.DataFrame(cp_layer_list)
        cp_df.to_csv(f"{self.pipeline_config['datadir']}/central_portal_layer_catalog.csv", index=False, quoting=csv.QUOTE_MINIMAL)
        return cp_df


    def _add_layercatalog_themes(self, cp_df: pd.DataFrame):
        """
        Adds current layer catalog themes to the CP dataframe, shows which layers in CP have been given themes
        :param cp_df: The CP dataframe to add layer catalog themes to.
        :type cp_df: pd.DataFrame
        :return: The CP dataframe with layer catalog themes added.
        :rtype: pd.DataFrame
        """
        logger.info("Adding layer catalog themes to CP dataframe")

        for id, layermetadata in self.pipeline_layer_catalog.layer_catalog_dict.items():
            if int(id) in cp_df['id'].values:
                cp_df.loc[cp_df['id'] == layermetadata['id'], f"native_asset"] = layermetadata.get('native_asset', '')
                cp_df.loc[cp_df['id'] == layermetadata['id'], f"edito_family"] = layermetadata['subtheme']
                cp_df.loc[cp_df['id'] == layermetadata['id'], f"edito_collection"] = layermetadata['subsubtheme']
                cp_df.loc[cp_df['id'] == layermetadata['id'], f"collection_item"] = layermetadata['name']
                cp_df.loc[cp_df['id'] == layermetadata['id'], f"in_stac"] = layermetadata.get('in_stac', '')

        cp_df.to_csv(f"{self.pipeline_config['datadir']}/stac/emodnet_edito_transformation_table.csv", index=False, quoting=csv.QUOTE_MINIMAL)
        logger.info("CP transformation table created successfully in data/stac/emodnet_edito_transformation_table.csv")

        return cp_df

class AttributesTable:
    """
    Creates an table of the attributes from all the layers that have converted ARCO products, and the arco variables(standard names) 
    that are in the local STAC.
    """
    def __init__(self, pipeline_config, layer_catalog: ('LayerCatalog')):
        """
        :param pipeline_config: The pipeline configuration dictionary
        :type pipeline_config: dict
        :param layer_catalog: The layer catalog being processed by the pipeline
        :type layer_catalog: LayerCatalog
        """
        self.pipeline_config = pipeline_config
        self.layer_catalog = layer_catalog
        self.attributes_table = None
        self.attributes_table = self.create_attributes_table()

    def create_attributes_table(self):
        from metagisconfig2stac.utils.core import CoreUtils
        attributes_data = []

        # check each layer in layer catalog for native and arco attributes, 
        # check arco assets for arco attributes, check for data product, add to attributes table
        for id, layermetadata in self.layer_catalog.layer_catalog_dict.items():
            base_layer_data = {
                'id': layermetadata['id'],
                'edito_variable_family': layermetadata['subtheme'],
                'edito_collection': layermetadata['subsubtheme'],
                'collection_item': layermetadata['name'],
                'download_url': layermetadata.get('download_url', ""),
                'native_asset_s3_url': layermetadata.get('native_asset', ""),
                'data_product': layermetadata.get('native_data_product', ""),
                'updated_native_variable': "",
                'updated_arco_variable': "",
                'in_arco': False,
                'in_stac': False
            }

            # add data product to attributes table if available
            if 'native_data_product' not in layermetadata:
                logger.info(f"no data_product specified for layer {layermetadata['id']} {layermetadata['name']}")
            elif 'native_data_product' in layermetadata:
                logger.info(f"data_product specified for layer {layermetadata['id']} {layermetadata['name']}")
                base_layer_data['data_product'] = layermetadata['native_data_product']

            # check for native and arco attributes
            if 'native_arco_mapping' in layermetadata:
                arco_attributes_from_metagis = layermetadata['native_arco_mapping']
                for updated_native_variable in arco_attributes_from_metagis.keys():
                    updated_arco_variable = arco_attributes_from_metagis[updated_native_variable]
                    layer_data = base_layer_data.copy()
                    layer_data['updated_native_variable'] = updated_native_variable
                    layer_data['updated_arco_variable'] = updated_arco_variable

                    # check arco assets for arco attributes
                    if len(layermetadata['converted_arco_assets']) > 0:
                        arcovars = CoreUtils.get_arco_assets_variables(layermetadata)
                        if len(arcovars) == 0:
                            logger.error(f"no arco variables found in arco assets for layer {layer_data['id']}")
                            layer_data['in_arco'] = False
                        elif updated_arco_variable in arcovars:
                            layer_data['in_arco'] = True
                        else:
                            logger.error(f"arco variable {updated_arco_variable} not found in arco assets {arcovars}")
                            layer_data['in_arco'] = False
                    
                    # check if arco var added to STAC and metadata by stac builder
                    if 'arco_vars_in_stac' in layermetadata and updated_arco_variable in layermetadata['arco_vars_in_stac']:
                        layer_data['in_stac'] = True
                    else:
                        logger.error(f"arco variable {updated_arco_variable} not in STAC for {layer_data['id']}")
                        layer_data['in_stac'] = False

                    attributes_data.append(layer_data)
            else:
                logger.info(f"no attributes in metadata, no arco conversion")
                attributes_data.append(base_layer_data)

        attributes_df = pd.DataFrame(attributes_data)
        attributes_df.to_csv(f"{self.pipeline_config['datadir']}/stac/attributes_table_{today}.csv", index=False, quoting=csv.QUOTE_MINIMAL)
        return attributes_df   
