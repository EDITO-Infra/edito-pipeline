import json
import logging
from datetime import datetime
from shapely.geometry import Polygon, mapping
import pytz
from pytz import utc
import pystac
from pystac.extensions.scientific import ScientificExtension
from metagisconfig2stac.layer import Layer
from .utils.core import CoreUtils
import os
import xarray as xr

# Setup loggers
logger = logging.getLogger(__name__)

class STACBuilder:
    """
    Builds a local STAC using the updated layers from the Metagis database.
    """
    def __init__(self, pipeline_config):
        """
        :param pipeline_config: The pipeline configuration.
        :type pipeline_config: dict
        """
        self.pipeline_config = pipeline_config
        self.datadir = pipeline_config['datadir']
        self.stac_root_ref = f"{self.datadir}/stac/catalog.json"
        self.create_or_load_local_stac()
        self.stacutils = STACUtils(pipeline_config)


    def create_or_load_local_stac(self):
        """
        Create or load the local STAC catalog.
        """
        if os.path.exists(self.stac_root_ref):
            self.local_stac = pystac.Catalog.from_file(self.stac_root_ref)
        else:
            logger.info(f"Creating new STAC catalog at {self.stac_root_ref}")
            self.local_stac = pystac.Catalog(
                id = CoreUtils.custom_slugify(self.pipeline_config['stac_title']),
                description = f'STAC Catalog for {self.pipeline_config["stac_title"]}',
                title=self.pipeline_config['stac_title'],
                href=self.stac_root_ref
            )
            self.local_stac.normalize_hrefs(self.stac_root_ref)
            self.save_catalog(self.local_stac)
        return self.local_stac


    def add_layer_to_stac(self, layer: ('Layer')):
        """
        Add metagis layer to STAC catalog. Directs creation of native and arco feature types to _add_layer_data_to_stac().
        Returns layer with 'in_stac' status.

        :param layer: The layer to add to the STAC.
        :type layer: Layer
        :return: The updated layer.
        :rtype: Layer

        """
        layer.metadata['in_stac'] = 'no'
        self.layer_in_stac = False
    
        variable_family_catalog = self.create_or_use_variable_family(layer, self.local_stac)
        
        logger.info(f"looking for native asset and adding to stac")
        if 'native_asset' not in layer.metadata:
            logger.error(f"no native asset found for {layer.metadata['id']}, layer will not be added to STAC")
            return

        layer,_ = self._add_layer_data_to_stac(layer, 'native', layer.metadata['subsubtheme'], variable_family_catalog)
    
        
        logger.info(f"looking for converted arco assets and adding to stac")
        if layer.metadata.get('converted_arco_assets'):
            # lookup variables from ARCO assets
            arco_assets_vars = CoreUtils.get_arco_assets_variables(layer.metadata)
            logger.info(f"Adding ARCO items to STAC for {layer.metadata['id']}")
            layer_arcovars_in_stac = {}
            for native_var, arco_var in layer.metadata['native_arco_mapping'].items():
                if arco_var in arco_assets_vars:
                    logger.info(f"Adding Arco variable {arco_var} to STAC")
                    layer, item = self._add_layer_data_to_stac(layer, 'arco', arco_var, variable_family_catalog)
                    layer_arcovars_in_stac[arco_var] = item.id
                else:
                    logger.error(f"{layer.metadata['id']} Arco variable {arco_var} not found in converted Arco assets, {arco_var} can't go in STAC")
                # add arco vars in stac to layer metadata
                layer.metadata['arco_vars_in_stac'] = layer_arcovars_in_stac
        return layer


    def _add_layer_data_to_stac(self, layer:('Layer'), feature_type: str, variable: str, variable_family_catalog: pystac.Catalog):
        """
        Add layer data to the STAC, for native or ARCO feature types. Returns updated layer and item.

        :param layer: The layer to add to the STAC.
        :type layer: Layer
        :param feature_type: The type of feature to add, either 'native' or 'arco'. 
        :type feature_type: str
        :param variable: The variable name to add to the STAC.
        :type variable: str
        :param variable_family_catalog: The parent catalog for the variable family.
        :type variable_family_catalog: pystac.Catalog
        :return: The updated layer.
        :rtype: Layer
        """

        if variable == "" or variable is None:
            if feature_type == 'native':
                logger.error(f"{layer.metadata['id']} {feature_type}, no collection, check metagis layer subsubtheme")
            if feature_type == 'arco':
                logger.error(f"{layer.metadata['id']} {feature_type}, no collection, check metagis layer attributes")
            return 
        logger.info(f'adding or using collection for variable {variable}')

        collection = self.create_or_use_collection(layer, variable_family_catalog, variable)
        if not collection:
            logger.error(f'Failed to add {variable} to {variable_family_catalog}')
            return

        # return the updated collection in case item is updated
        collection, item = self.create_stac_item(layer, feature_type, collection, variable if feature_type == 'arco' else None)
        if item is None:
            logger.error(f'Failed to make item from {variable}')
            return

        collection.add_item(item)
        collection.extent = pystac.Extent.from_items(collection.get_all_items())
        self.save_catalog(self.local_stac)
        self.layer_in_stac = True
        layer.metadata['in_stac'] = 'yes'
        logger.info(f"Successfully added {feature_type} STAC item {layer.metadata['id']}")
        return layer, item


    def save_catalog(self, catalog: pystac.Catalog):
        """
        :param catalog: The STAC catalog to save.
        :type catalog: pystac.Catalog
        """
        try:
            catalog.normalize_hrefs(self.stac_root_ref)
            catalog.save(catalog_type='SELF_CONTAINED')
            logger.info("Catalog saved successfully.")
        except Exception as e:
            logger.error(f"Failed to save catalog: {e}")


    def create_or_use_variable_family(self, layer: ('Layer'), local_stac: pystac.Catalog):
        """
        Make or use an existing variable family catalog in the local STAC.

        :param layer: The layer to add to the STAC.
        :type layer: Layer
        :param local_stac: The local STAC catalog.
        :type local_stac: pystac.Catalog
        """
            
        variable_family = CoreUtils.custom_slugify(layer.metadata['subtheme'])
        logger.info(f"Adding variable family {variable_family} for{layer.metadata['id']} {layer.metadata['name']}")
        
        if not local_stac.get_children() or variable_family not in [child.id for child in local_stac.get_children()]:
            variable_family_catalog = pystac.Catalog(
                id=variable_family,
                description=f"Variable Family {variable_family}",
                title=variable_family,
                href=self.stac_root_ref
            )
            local_stac.add_child(variable_family_catalog)
        else:
            variable_family_catalog = local_stac.get_child(variable_family)
        logger.info(f'family {variable_family} in STAC')
        return variable_family_catalog
        

    def create_or_use_collection(self, layer: ('Layer'), variable_family_catalog: pystac.Catalog, variable: str):
        """
        Make or use an existing collection in the variable family catalog.

        :param layer: The layer to add to the STAC.
        :type layer: Layer
        :param variable_family_catalog: The variable family catalog.
        :type variable_family_catalog: pystac.Catalog
        :param variable: The variable name to add to the STAC.
        :type variable: str
        :return: The collection for the variable.
        :rtype: pystac.Collection
        """
        collection_id = CoreUtils.custom_slugify(variable)
        prefix = self.stacutils.check_standard_name_convention(collection_id, variable_family_catalog.id)
        collection_id = f"{prefix}-{collection_id}"
        logger.info(f"Adding collection or using {collection_id} for {layer.metadata['id']} {layer.metadata['name']}")

        if not variable_family_catalog.get_children() or collection_id not in [child.id for child in variable_family_catalog.get_children()]:
            logger.info(f"Creating collection {collection_id} for {variable} {layer.metadata['id']} {layer.metadata['name']}")
            bounding_box = layer.metadata.get('geographic_extent', [-180, -90, 180, 90])
            polygon = Polygon([(bounding_box[0], bounding_box[1]), (bounding_box[0], bounding_box[3]),
                            (bounding_box[2], bounding_box[3]), (bounding_box[2], bounding_box[1])])
            unique_providers = {}
            for provider_dict in layer.metadata['provider']:
                if provider_dict['name'] not in unique_providers:
                    unique_providers[provider_dict['name']] = provider_dict['role']

            var_collection = pystac.Collection(
                id=collection_id,
                title=variable,
                description=f"A collection of {variable} data",
                license="CC-BY-4.0+",
                providers=[pystac.Provider(name=provider, roles=role) for provider, role in unique_providers.items()],
                extent=None,
            )
            logger.info(f'adding collection {var_collection} to {variable_family_catalog}')
            variable_family_catalog.add_child(var_collection)
        else:
            logger.info(f"Using existing collection {collection_id} for {variable} {layer.metadata['id']} {layer.metadata['name']}")
            var_collection = variable_family_catalog.get_child(collection_id)

        logger.info(f'collection {var_collection} in STAC')
        return var_collection


    def create_stac_item(self, layer: ('Layer'), feature_type: str, collection: pystac.Collection, variable=None):
        """
        Make a STAC item for the layer. Remove existing item if it exists. Add to collection.

        :param layer: The layer to add to the STAC.
        :type layer: Layer
        :param feature_type: The type of feature to add, either 'native' or 'arco'.
        :type feature_type: str
        :param collection: The collection to add the item to.
        :type collection: pystac.Collection
        :param variable: The variable name to add to the STAC.
        :type variable: str
        :return: The updated collection and item.
        :rtype: tuple(pystac.Collection, pystac.Item)
        """
        logger.info(f"making or returning {feature_type} item for {layer.metadata['id']} {layer.metadata['name']} {collection}")
        
        start_datetime, end_datetime = STACUtils.format_start_end_datetimes_stac(layer.metadata['temporal_extent'])
        bounding_box = layer.metadata['geographic_extent']
        min_lon, min_lat, max_lon, max_lat = bounding_box
        polygon = Polygon([(min_lon, min_lat), (min_lon, max_lat), (max_lon, max_lat), (max_lon, min_lat)])

        if feature_type == 'native':
            itemid = CoreUtils.custom_slugify(layer.metadata['name'])
        if feature_type == 'arco':
            bounding_box_str = f"{min_lon}_{min_lat}_{max_lon}_{max_lat}"
            startdatestr = start_datetime.strftime("%Y-%m-%dT%H:%M:%SZ")
            enddatestr = end_datetime.strftime("%Y-%m-%dT%H:%M:%SZ")
            #itemid = f'{variable}_{start_datetime}_{end_datetime}_{bounding_box_str}'
            itemid = f"{variable}_{startdatestr}_{enddatestr}_{bounding_box_str}"

            # where a description is available in the arco attributes, use in the item id
            if 'attributes' in layer.metadata and variable in layer.metadata['attributes']:
                if 'arco_variable_description' in layer.metadata and variable in layer.metadata['arco_variable_description']:
                    itemid = f"{variable}_{layer.metadata['arco_variable_description'][variable]}_{bounding_box_str}_{start_datetime}_{end_datetime}"
                    logger.info(f"item id updated with arco variable description {layer.metadata['arco_variable_description'][variable]}")
        
        item = pystac.Item(
            id=itemid,
            geometry=mapping(polygon),
            bbox=[min_lon, min_lat, max_lon, max_lat],
            datetime=None,
            start_datetime=start_datetime,
            end_datetime=end_datetime,
            properties={
                "provider": STACUtils.get_stac_item_provider(layer),
                "data rights and restrictions": layer.metadata.get('data_rights_restrictions', 'CC-BY-4.0'),
                "proj:epsg": layer.metadata.get('proj:epsg', 4326),
                "title": itemid.replace('_', ' ').capitalize(),
            },
            stac_extensions=["https://stac-extensions.github.io/projection/v1.1.0/schema.json"]
        )
        logger.info(f'adding assets to {item.id}')
        asset_list = layer.metadata['assets'] + [layer.metadata['native_asset']]
        for asset in asset_list:
            try:
                asset_type, title, mediatype, roles = STACUtils.get_stac_media_type(asset)
            except Exception as e:
                logger.info(f"Skipping asset {asset} for {item.id} {e}")      
                continue
            item.assets[asset_type] = pystac.Asset(href=asset, media_type=mediatype, title=title, roles=roles)
        
        if feature_type == 'arco':
            logger.info(f'trying to add arco assets for arco feature')
            if 'converted_arco_assets' in layer.metadata:
                logger.info(f'processing arco assets to {item.id}')
                for asset in layer.metadata['converted_arco_assets']:
                    asset_type, title, mediatype, roles = self.stacutils.get_stac_media_type(asset)
                    item.assets[asset_type] = pystac.Asset(href=asset, media_type=mediatype, title=title, roles=roles)
            
            if 'native_arco_mapping' in layer.metadata:
                logger.info(f'adding native variable to properties')
                for key, value in layer.metadata['native_arco_mapping'].items():
                    if value == variable:
                        item.properties['native_variable'] = key
                        break
                    
            if 'categorical_encoding' in layer.metadata and variable in layer.metadata['categorical_encoding']:
                logger.info(f'adding categorical encoding to properties')
                item.properties['categorical_encoding'] = layer.metadata['categorical_encoding'][variable]
        
        if item.id in [item.id for item in collection.get_items()]:
            logger.info(f'Item already in {collection} {variable}, remove and update')
            collection.remove_item(item.id)
            self.create_stac_item(layer, feature_type, collection, variable)
        
        return collection, item

class STACUtils:
    """
    Utility functions for adding layers to a STAC.
    """
    def __init__(self, pipeline_config: dict):
        """
        :param pipeline_config: The pipeline configuration.
        :type pipeline_config: dict
        """
        self.pipeline_config = pipeline_config
        self.datadir = pipeline_config['datadir']

    @staticmethod
    def format_start_end_datetimes_stac(temporal_extent):
        """
        Convert start and end datetime strings from a temporal extent into Python datetime objects.

        :param temporal_extent: A dictionary with 'start' and 'end' datetime strings.
        :type temporal_extent: dict
        :return: A tuple containing the start and end datetimes in UTC format.
        :rtype: tuple(datetime, datetime)
        :raises ValueError: If the datetime strings cannot be parsed.
        """
        start_datetime_str = temporal_extent['start']
        end_datetime_str = temporal_extent['end']
        try:
            start_date = datetime.strptime(start_datetime_str, "%Y-%m-%dT%H:%M:%SZ").date()
            start_datetime = datetime.combine(start_date, datetime.min.time()).replace(tzinfo=utc)
            end_date = datetime.strptime(end_datetime_str, "%Y-%m-%dT%H:%M:%SZ").date()
            end_datetime = datetime.combine(end_date, datetime.min.time()).replace(tzinfo=utc)
            return start_datetime, end_datetime
        except ValueError as e:
            raise ValueError(f"Error converting datetime string: {e}")
        

    @staticmethod
    def find_possible_stac_extensions(item):
        """
        Identify possible STAC extensions based on the item properties and assets.

        :param item: The STAC item as a dictionary.
        :type item: dict
        :return: None. Appends appropriate STAC extension URLs to a list.
        :rtype: None
        """
        extensions = []
        if 'properties' in item:
            if 'proj:epsg' in item['properties']:
                extensions.append('https://stac-extensions.github.io/projection/v1.1.0/schema.json')
            if 'zarr' in item['assets']:
                extensions.append('https://stac-extensions.github.io/datacube/v1.0.0/schema.json')
        return extensions


    @staticmethod
    def get_stac_media_type(asset_url: str) -> tuple[str, str, str, list[str]]:
        """
        Get the media type of a STAC asset based on its URL or file extension.

        :param asset_url: The URL of the asset.
        :type asset_url: str
        :return: A tuple containing asset type, title, media type, and role.
        :rtype: tuple(str, str, str, list[str])
        :raises ValueError: If the media type is not supported.
        """
        media_types = {
            '.zarr': ('zarr', 'Zarr', 'application/vnd+zarr', ['data']),
            '.zarr/': ('zarr', 'Zarr', 'application/vnd+zarr', ['data']),
            '.nc': ('netcdf', 'NetCDF', 'application/vnd+netcdf', ['data']),
            '.zip': ('zip', 'Zip', 'application/zip', ['data']),
            '.tif': ('geotiff', 'GeoTIFF', 'image/tiff; application=geotiff', ['data']),
            '.tiff': ('geotiff', 'GeoTIFF', 'image/tiff; application=geotiff', ['data']),
            '.parquet': ('parquet', 'Parquet', 'application/vnd+parquet', ['data']),
            '.parquet/': ('parquet', 'Parquet', 'application/vnd+parquet', ['data']),
            '.geoparquet': ('geoparquet', 'GeoParquet', 'application/vnd+parquet', ['data']),
            '.geoparquet/': ('geoparquet', 'GeoParquet', 'application/vnd+parquet', ['data']),
            '.csv': ('csv', 'CSV', 'text/csv', ['data']),
            '.json': ('json', 'JSON', 'application/json', ['metadata']),
            '.html': ('html', 'HTML', 'text/html', ['metadata']),
            '.png': ('png', 'PNG', 'image/png', ['thumbnail']),
            '.jpg': ('jpg', 'JPEG', 'image/jpeg', ['thumbnail']),
        }

        url_patterns = {
            'wms?SERVICE=WMS&REQUEST=GetMap': ('wms', 'Web Map Service (WMS)', 'OGC:WMS', ['service']),
            'xml': ('xml', 'XML', 'application/xml', ['metadata']),
            'csw?request': ('csw', 'CSW', 'application/csw', ['metadata']),
            # 'mda.vliz.be/download.php?file=VLIZ': ('mda-link', 'MDA Link', 'application/vnd+mda', ['data']),
        }

        for ext, (asset_type, title, mediatype, role) in media_types.items():
            if asset_url.endswith(ext):
                return asset_type, title, mediatype, role

        for pattern, (asset_type, title, mediatype, role) in url_patterns.items():
            if pattern in asset_url:
                return asset_type, title, mediatype, role

        raise ValueError(f"Media type not supported in EDITO STAC: {asset_url}")

    @staticmethod
    def get_stac_item_provider(layer):
        """
        Get provider information for the STAC item.

        :param layer: The layer to add to the STAC.
        :type layer: Layer
        :return: The provider information.
        :rtype: list[dict]
        """
        # if thematic lot not in layer metadata provider, add to provider
        item_provider = layer.metadata.get('provider', f"{layer.metadata['thematic_lot']}")
        thematic_lot = layer.metadata.get('thematic_lot')
        thematic_lot_found = False

        for provider in item_provider:
            if thematic_lot in provider.keys() or thematic_lot in provider.values():
                thematic_lot_found = True
                break
        if not thematic_lot_found:
            item_provider.append({'name': thematic_lot, 'role': 'provider'})
        return item_provider


    def update_emodnet_standard_names(edito_collection, variable_family):
        """
        Update the EMODnet standard names JSON file with a new collection.

        :param edito_collection: The collection name to add to the EMODnet standard names.
        :type edito_collection: str
        :param variable_family: The variable family associated with the new collection.
        :type variable_family: str
        :return: The prefix used for the collection in the EMODnet standard names.
        :rtype: str
        """
        prefix = 'emodnet'
        filename = '../data/standard_names/emodnet_standard_names.json'
        if os.path.exists(filename):
            try:
                with open(filename, 'r') as f:
                    data = json.load(f)
            except Exception as e:
                logger.error(f"Error loading emodnet_standard_names.json: {e}")
                return
        else:
            data = {}
            data['emodnet_standard_names'] = []

        for emodnet_entry in data['emodnet_standard_names']:
            if edito_collection == emodnet_entry['standard_name']:
                logger.info(f"{edito_collection} already in emodnet_standard_names.json, check failed")
                return
        
        new_entry = {
            "Convention": prefix,
            "standard_name": edito_collection,
            "description": "",
            "canonical_units": "",
            "amip": "",
            "grib": "",
            "aliases": [],
            "variable_family": variable_family,
        }
        data['emodnet_standard_names'].append(new_entry)
        with open(filename, 'w') as f:
            json.dump(data, f, indent=4)
        logger.info(f"{edito_collection} added to emodnet_standard_names.json")
        return prefix


    def check_standard_name_convention(self, edito_collection, variable_family):
        """
        Check if a collection exists in any standard names files and update the EMODnet standard names if not found.

        :param datadir: Pipeline data directory
        :type datadir: str
        :param edito_collection: The collection name to check.
        :type edito_collection: str
        :param variable_family: The associated variable family.
        :type variable_family: str
        :return: The standard name convention for the collection.
        :rtype: str
        """
        standard_names_dir = f'{self.datadir}/standard_names'
        for namefile in os.listdir(standard_names_dir):
            logger.info(f"Checking {namefile}")
            filepath = os.path.join(standard_names_dir, namefile)
            
            try:
                with open(filepath, 'r') as f:
                    namesdict = json.load(f)
            except Exception as e:
                logger.error(f"Error loading {namefile}: {e}")
                continue

            if namesdict is None:
                continue
            logger.info(f"Loaded {namefile}")
            try:
                namesdictlist = list(namesdict.values())[0]
                for namesitem in namesdictlist:
                    if namesitem.get('standard_name') == edito_collection:
                        standard_name_convention = list(namesdict.keys())[0].replace('_standard_names', '')
                        return standard_name_convention  # this is the prefix for the collection id
            except Exception as e:
                logger.error(f"Unexpected error processing {namefile}: {e}")
                continue
            continue
        logger.info(f"{edito_collection} not found in standard names files, updating emodnet standard names")
        standard_name_convention = self.update_emodnet_standard_names(edito_collection, variable_family)
        return standard_name_convention
    

    def update_emodnet_standard_names(self, edito_collection, variable_family):
        """
        Update the EMODnet standard names JSON file with a new collection.

        :param edito_collection: The collection name to add to the EMODnet standard names.
        :type edito_collection: str
        :param variable_family: The variable family associated with the new collection.
        :type variable_family: str
        :return: The prefix used for the collection in the EMODnet standard names.
        :rtype: str
        """
        prefix = 'emodnet'
        filename =f'{self.datadir}/standard_names/emodnet_standard_names.json'
        if os.path.exists(filename):
            data = CoreUtils.load_local_json(filename)
        else:
            data = {}
            data['emodnet_standard_names'] = []

        for emodnet_entry in data['emodnet_standard_names']:
            if edito_collection == emodnet_entry['standard_name']:
                logger.info(f"{edito_collection} already in emodnet_standard_names.json, check failed")
                return
        
        new_entry = {
            "Convention": prefix,
            "standard_name": edito_collection,
            "description": "",
            "canonical_units": "",
            "amip": "",
            "grib": "",
            "aliases": [],
            "variable_family": variable_family,
        }
        data['emodnet_standard_names'].append(new_entry)
        with open(filename, 'w') as f:
            json.dump(data, f, indent=4)
        logger.info(f"{edito_collection} added to emodnet_standard_names.json")
        return prefix
