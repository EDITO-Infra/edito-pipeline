import json
import logging
from typing import List, TYPE_CHECKING
import requests
import xml.etree.ElementTree as ET
from io import StringIO
from datetime import datetime, timezone
import pytz
import pandas as pd
import os
import xarray as xr
from metagisconfig2stac.utils import EMODnetS3, S3Utils, CoreUtils
if TYPE_CHECKING:
    from metagisconfig2stac.layer_catalog import LayerCatalog


logger = logging.getLogger(__name__)
class Layer:
    """
    Class representation of the metadata from each Layer in Metagis. Updates its own basic metadata, providers data rights, temporal extent,
    geographic extent, and external assets.  
    """

    def __init__(self, pipeline_config: dict, metadata: dict):
        """
        Initializes a Layer instance with an empty metadata dictionary and a reference to the Metagis catalog.

        Args:
            metagiscatalog: A reference to the MetagisEditoLayerCollection instance that this layer belongs to.
        """
        self.pipeline_config = pipeline_config
        self.datadir = pipeline_config['datadir']
        self.metadata = metadata
        self.metadata['assets'] = []
        self.temp_assets = f'{self.datadir}/temp_assets'
        self.converted_arco = f'{self.temp_assets}/converted_arco'
        os.makedirs(self.converted_arco, exist_ok=True)
        os.makedirs(self.temp_assets, exist_ok=True)
        self.metadata['converted_arco_assets'] = []


    def update_basic_metadata(self):
        """
        Updates the layer's metadata with the provided dictionary.

        :param metadata: The new metadata for the layer.
        :type metadata: dict

        :return: The updated metadata dictionary for the layer.
        :rtype: dict
        """
        updater = LayerMetadataUpdater(self)
        updater.add_metadataSources()
        updater.update_external_assets()
        logger.info('Update layer metadata')
        return


    def update_providers_data_rights(self):
        """
        Updates the layer's metadata with the provided dictionary.

        :param metadata: The new metadata for the layer.
        :type metadata: dict

        :return: The updated metadata dictionary for the layer.
        :rtype: dict
        """
        updater = LayerMetadataUpdater(self)
        updater.update_providers_rights()
        logger.info('Update providers data rights')
        return


    def update_temporal_extent(self):
        """
        Updates the temporal extent of the layer based on the temporal extent of the native asset.

        :return: The updated metadata dictionary for the layer.
        :rtype: dict
        """
        updater = LayerMetadataUpdater(self)
        updater.update_temporal_extent()
        logger.info('Update temporal extent')
        return

    def update_geographic_extent(self):
        """
        Updates the geographic extent of the layer based on available metadata or geographic extent of available assets.

        :return: The updated metadata dictionary for the layer.
        :rtype: dict
        """
        updater = LayerMetadataUpdater(self)
        updater.update_geographic_extent()
        logger.info('Update geographic extent')
        return


    def transfer_native_asset_to_s3(self, layer_catalog_dict: dict):
        """
        Transfers the native asset for the layer to S3.

        :return: The layer object with updated metadata if the asset is found or downloaded.
        :rtype: Layer

        """
        native_asset = LayerNativeAsset(self, layer_catalog_dict)
        logger.info('Transferring native asset to S3')
        try:
            self = native_asset.handle_native_transfer()
            logger.info('Native asset transferred to S3')
        except Exception as e:
            logger.error(f"Failed to transfer native assets to S3: {e}")
        return self


    def convert_native_to_arco(self):
        """
        Converts the native asset for the layer to Arco format.

        :return: The layer object with updated metadata if the asset is found or converted.
        :rtype: Layer
        """
        logger.info('Converting native asset to Arco')
        from .arco_converter import ArcoConversionManager
        arco_manager = ArcoConversionManager(self.pipeline_config)
        
        try:
            self = arco_manager.manage_arco_conversion(self)
            logger.info('Native asset converted to Arco')
        except Exception as e:
            logger.error(f"Failed to convert native asset to Arco: {e}")
            
        return self


    def add_to_stac(self):
        """
        Adds the layer to the STAC catalog.

        :return: The layer object with updated metadata if the asset is found or converted.
        :rtype: Layer
        """
        logger.info('Adding layer to STAC')
        from .stac import STACBuilder
        stac_builder = STACBuilder(self.pipeline_config)
        try:
            updated_layer = stac_builder.add_layer_to_stac(self)
            return updated_layer
        except Exception as e:
            logger.error(f"Failed to add layer to STAC catalog: {e}")
            return


    def create_layer_metadata(self, metadata):
        """
        Updates the layer's metadata from the layer catalog (Layer Collection) and initializes an empty list for assets.

        Args:
            metadata (dict): The new metadata for the layer.

        Returns:
            dict: The updated metadata dictionary for the layer.
        """

        self.metadata['assets'] = []
        self.metadata['converted_arco_assets'] = []
        return self.metadata

class LayerMetadataUpdater:
    def __init__(self, layer: ('Layer')):
        self.layer = layer
        # self.pipeline_config = layer.pipeline_config
        self.layerid = self.layer.metadata['id']
        self.layername = self.layer.metadata['name']


# method to add the metadata sources (ex. download url, geonetwork uri, etc) directly to the layer metadata
    def add_metadataSources(self):
        logger.info(f"Adding metadata sources for {self.layerid} {self.layername}")
        metadataSources = self.layer.metadata['metadataSources']
        if isinstance(metadataSources, list):
            for source in metadataSources:
                self._find_metadata_sources(source)
        elif isinstance(metadataSources, dict):
            for source in metadataSources.values():
                self._find_metadata_sources(source)
        
    def _find_metadata_sources(self, source):
            logger.info(f"looking for download url in metadata sources {self.layerid} {self.layername} ")
            if source.get('metadata_type', '') == 'download_url':
                download_url = source['metadata_value']
                self.layer.metadata['download_url'] = download_url
            logger.info(f"looking for geonetwork uri in metadata sources {self.layerid} {self.layername}")
            if source.get('metadata_type', '') == 'geonetwork_uri':
                self.layer.metadata['assets'] = []
                geonetwork_uri = source['metadata_value']
                self.layer.metadata['geonetwork_uri'] = geonetwork_uri
                self.layer.metadata['assets'].append(f"https://emodnet.ec.europa.eu/geonetwork/srv/api/records/{geonetwork_uri}/formatters/xml")
                self.layer.metadata['assets'].append(f"https://emodnet.ec.europa.eu/geonetwork/emodnet/eng/csw?request=GetRecordById&service=CSW&version=2.0.2&elementSetName=full&id={geonetwork_uri}")
            logger.info('looking for native data product in metadata sources')
            if source.get('metadata_type', '') == 'edito_info':
                self.layer.metadata['native_data_product'] = source['metadata_value']

            # add unfound if not found
            if 'download_url' not in self.layer.metadata:
                logger.info(f"no download url found in metadata sources for {self.layerid} {self.layername}")
                self.layer.metadata['download_url'] = ""
            if 'geonetwork_uri' not in self.layer.metadata:
                logger.info(f"no geonetwork uri found in metadata sources for {self.layerid} {self.layername}")
                self.layer.metadata['geonetwork_uri'] = ""

    def update_geo_extent_from_netcdf_or_zarr(self):
        potential_assets = set(self.layer.metadata.get('assets', []))
        
        for asset in potential_assets:
            ds = None
            if asset.endswith('.nc') and 'https://s3' in asset:
                logger.info('netcdf asset found on s3, using for geographic extent')
                asset = asset + '#mode=bytes'
                try:
                    ds = xr.open_dataset(asset, engine='netcdf4')
                except Exception as e:
                    logger.info(f"Error opening asset {asset}: {e}")
                    continue
            elif asset.endswith('.zarr') or asset.endswith('.zarr/'):
                logger.info('zarr asset found, using for geographic extent')
                try:
                    ds = xr.open_dataset(asset, engine='zarr')
                except Exception as e:
                    logger.info(f"Error opening asset {asset}: {e}")
                    continue
            if ds:
                if ('latitude' in ds.dims and 'longitude' in ds.dims) or ('lat' in ds.dims and 'lon' in ds.dims):
                    lat_key = 'latitude' if 'latitude' in ds.dims else 'lat'
                    lon_key = 'longitude' if 'longitude' in ds.dims else 'lon'
                    
                    bbox = [
                        round(float(ds[lon_key].values.min()), 4),
                        round(float(ds[lat_key].values.min()), 4),
                        round(float(ds[lon_key].values.max()), 4),
                        round(float(ds[lat_key].values.max()), 4)
                    ]
                    self.layer.metadata['geographic_extent'] = bbox
                    logger.info(f"geographic extent updated from {asset}")
                    return True
        return False
    
# method to update the geographic extent, first try via bbox, then zarr or netcdf on s3, then geonetwork, then world bounds
    def update_geographic_extent(self):
        logger.info(f"Updating geographic extent for {self.layerid} {self.layername}")
        if 'geographic_extent' in self.layer.metadata and len(self.layer.metadata['geographic_extent']) == 4:
            logger.info('full geographic extent already exists')
            return
        
        if 'bbox' in self.layer.metadata and self.layer.metadata['bbox']:
            bbox = self.layer.metadata['bbox'][0]
            self.layer.metadata['geographic_extent'] = [bbox['west'], bbox['south'], bbox['east'], bbox['north']]
            return

        if self.update_geo_extent_from_netcdf_or_zarr():
            return

        if 'geonetwork_uri' in self.layer.metadata:
            logger.info('getting geographic extent from geonetwork')
            geonet_update = GeonetworkUpdater(self.layer)
            if geonet_update.get_geographic_extent():
                return

        self.layer.metadata['geographic_extent'] = [-180, -90, 180, 90]
        logger.warning(f"Using world bounds {self.layerid} id {self.layername}")
        return

# Methods for updating external assets
    def update_external_assets(self):
        logger.info(f"Updating external assets for {self.layerid} {self.layername}")
        self.test_wms_endpoint()
        if self.layer.metadata.get('geonetwork_uri'):
            logger.info('getting assets from geonetwork')
            geonet_update = GeonetworkUpdater(self.layer)
            geonet_update.get_csw_assets()
        return self.layer
    

    def test_wms_endpoint(self):
        """"
        Test the WMS endpoint for the layer and add it to the layer metadata if successful.

        :param layer: The layer object to test the WMS endpoint for.
        :type layer: Layer

        :return: None
        """
        # Check if the layer has a previous state with a WMS asset
        if 'previous_layer' in self.layer.metadata:
            previous_layer = self.layer.metadata['previous_layer']
            if previous_layer and 'assets' in previous_layer:
                for asset in previous_layer['assets']:
                    if 'wms?SERVICE=WMS&REQUEST=GetMap&LAYER' in asset:
                        self.layer.metadata['assets'].append(asset)
                        logger.info('WMS request found in previous state')
                        return

        # check if the layer has a WMS endpoint in the metadata properties
        if not self.layer.metadata.get('properties'):
            logger.warning('No properties found in layer metadata.')
            return
        properties = self.layer.metadata.get('properties')
        base_url = properties['url']
        layers = properties['params']['LAYERS']
        version = properties['params']['VERSION']
        time = properties['params'].get('TIME')
        wms_extent = properties.get('extent', self.layer.metadata.get('geographic_extent', [-180, -90, 180, 90]))
        bbox = f"{wms_extent[0]},{wms_extent[1]},{wms_extent[2]},{wms_extent[3]}"
        wms_request_url = f"{base_url}?SERVICE=WMS&REQUEST=GetMap&LAYERS={layers}&VERSION={version}&CRS=CRS:84&BBOX={bbox}&WIDTH=800&HEIGHT=600&FORMAT=image/png"
        if time:
            wms_request_url += f"&time={time}"
        if 'GetLegendGraphic' in base_url:
            logger.info("GetLegendGraphic URL detected, skipping WMS request.")
            return
        try:
            response = requests.head(wms_request_url)
        except Exception as e:
            logger.info(f"Request to {wms_request_url} failed with error {e}.")
            return
        if response.status_code != 200:
            logger.info(f"Request to {wms_request_url} failed with status code {response.status_code}.")
            return 

        if response.headers['Content-Type'] != 'image/png':
            logger.info(f"Request to {wms_request_url} returned content of type {response.headers['Content-Type']}, expected 'image/png'.")
            return
        
        logger.info(f"Request to {wms_request_url} was successful and returned an image.")
        self.layer.metadata['assets'].append(wms_request_url)
        return


# methods to update providers and sharing rights
    def update_providers_rights(self):
        self.layer.metadata['provider'] = {}
        self.layer.metadata['data_rights_restrictions'] = 'CC-BY-4.0'
        if self.layer.metadata['responsibleParties']:
            self.layer.metadata['provider'] = self.layer.metadata['responsibleParties']
            return
        elif 'geonetwork_uri' in self.layer.metadata:
            logger.info('getting providers from geonetwork')
            geonet_updater = GeonetworkUpdater(self.layer)
            if geonet_updater.get_geonetwork_providers():
                return
        
        self.layer.metadata['provider'] = [{'name': self.layer.metadata['thematic_lot'], 'role': 'provider'}]
        return

# methods to update the temporal extent, first try via ARCO assets, then geonetwork, then todays date
    def update_temporal_extent(self):
        
        metadata = self.layer.metadata
        metadata['temporal_extent'] = {'start': None, 'end': None}
        logger.info('updating temporal extent from first available: zarr, parquet, metagis, geonetwork')
        self.update_temporal_extent_from_netcdf_or_zarr()
            
        self.temporal_extent_from_parquet()
            
        self.temporal_extent_from_metagis()

        if 'geonetwork_uri' in metadata:
            logger.info('getting temporal extent from geonetwork')
            geonet_updater = GeonetworkUpdater(self.layer)
            geonet_updater.temporal_extent_from_geonetwork()
    
        self.set_begin_end_dates()
        self.finalize_temporal_extent()
        return

    def finalize_temporal_extent(self):
        for key in ['start', 'end']:
            if self.layer.metadata['temporal_extent'].get(key):
                try:
                    dt = pd.to_datetime(self.layer.metadata['temporal_extent'][key], utc=True)
                    self.layer.metadata['temporal_extent'][key] = dt.strftime("%Y-%m-%dT%H:%M:%SZ")
                except Exception as e:
                    logger.warning(f"Error converting {key} datetime: {e}")


    def set_begin_end_dates(self):
        temporal_extent = self.layer.metadata['temporal_extent']
        if temporal_extent['start'] is None:
            # if no start date found, set to 1900-01-01
            temporal_extent['start'] = datetime(1900, 1, 1, tzinfo=pytz.utc).isoformat()
            logger.warning(f"No start date found for ID {self.layerid} {self.layername}, set to 1900-01-01")

        if temporal_extent['end'] is None:
            # set to end of this year if no end date found
            temporal_extent['end'] = datetime(datetime.now().year, 12, 31, 23, 59, 59, tzinfo=pytz.utc).isoformat()
            logger.warning(f"No end date found for ID {self.layerid} {self.layername}, set to 2100-01-01")
        
        # make sure that the start date is before the end date
        if temporal_extent['start'] > temporal_extent['end']:
            logger.warning(f"Start date is after end date for {self.layerid} {self.layername}, swapping dates.")
            temporal_extent['start'], temporal_extent['end'] = temporal_extent['end'], temporal_extent['start']
        
        self.layer.metadata['temporal_extent'] = temporal_extent
        return 


    def update_temporal_extent_from_netcdf_or_zarr(self):
        temporal_extent = self.layer.metadata['temporal_extent']
        potential_assets = set(self.layer.metadata.get('assets', []))

        for asset in potential_assets:
            try:
                ds = None
                if asset.endswith('.nc') and 'https://s3' in asset:
                    logger.info('netcdf asset found on s3, using for temporal extent')
                    asset = asset + '#mode=bytes'
                    ds = xr.open_dataset(asset, engine='netcdf4')
                elif asset.endswith('.zarr') or asset.endswith('.zarr/'):
                    logger.info('zarr asset found, using for temporal extent')
                    ds = xr.open_dataset(asset, engine='zarr')
            except Exception as e:
                logger.info(f"Error opening asset {asset}: {e}")
                continue
            if ds:
                if 'time' in ds.dims:
                    start_datetime = pd.to_datetime(ds['time'].values[0])
                    end_datetime = pd.to_datetime(ds['time'].values[-1])
                    temporal_extent['start'] = start_datetime.isoformat()
                    temporal_extent['end'] = end_datetime.isoformat()
                logger.info(f"geographic extent updated from {asset}")
        return
    

    def temporal_extent_from_parquet(self):
        temporal_extent = self.layer.metadata['temporal_extent']
        if temporal_extent['start'] is not None and temporal_extent['end'] is not None:
            return
        potential_assets = set(self.layer.metadata.get('assets', []))
        if 'converted_arco_assets' in self.layer.metadata:
            potential_assets.update(self.layer.metadata['converted_arco_assets'])
        for asset in potential_assets:
            if asset.endswith('.parquet'):

                try:
                    df = S3Utils.read_parquet_from_s3(asset)
                except Exception as e:
                    logger.info(f"Error reading parquet file {asset}: {e}, trying geoparquet")
                    try:
                        df = S3Utils.read_geoparquet_s3(asset)
                    except Exception as e:
                        logger.info(f"Error reading geoparquet file {asset}: {e}")
                        continue

                time_columns = [col for col in df.columns if pd.api.types.is_datetime64_any_dtype(df[col])]
                if time_columns:
                    start_datetime = df[time_columns[0]].min().compute()
                    end_datetime = df[time_columns[0]].max().compute()
                    temporal_extent['start'] = start_datetime.strftime("%Y-%m-%dT%H:%M:%S")
                    temporal_extent['end'] = end_datetime.strftime("%Y-%m-%dT%H:%M:%S")
                    logger.info(f"temporal extent updated from parquet {asset}")
        return 

    def temporal_extent_from_metagis(self):
        temporal_extent = self.layer.metadata['temporal_extent']
        if temporal_extent['start'] is not None and temporal_extent['end'] is not None:
            return
        temporal_extent_str = self.layer.metadata['temporalDatesExtent']
        if temporal_extent_str is None:
            return
        try:
            # Check if the string is a single date
            temporal_extent = self.check_single_date(temporal_extent_str, temporal_extent)
            if temporal_extent['end'] is not None:
                logger.info(f"end date found in metagis single date {self.layerid} {self.layername}")
                return
        except Exception as e:
            logger.info(f"Error {e} parsing dates from Layer")
        
        try:
            # Identify the primary separator by counting occurrences
            separator_counts = {sep: temporal_extent_str.count(sep) for sep in [',', '/', '-']}
            # Filter for separators that occur exactly once
            separators_once = {sep: count for sep, count in separator_counts.items() if count == 1}
            # If multiple separators occur exactly once, prioritize them
            for preferred_separator in [',', '/', '-']:
                if preferred_separator in separators_once:
                    primary_separator = preferred_separator
                    break
            else:
                # Handle the case where no preferred separator is found
                logger.info("Unexpected condition: No preferred separator found among those occurring exactly once.")
                return
            # Split the string using the primary separator
            dates = temporal_extent_str.split(primary_separator)
            if len(dates) > 2:
                logger.info(f"Unexpected condition: {len(dates)} dates found in temporal extent string.")
                sorted_dates = sorted(dates)
                temporal_extent['start'] = pd.to_datetime(sorted_dates[0].strip()).isoformat()
                temporal_extent['end'] = pd.to_datetime(sorted_dates[-1].strip()).isoformat()
                logger.info(f"temporal extent updated from metagis {self.layerid} {self.layername}")
                return
            
            start_date, end_date = dates
            temporal_extent['start'] = pd.to_datetime(start_date.strip()).isoformat()
            temporal_extent['end'] = pd.to_datetime(end_date.strip()).isoformat()
            logger.info(f"temporal extent updated from metagis {self.layerid} {self.layername}")
        except Exception as e:
            logger.info(f"Error {e} parsing dates from Layer")
        return 

    def check_single_date(self, date_str, temporal_extent):
        """
        Check if the string is a single date in any common format (year, month, or full date).
        If so, update the temporal_extent dictionary.
        """
        # List of common date formats to try
        date_formats = [
            '%Y-%m-%d',       # ISO format: 2023-12-31
            '%d-%m-%Y',       # Day-Month-Year: 31-12-2023
            '%m-%d-%Y',       # Month-Day-Year: 12-31-2023
            '%Y/%m/%d',       # ISO with slashes: 2023/12/31
            '%d/%m/%Y',       # Day/Month/Year: 31/12/2023
            '%m/%d/%Y',       # Month/Day/Year: 12/31/2023
            '%Y.%m.%d',       # Dotted format: 2023.12.31
            '%d.%m.%Y',       # Day.Month.Year: 31.12.2023
            '%Y-%m',          # Year-Month: 2023-05
            '%Y/%m',          # Year/Month: 2023/05
            '%B %Y',          # Full month name and year: May 2023
            '%b %Y',          # Abbreviated month name and year: May 23
            '%Y',             # Year only: 2023
        ]
        
        single_date = None
        for fmt in date_formats:
            try:
                single_date = pd.to_datetime(date_str.strip(), format=fmt)
                break  # Stop as soon as a valid format is found
            except ValueError:
                continue  # Try the next format
        
        if single_date and single_date > datetime(1900, 1, 1):
            # If a valid single date is found, use it as the start date
            temporal_extent['start'] = None
            temporal_extent['end'] = single_date.isoformat()
            logger.info(f"using single date as valid end date for data from {self.layerid} {self.layername}")
            logger.info(f"Temporal extent updated from metagis (single date): {self.layerid} {self.layername}")
        else:
            logger.warning(f"Could not parse date: {date_str}")
        
        return temporal_extent

class GeonetworkUpdater:
    def __init__(self, layer):
        self.layer = layer
        self.layerid = layer.metadata['id']
        self.layername = layer.metadata['name']
        self.uri = layer.metadata['geonetwork_uri']
        self.xml_root, self.namespaces = self.get_xml_root_namespaces()
        self.csw_root = self.get_csw_root()


    def get_xml_root_namespaces(self):
        geonetwork_uri = self.layer.metadata['geonetwork_uri']
        xml_url = f'https://emodnet.ec.europa.eu/geonetwork/srv/api/records/{geonetwork_uri}/formatters/xml'
        try:
            xml_response = requests.get(xml_url)
        except Exception as e:
            logger.error(f"Failed to fetch XML from {xml_url}: {e}")
            return None, None
        if xml_response.status_code == 200:
            try:
                namespaces = dict([node for _, node in ET.iterparse(StringIO(xml_response.text), events=['start-ns'])])
                xml_root = ET.fromstring(xml_response.text)
                return xml_root, namespaces
            except Exception as e:
                logger.error(f"Failed to get namespaces and xml root: {e} from {self.layerid} {self.layername}")
                return None, None
        else:
            logger.warning(f"Failed to fetch XML from {xml_url}.")
            return None, None
       
    
    def get_csw_root(self):
        geonetwork_uri = self.layer.metadata['geonetwork_uri']
        csw_url = f'https://emodnet.ec.europa.eu/geonetwork/emodnet/eng/csw?request=GetRecordById&service=CSW&version=2.0.2&elementSetName=full&id={geonetwork_uri}'
        try:
            csw_response = requests.get(csw_url)
        except Exception as e:
            logger.error(f"Failed to fetch CSW from {csw_url}: {e}")
            return None
        if csw_response.status_code == 200:
            csw_root = ET.fromstring(csw_response.text)
            csw_root = csw_root
            return csw_root
        else:
            logger.warning(f"Failed to fetch CSW from {csw_url}.")
            return None
    
    def extract_csw_assets(self, csw_root):
        logger.info('finding assets from geonetwork CSW')
        def _extract_doi_assets(external_links):
            doi_link = external_links.get('Digital Object Identifier (DOI)')
            doi_value = None
            if doi_link:
                doi_value = doi_link['link'].split('/')[-2:]
                if len(doi_value) > 1:
                    doi_value = '/'.join(doi_value)
            if doi_value:
                self.layer.metadata['sci:doi'] = doi_value
            return
        try:
            external_links = {}
            for elem in csw_root.iter():
                if elem.tag.endswith('URI') and elem.text:
                    protocol = elem.attrib.get('protocol', 'HTTP')
                    name = elem.attrib.get('description', f"{self.layername} data product")
                    link = elem.text.strip()
                    
                    external_links[name] = {
                        'link': link,
                        'protocol': protocol
                    }
            
            if external_links:
                _extract_doi_assets(external_links)
                for link in external_links:
                    self.layer.metadata['assets'].append(external_links[link]['link'])
        except Exception as e:
            logger.warning(f"Error fetching CSW assets: {e}")

        return

    def get_csw_assets(self):
        logger.info(f"Getting assets from geonetwork CSW for {self.layerid} {self.layername}")
        
        if self.csw_root is not None:
            self.extract_csw_assets(self.csw_root)
            
        else:
            logger.warning("No CSW root found.")
        

    def get_geographic_extent(self):
        
        if self.xml_root is None or self.namespaces is None:
            logger.error(f"Failed to get xml root and namespaces for {self.layerid} {self.layername} {self.uri}")
            return False
        
        geographical_element = self.xml_root.find(".//gmd:geographicElement/gmd:EX_GeographicBoundingBox", namespaces=self.namespaces)
            
        if geographical_element is not None:
            min_lon = float(geographical_element.find(".//gmd:westBoundLongitude/gco:Decimal", namespaces=self.namespaces).text)
            min_lat = float(geographical_element.find(".//gmd:southBoundLatitude/gco:Decimal", namespaces=self.namespaces).text)
            max_lon = float(geographical_element.find(".//gmd:eastBoundLongitude/gco:Decimal", namespaces=self.namespaces).text)
            max_lat = float(geographical_element.find(".//gmd:northBoundLatitude/gco:Decimal", namespaces=self.namespaces).text)

            self.layer.metadata['geographic_extent'] = [min_lon, min_lat, max_lon, max_lat]
            return True
        else:
            logger.warning("Geographic element not found in XML.")
            return False
    
    def temporal_extent_from_geonetwork(self):
        self.temporal_extent = self.layer.metadata['temporal_extent']
        if self.temporal_extent['start'] is not None and self.temporal_extent['end'] is not None:
            return
        self.try_read_temporal_element_from_xml()
        if not self.temporal_extent.get('start') or not self.temporal_extent.get('end'):
            logger.warning('Failed to read temporal extent from geonetwork XML')
            
        self.fill_dates_and_format()
        return

    def try_read_temporal_element_from_xml(self):
        
        if self.xml_root is None or self.namespaces is None:
            logger.error(f"Failed to get xml root and namespaces for {self.layerid} {self.layername} {self.uri}")
            return
        gml_prefixes = [prefix for prefix, uri in self.namespaces.items() if 'gml' in uri]

        for gml_prefix in gml_prefixes:
            gml_namespace = self.namespaces[gml_prefix]
            time_period_xpath = f".//{{{gml_namespace}}}TimePeriod"
            time_period = self.xml_root.find(time_period_xpath, namespaces=self.namespaces)
            logger.info(f"{gml_prefix} Time period found: {time_period}")
            if time_period is not None:
                logger.info('try to find xpath begin and end position')
                begin_pos = time_period.find("gml:beginPosition", namespaces=self.namespaces)
                end_pos = time_period.find("gml:endPosition", namespaces=self.namespaces)

                for pos, temp in zip([begin_pos, end_pos], ['start', 'end']):
                    if pos is not None and pos.text is not None:
                        self.temporal_extent[temp] = pos.text
                        logger.info(f"{temp} position found: {pos.text}")
                    else:
                        logger.warning(f"No {temp} date found for {self.layerid} {self.layername} {self.uri}")
                        self.temporal_extent[temp] = None
                
                if self.temporal_extent.get('end') is None and self.temporal_extent.get('start') is not None:
                    logger.warning(f"No end date found for {self.layerid} {self.layername} {self.uri}")
                    self.temporal_extent['end'] = None
        return
    
    def fill_dates_and_format(self):
        logger.info('Filling dates and formatting')
        for key in ['start', 'end']:
            if self.temporal_extent.get(key):
                if len(self.temporal_extent[key]) == 4:
                    self.temporal_extent[key] += "-01-01T00:00:00"
                elif len(self.temporal_extent[key]) == 7:
                    self.temporal_extent[key] += "-01T00:00:00"
                elif len(self.temporal_extent[key]) == 10:
                    self.temporal_extent[key] += "T00:00:00"
                
                logger.info('formatting dates')
                formatted_value = self.format_datetimes(self.temporal_extent[key])
                if formatted_value:
                    self.temporal_extent[key] = formatted_value
                else:
                    logger.warning(f"Unable to format datetime {self.temporal_extent[key]} for {key}"
                                   f"set to None, will be set to today")
                    self.temporal_extent[key] = None

    def format_datetimes(self, value):
        # Remove milliseconds if present
        value = value.split('.')[0]
        
        try:
            # Try parsing the datetime string with timezone information
            logger.info(f"Trying to parse datetime with timezone: {value}")
            dt_obj = datetime.fromisoformat(value)
        except ValueError:
            try:
                # If parsing with timezone fails, assume the datetime is in local time and parse it
                logger.info(f"Trying to parse datetime without timezone: {value}")
                dt_obj = datetime.strptime(value, "%Y-%m-%dT%H:%M:%S")
                # Convert the datetime object to UTC
                dt_obj = dt_obj.replace(tzinfo=timezone.utc)
            except ValueError:

                logger.warning(f"Unable to parse and convert datetime {value}")
                return None
        
        # Convert the datetime object to UTC if it has timezone info
        logger.info(f"Converting datetime to UTC: {dt_obj}")
        dt_obj_utc = dt_obj.astimezone(timezone.utc)
        # Format the datetime string with 'Z' to indicate UTC
        logger.info(f"formatted datetime: {dt_obj_utc.strftime('%Y-%m-%dT%H:%M:%SZ')}")
        return dt_obj_utc.strftime("%Y-%m-%dT%H:%M:%SZ")

    def get_geonetwork_providers(self):
        
        if self.xml_root is None or self.namespaces is None:
            logger.error(f"Failed to get xml root and namespaces for {self.layerid} {self.layername} {self.uri}")
            return False
        
        responsible_party = self.xml_root.find('.//gmd:CI_ResponsibleParty', namespaces=self.namespaces)
        if responsible_party is not None:
            organisation_name = responsible_party.find('.//gmd:organisationName/gco:CharacterString', namespaces=self.namespaces)

            if organisation_name.text is not None:
                self.layer.metadata['provider'] = []
                provider_dict = {}
                provider_dict['name'] = organisation_name.text
                provider_dict['role'] = 'provider'
                self.layer.metadata['provider'].append(provider_dict)
        # Data Rights and Constraints
        
        constraints_element = self.xml_root.find(
            ".//gmd:otherConstraints/gco:CharacterString", namespaces=self.namespaces)
        if constraints_element is not None:
            constraints = constraints_element.text
            self.layer.metadata['data_rights_restrictions'] = constraints
            logger.info(f"Data rights constraints from geonetwork: {constraints}")
        # if nothing found in XML, set defaults
        if not self.layer.metadata['provider']:
            self.layer.metadata['provider'] = [{
                'name': self.layer.metadata['thematic_lot'],
                'role': 'provider'
            }]
            logger.info('No provider found in geonetwork metadata, using thematic lot as provider')
        
        return True

class LayerNativeAsset:
    """
    Handles the transfer of native assets for a layer in the Metagis catalog.
    
    This includes checking if the layer's native assets have already been transferred
    in a previous state or can be reused from another layer with the same download URL,
    downloading new native products if necessary, and uploading them to S3.
    """
    
    def __init__(self, layer: ('Layer'), layer_catalog_dict: dict):
        """
        Initializes the NativeTransfer instance.
        
        :param layer: The layer object containing metadata and other information.
        :param metagiscatalog: The Metagis catalog object.
        :param s3_upload_dir: The S3 directory where assets will be uploaded. Defaults to 'emodnet_native_test'.
        """
        self.layer = layer
        self.layer_catalog_dict = layer_catalog_dict
        self.metadata = layer.metadata
        self.layerid = layer.metadata['id']
        self.layername = layer.metadata['name']
        self.pipeline_config = layer.pipeline_config
        
        self.temp_dir = self.layer.temp_assets
        os.makedirs(self.temp_dir, exist_ok=True)


    def handle_native_transfer(self):
        """
        Main method to transfer a valid native asset. Confirm native transfer, check for native asset in previous state,
        check for native asset in current state, check for subsettable ARCO asset, lastly, transfer to S3.
        
        :return: The layer object with updated metadata if the asset is found or downloaded.
        :rtype: Layer
        """
        if 'download_url' not in self.layer.metadata:
            logger.error(f'no download url for {self.layerid} {self.layername}')
            return
        download_url = self.layer.metadata['download_url']

        # check if the native asset has already been transferred in a previous state
        if self.check_previous_layer_catalog() is not None:
            return self.layer
        # or can be reused from another layer with the same download URL
        if self.check_current_layer_catalog() is not None:
            return self.layer
        # confirm flag in pipeline config is set to True
        if not self.confirm_native_transfer(download_url):
            return self.layer
        
        # check if the download url is a subsettable ARCO asset
        if self.check_for_subsettable_arco_asset(download_url) is not None:
            return self.layer
        # if not found, download and upload the native asset to S3
        self.transfer_to_s3()
        return self.layer


    def check_for_subsettable_arco_asset(self, download_url: str):
        """
        Checks if the download url is a subsettable ARCO asset, used as native asset if found.

        :param download_url: The download URL to check.
        :type download_url: str
        :return: The layer object with updated metadata if the asset is found.
        :rtype: Layer
        """

        if 'https://s3.' in download_url:
            logger.info('testing s3 url for subset')
            if S3Utils.is_subsettable_arco_asset(download_url):
                self.metadata['native_asset'] = download_url
                self.metadata['assets'].append(self.metadata['native_asset'])
                logger.info(f"ARCO subsettable asset {self.metadata['native_asset']} found, added to Layer")
                return self.layer

    def check_previous_layer_catalog(self):
        """
        Check if there is a native asset in the previous state.
        
        return: The layer object with updated metadata if the asset is found.
        rtype: Layer
        """
        # see if this layer is in a previous state
        if 'previous_layer' in self.layer.metadata:
            previous_layer = self.layer.metadata['previous_layer']
            # check if there is a native asset in the previous state, it will be on s3, 
            # if the download urls match use the native asset
            if previous_layer and 'native_asset' in previous_layer:
                previous_download_url = previous_layer.get('download_url')
                current_download_url = self.layer.metadata.get('download_url')
                if previous_download_url == current_download_url:
                    self.layer.metadata['native_asset'] = previous_layer['native_asset']
                    self.layer.metadata['assets'].append(self.layer.metadata['native_asset'])
                    logger.info(f"native asset {previous_layer['native_asset']} found in previous state and added to Layer")
                    return self.layer
                else:
                    logger.info(f"download urls do not match for {self.layerid} {self.layername}")
                    return None
        else:
            logger.info(f"no previous layer catalog")
            return None


    def check_current_layer_catalog(self):
        """
        Checks layers in current layer catalog, if a layer with the same download URL already transferred
        the native asset, uses that if found.

        :return: The layer object with updated metadata if the asset is found or downloaded.
        :rtype: Layer
        """
        # search current state for a layer that may share the same download url and use its native asset if its been transferred
        for id, targetlayer in self.layer_catalog_dict.items():
            if 'download_url' not in targetlayer:
                continue
            if targetlayer['download_url'] == self.layer.metadata['download_url']:
                if 'native_asset' in targetlayer:
                    self.layer.metadata['native_asset'] = targetlayer['native_asset']
                    self.layer.metadata['assets'].append(self.layer.metadata['native_asset'])
                    logger.info(f"native asset {targetlayer['native_asset']} found in current state and added to Layer")
                    return self.layer
        return None


    def transfer_to_s3(self):
        """
        Checks for download urls, downloads the native product, and uploads it to S3.
        
        """
        logger.info("finding valid download urls")
        urls = self._get_download_urls()
        if not urls:
            logger.error(f"No download URLs found for {self.layerid} {self.layername}")
            return
        logger.info(f"downloading one of {urls}")

        file_path = self._download_native_product(urls)
        if not file_path:
            logger.error(f"Failed to download native product for {self.layerid} {self.layername}")
            return
        
        logger.info(f"uploading native product {self.layerid} {self.layername}")
        self._upload_native_product(file_path)

        return

    def is_valid_download_type(self, download_url: str):
        """
        checks validity of download URL based on file type and patterns.

        :param download_url: URL to check.
        :type download_url: str
        :return: The URL if it's valid, or None otherwise.
        :rtype: str or None
        """
        valid_endings = ['.zip', '.nc', '.parquet', '.csv', '.parquet/', '.geoparquet/']
        valid_patterns = ['mda.vliz.be/download.php?file=VLIZ', 'parquet/', 'geoparquet/']

        if any(download_url.endswith(pattern) for pattern in valid_endings):
            return download_url
        if any(pattern in download_url for pattern in valid_patterns):
            return download_url


    def _get_download_urls(self):
        """
        Finds any valid download URLs from the layer metadata.
        
        :return: A list of valid download URLs.
        """
        urls = []
        if 'download_url' in self.metadata:
            if self.is_valid_download_type(self.metadata['download_url']):
                urls.append(self.metadata['download_url'])
            else:
                logger.error(f"Invalid download URL {self.metadata['download_url']}")
                logger.error(f"for {self.metadata['id']} {self.metadata['name']}")
        if 'assets' in self.metadata:
            for asset in self.metadata['assets']:
                if self.is_valid_download_type(asset):
                    urls.append(asset)
        return urls


    def _download_native_product(self, urls: list):
        """
        return first successful download from valid urls
        
        param urls: list of download urls
        :type urls: list
        :return: path to downloaded file
        :rtype: str
        """
        try:
            for url in urls:
                file_path = CoreUtils.download_asset(url, self.temp_dir)
                if file_path:
                    return file_path
                else:
                    return None
        except Exception as e:
            logger.warning(f"download failed {url} error {e}")
        return None


    def _upload_native_product(self, file_path: str):
        """
        uploads downloaded native product to S3. s3://emodnet/thematic_lot/id/product'
        
        :param file_path: The path to the native product file.
        :return: The updated metadata object with the new native asset URLs.
        """
        s3_dir = f"{CoreUtils.custom_slugify(self.layer.metadata['thematic_lot'])}/{self.layer.metadata['id']}"

        # pass in emodnet s3 object to upload to emodnet s3 bucket
        emodnet_s3 = EMODnetS3(self.pipeline_config)
        botoclient = emodnet_s3.botoclient
        bucket = emodnet_s3.bucket
        host = emodnet_s3.host
        s3_url = S3Utils.upload_to_s3(botoclient, bucket, host, file_path, s3_dir)

        if not s3_url:
            logging.error(f"Failed to upload {file_path} to S3")
            return
        self.metadata['native_asset'] = s3_url
        self.metadata['assets'].append(s3_url)
        logger.info(f"Upload {self.metadata['native_asset']} successful")
        return


    def confirm_native_transfer(self, download_url):
        """
        checks if native transfer flag is True or False in pipeline config. If False, test download url
        and use as native asset. If True, continue with transfer.
        
        :param download_url: The download URL for the layer.
        :type download_url: str
        :return: True if the transfer is flagged as True, False otherwise.
        rtype: bool
        """
        # check if native transfer is flagged as False in the pipeline config
        if 'transfer_native_to_s3' in self.pipeline_config and self.pipeline_config['transfer_native_to_s3'] == False:
            logger.info(f"Native transfer flagged as False, skipping transfer for {self.layerid} {self.layername}")
            logger.info(f"testing download_url {self.layer.metadata['download_url']}")
            status = requests.head(download_url).status_code
            if status == 200:
                self.metadata['native_asset'] = download_url
                self.metadata['assets'].append(self.metadata['native_asset'])
                logger.info(f"{download_url} being used for native asset")
                logger.info(f"{download_url} being used for native asset")
                return False
            elif self.check_for_subsettable_arco_asset(download_url) is not None:
                return False
            else:
                logger.error(f"Download url {download_url} not found")
                return False
        return True