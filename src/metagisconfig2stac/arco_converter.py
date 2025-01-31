import os
import logging
from zipfile import ZipFile
from typing import Any, Dict, List, TYPE_CHECKING
import fiona
if TYPE_CHECKING:
    from .layer import Layer

from .utils.core import CoreUtils
from .utils.s3 import S3Utils
from .arco_conversion import ArcoConvUtils
logger = logging.getLogger(__name__)


class ArcoConversionManager:
    """
    Handles the conversion of the native asset of a given layer to ARCO formats.

    This class provides methods to manage the conversion process, 
    including checking for necessary conversion attributes, downloading or retrieving the native asset, 
    processing the downloaded product, and converting it to various ARCO formats such as Zarr, Parquet, and GeoParquet. 
    It also handles the uploading of converted assets to S3 and updating the layer's metadata with the new ARCO assets.

    :param layer: The layer object containing metadata and other information.
    :type layer: dict
    :param layer_catalog: The layer catalog object to interact with.
    :type layer_catalog: layercatalog
    """
    def __init__(self, pipeline_config: dict):
        """

        param layer (dict): The layer object containing metadata and other information.
        param layercatalog (layercatalog): The layer catalog object to interact with.

        """
        self.pipeline_config = pipeline_config
        self.layer = None

    def confirm_arco_conversion(self):
        """
        :return: bool: True if the layer's native asset should be converted to ARCO formats, False otherwise.
        :rtype: bool
        """
        return (
            self.pipeline_config.get('convert_arco', True) and
            'attributes' in self.layer.metadata
        )

    def manage_arco_conversion(self, layer: 'Layer'):
        """
        Stepwise process to convert the native asset of a layer to ARCO formats. 
        -Confirm if native product should be converted
        -add native_arco_mapping using layer attributes
        -Check previous state for converted arco asset
        -If native asset is subsettable ARCO data, convert to EDITO form
        -Download or retrieve the native asset
        -Process the downloaded product
        -Select the conversion method based on the product type
        """
        self.layer = layer
        self.layerid = self.layer.metadata['id']
        self.layername = self.layer.metadata['name']
        if not self.confirm_arco_conversion():
            logger.info(f"Skipping ARCO conversion for {self.layerid} {self.layername}")
            return
        
        logger.info(f"Adding native ARCO mapping for {self.layerid}")
        self.layer = ArcoConvUtils._add_arco_mapping(self.layer)

        if self._check_previous_state_arco_assets():
            logger.info(f"ARCO assets already exist for {self.layerid}")
            return

        # check if the native asset is a subsettable ARCO asset
        if S3Utils.is_subsettable_arco_asset(self.layer.metadata['native_asset']):
            s3url= self._convert_subsettable_arco_asset(self.layer.metadata['native_asset'])
            if s3url:
                logger.info(f"Converted subsettable ARCO asset {self.layer.metadata['native_asset']} to EDITO form")
                return
        
        # download or find the native asset in temp assets
        downloaded_asset_path = self._download_or_get_asset()
        if not downloaded_asset_path:
            logger.error(f"No asset for local ARCO conversion: {self.layerid}")
            return

        native_product_path = self._process_downloaded_product(downloaded_asset_path)

        self._select_conversion(native_product_path)
        logger.info(f"ARCO conversion completed for {self.layerid}")
        return layer

    def _check_previous_state_arco_assets(self):
        """
        :return: bool: True if previous state of this layer is equal and has converted arco assets.
        :rtype: bool
        """
        previous_layer = self.layer.metadata.get('previous_layer', {})
        if not previous_layer.get('converted_arco_assets'):
            return False

        if (previous_layer.get('native_asset') == self.layer.metadata['native_asset'] and
            previous_layer.get('native_arco_mapping') == self.layer.metadata['native_arco_mapping']):
            self.layer.metadata['converted_arco_assets'].extend(
                asset for asset in previous_layer['converted_arco_assets']
                if any(t in asset for t in ['.zarr', '.parquet'])
            )
            return True
        return False


    def _download_or_get_asset(self):
        """
        retrieves the native asset from the layer's metadata or downloads it if it is not found in the temp assets directory.

        :return: str: The path to the downloaded or retrieved native asset.
        :rtype: str
        """
        native_asset_name = os.path.basename(self.layer.metadata['native_asset'])
        if native_asset_name in os.listdir(self.layer.temp_assets):
            logger.info(f"found in temp assets: {native_asset_name}")
            return os.path.join(self.layer.temp_assets, native_asset_name)

        return CoreUtils.download_asset(self.layer.metadata['native_asset'], self.layer.temp_assets)


    def _process_downloaded_product(self, downloaded_product):
        """
        Gets the data product needed for conversion. Extracts zip files and searches for data product ('edito_info')
        
        :param downloaded_product: The path to the downloaded product.
        :type downloaded_product: str
        :return: Path to the target product or None if not found.
        :rtype: str
        """
        file_types= ['.nc', '.shp', '.gdb', '.tif', '.csv', '.parquet', '.zarr']
        patterns = ['.zarr/', '.parquet/']
        file_type = os.path.splitext(downloaded_product)[1]
        
        if file_type in file_types:
            logger.info(f"Processing {downloaded_product} as {file_type} file.")
            return downloaded_product

        if file_type == '.zip':
            return self._process_zip_file(downloaded_product)

        if any(p in downloaded_product for p in patterns):
            logger.info(f"Processing {downloaded_product} as {file_type} file.")
            return downloaded_product
        
        logger.error(f"Unsupported file type for conversion: {file_type}")
        return None


    def _process_zip_file(self, zip_file):
        """
        Extracts and processes a zip file to determine the target product for conversion.
        
        :param zip_file: The path to the zip file.
        :type zip_file: str
        :return: Path to the target product or None if not found.
        :rtype: str
        """
        extracted_products = self.extract_zip_find_products(zip_file)

        if not extracted_products:
            logger.error(f"No suitable data products found in the zip file: {zip_file}")
            return None

        target_product = self._determine_target_product(extracted_products)
        if not target_product:
            logger.error("Target product not specified or not found in extracted products.")
            return None

        return target_product


    def _determine_target_product(self, extracted_products):
        """
        Determines the target product for conversion from the extracted products.
        Handles geodatabase layers by splitting the metadata if needed.

        :param extracted_products: List of tuples (path, basename) of extracted products.
        :type extracted_products: list
        :return: Path to the target product or None if not found.
        :rtype: str
        """
        native_product = self.layer.metadata.get('native_data_product')
        if not native_product:
            logger.error("Target product not specified in the layer metadata.")
            return None

        if '.gdb_' in native_product:
            geodatabase, gdb_layer = native_product.split('.gdb_')
            self.layer.metadata['native_data_product'] = f"{geodatabase}.gdb"
            self.layer.metadata['gdb_layer'] = gdb_layer
            native_product = f"{geodatabase}.gdb"

        for product_path, product_basename in extracted_products:
            if native_product == product_basename:
                logger.info(f"Found target product: {product_basename}")
                return product_path

        logger.error(f"Target product '{native_product}' not found in extracted data products.")
        return None


    def extract_zip_find_products(self, zip_file):
        """
        Extracts the contents of a zip file and identifies possible data products for conversion.

        :param zip_file: The path to the zip file.
        :type zip_file: str
        :return: List of tuples (path, basename) of extracted products.
        :rtype: list
        """
        extract_dir = os.path.join(self.layer.temp_assets, "extracted")
        os.makedirs(extract_dir, exist_ok=True)

        with ZipFile(zip_file) as z:
            z.extractall(extract_dir)
            logger.info(f"Extracted files to {extract_dir} from {zip_file}")

        return self._find_convertible_products(extract_dir)


    def _find_convertible_products(self, directory):
        """
        Searches a directory for files and directories that are suitable data products.

        :param directory: Directory to search for data products.
        :type directory: str
        :return: List of tuples (path, basename) of suitable data products.
        :rtype: list
        """
        file_types = ['.shp', '.gdb', '.tif', '.csv', '.nc', '.parquet']
        products = []

        for root, dirs, files in os.walk(directory):
            # Add directories ending with .gdb
            products.extend((os.path.join(root, d), d) for d in dirs if d.endswith('.gdb'))
            # Add files with specified extensions
            products.extend((os.path.join(root, f), f) for f in files if any(f.endswith(ft) for ft in file_types))

        logger.info(f"Found {len(products)} possible data products in {directory}.")
        return products


    def _select_conversion(self, native_product_path):
        """
        Converts selected product to ARCO formats.
        :param native_product_path: str: The path to the native product for conversion.
        :type native_product_path: str
        :return: bool: True if the conversion was successful, False otherwise.
        rtype: bool
        """
       
        file_type = os.path.splitext(os.path.basename(native_product_path))[1]
        logger.info(file_type)
        
        converters = {
            '.nc': self.transform_nc_to_zarr,
            '.gdb': self.transform_gdb_to_parquet_zarr,
            '.shp': self.transform_shp_to_parquet_zarr,
            '.tif': self.transform_tif_to_zarr,
            '.csv': self.transform_csv_to_parquet_zarr,
            }
        if file_type in converters:
            try:
                converters[file_type](native_product_path)
            except Exception as e:
                logger.error(f"Error {converters[file_type]} {native_product_path}: {e}")
        else:
            logger.error(f"Unsupported file type for {native_product_path}")
        
        CoreUtils.empty_directory(self.layer.temp_assets)
        return


    def _convert_and_upload(self, converter_class, convert_method, file_path, file_type):
        """
        Helper function to convert a file and upload it to S3.

        :param converter_class: Class used for conversion.
        :param convert_method: Method used for the specific conversion.
        :param file_path: Path of the input file.
        :param file_type: Type of file being processed (e.g., 'netcdf', 'tiff').
        """
        logger.info(f"Converting {file_type} {file_path}")
        converter = converter_class(self.layer, file_path)
        output_paths = convert_method(converter)

        if not output_paths:
            logger.error(f"Error converting {file_path} to target format(s)")
            return

        for path in output_paths if isinstance(output_paths, (list, tuple)) else [output_paths]:
            if ArcoConvUtils.upload_arco_to_s3_update_assets(self.layer, path) is None:
                logger.error(f"Error uploading {path} to S3")
                return
            
        logger.info(f"Conversion and upload for {file_path} completed")
        return


    def transform_nc_to_zarr(self, native_product_path):
        """
        :param native_product_path: str: The path to the native product to convert.
        :type native_product_path: str
        
        :return: bool: True if the conversion was successful, False otherwise.
        :rtype: bool
        """
        from .arco_conversion.nc_to_zarr import NetCDFToZarr
        return self._convert_and_upload(
            NetCDFToZarr,
            lambda converter: converter.netcdf_to_zarr(),
            native_product_path,
            "netcdf"
        )

    def transform_tif_to_zarr(self, native_product_path):
        from .arco_conversion.tif_to_zarr import TifToZarr
        return self._convert_and_upload(
            TifToZarr,
            lambda converter: converter.convert_tiff_to_zarr(),
            native_product_path,
            "tiff"
        )

    def transform_csv_to_parquet_zarr(self, native_product_path):
        from .arco_conversion.csv_to_parquet import CsvToParquet
        from .arco_conversion.parquet_to_zarr import ParquetToZarr
        
        csv_to_parquet = CsvToParquet(self.layer, native_product_path)
        parquet_paths = csv_to_parquet.csv_to_parquet()
        if not parquet_paths:
            logger.error(f"Error converting {native_product_path} to parquet/geoparquet")
            return
        
        for path in parquet_paths:
            if ArcoConvUtils.upload_arco_to_s3_update_assets(self.layer, path) is None:
                logger.error(f"Error uploading {path} to S3")
                return
        
        geoparquet_path = parquet_paths[1]  # Assume second path is geoparquet
        return self._convert_and_upload(
            ParquetToZarr,
            lambda converter: converter.parquet_to_zarr(geoparquet_path),
            geoparquet_path,
            "geoparquet"
        )

    def transform_shp_to_parquet_zarr(self, native_product_path):
        from .arco_conversion.shp_to_parquet import ShpToParquet
        from .arco_conversion.parquet_to_zarr import ParquetToZarr
        logger.info(f"Converting {native_product_path} to parquet and geoparquet")
        shp_to_parquet = ShpToParquet(self.layer, native_product_path, shplayer=0)
        parquet_paths = shp_to_parquet.shp_to_parquet()
        if not parquet_paths:
            logger.error(f"Error converting {native_product_path} to parquet/geoparquet")
            return
        logger.info(f"Converted to parquet and geoparquet, syncing to S3")
        for path in parquet_paths:
            if ArcoConvUtils.upload_arco_to_s3_update_assets(self.layer, path) is None:
                logger.error(f"Error uploading {path} to S3")
                return
        logger.info(f"converting to zarr")
        geoparquet_path = parquet_paths[1]  # Assume second path is geoparquet
        return self._convert_and_upload(
            ParquetToZarr,
            lambda converter: converter.parquet_to_zarr(geoparquet_path),
            geoparquet_path,
            "geoparquet"
        )

    def transform_gdb_to_parquet_zarr(self, native_product_path):
        from .arco_conversion.gdb_to_parquet import GdbToParquet
        from .arco_conversion.parquet_to_zarr import ParquetToZarr
        
        gdblayer = self.layer.metadata.get('gdb_layer') or fiona.listlayers(native_product_path)[0]
        if not gdblayer:
            logger.error(f"No gdb layer specified for {native_product_path}")
            return False
        
        logger.info(f"Converting {native_product_path} to parquet and geoparquet")
        gdb_to_parquet = GdbToParquet(self.layer, native_product_path, gdblayer)
        parquet_paths = gdb_to_parquet.gdb_to_parquet()
        if not parquet_paths:
            logger.error(f"Error converting {native_product_path} to parquet/geoparquet")
            return
        
        logger.info(f"Converted to parquet and geoparquet, syncing to S3")
        for path in parquet_paths:
            if ArcoConvUtils.upload_arco_to_s3_update_assets(self.layer, path) is None:
                logger.error(f"Error uploading {path} to S3")
                return
            
        logger.info(f"converting to zarr")
        geoparquet_path = parquet_paths[1] 
        return self._convert_and_upload(
            ParquetToZarr,
            lambda converter: converter.parquet_to_zarr(geoparquet_path),
            geoparquet_path,
            "geoparquet"
        )

    def _convert_subsettable_arco_asset(self, asset):
        """
        Converts a subsettable ARCO asset to its EDITO form by renaming dataset variables.
        For zarr, parquet and geoparquet

        """
        if S3Utils.is_zarr_asset(asset):
            logger.info(f"Converting subsettable ARCO zarr asset {asset} to EDITO form, use s3 link as path")
            edito_zarr = ArcoConvUtils._rename_zarr_variables(self.layer, asset)
            s3url = ArcoConvUtils.upload_arco_to_s3_update_assets(self.layer, edito_zarr)
            
        
        if S3Utils.is_geoparquet_asset(asset):
            logger.info(f"Converting subsettable ARCO geoparquet asset {asset} to EDITO form, use s3 link as path")
            pqtype = 'geoparquet'
            s3url = ArcoConvUtils.rename_s3_parquet_columns_edito(self.layer, asset, pqtype)
            
        elif S3Utils.is_parquet_asset(asset):
            pqtype = 'parquet'
            logger.info(f"Converting subsettable ARCO parquet asset {asset} to EDITO form, use s3 link as path")
            s3url = ArcoConvUtils.rename_s3_parquet_columns_edito(self.layer, asset, pqtype)
        
        if not s3url:
            logger.error(f"Error converting subsettable ARCO asset {asset} to EDITO form")
            return None
        return s3url
        