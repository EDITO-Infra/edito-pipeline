
import os
import logging
import pandas as pd
import geopandas as gpd
import fiona
import numpy as np
import rasterio
import dask.dataframe as dd
import dask.array as da
import dask
import rioxarray as rxr
import xarray as xr
from .utils import ArcoConvUtils
from typing import List, Dict, Any, TYPE_CHECKING

if TYPE_CHECKING:
    from layer import Layer

logger = logging.getLogger(__name__)

class NetCDFToZarr:
    """
    Convert a NetCDF file to Zarr format. Read the NetCDF file, apply variable renaming based on the layer metadata's 'native_arco_mapping',
    and convert geospatial coordinates if necessary. Save the converted dataset to Zarr format. Update the layer metadata with the temporal
    extent of the dataset and upload the Zarr dataset to S3.

    :param layer: Layer object with data product needing conversion.
    :type layer: Layer
    :param native_product_path: Path to the NetCDF file.
    :type native_product_path: str
    """
    def __init__(self, layer: 'Layer', native_product_path: str):
        
        self.layer = layer
        self.metadata = layer.metadata
        self.native_product_path = native_product_path
        self.converted_arco = layer.converted_arco

    def netcdf_to_zarr(self):
        """
        Converts a NetCDF file to Zarr format.

        This method reads a NetCDF file specified by the native_product_path attribute, applies variable renaming based on the
        metadata's 'native_arco_mapping', and converts geospatial coordinates if necessary. The converted dataset is then saved
        in Zarr format to the converted_arco. Additionally, it updates the metadata with the temporal extent of the dataset
        and uploads the Zarr dataset to S3.

        :return zarr_path: Path to the Zarr file.
        :rtype str
        """
        native_product_path = self.native_product_path
        layer = self.layer
        title = os.path.splitext(os.path.basename(native_product_path))[0]
        dataset = xr.open_dataset(native_product_path, chunks='auto')

        for native_attribute in layer.metadata['native_arco_mapping'].keys():
            if native_attribute not in dataset.data_vars:
                logger.error(f"metagis {layer.metadata['id']} native variable attribute {native_attribute} not in dataset")

        for nativevar, arcovar in layer.metadata['native_arco_mapping'].items():
            dataset = dataset.rename({nativevar: arcovar})
            logger.info(f"Renaming var {nativevar} to {arcovar}")

        if 'lat' in dataset.variables:
            dataset = dataset.rename({'lat': 'latitude'})
        if 'lon' in dataset.variables:
            dataset = dataset.rename({'lon': 'longitude'})

        if 'time' in dataset.dims or 'time' in dataset.coords:
            start_time = pd.to_datetime(dataset['time'].values[0])
            end_time = pd.to_datetime(dataset['time'].values[-1])
            layer.metadata['temporal_extent'] = {'start': start_time.strftime("%Y-%m-%d %H:%M:%S"), 'end': end_time.strftime("%Y-%m-%d %H:%M:%S")}
        
        zarr_path = os.path.join(layer.converted_arco, f"{title}.zarr")
        dataset.to_zarr(store=zarr_path, consolidated=True, mode='w')
        logger.info(f"Zarr made at {zarr_path} from {os.path.basename(native_product_path)}")
        return zarr_path
