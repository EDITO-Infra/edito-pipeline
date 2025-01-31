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
    from metagisconfig2stac.layer import Layer
logger = logging.getLogger(__name__)



class TifToZarr:
    """
    Converts Geotiff files to zarr format.

    :param: layer: Layer object with data product needing conversion.
    :type: Layer
    :param: native_product_path: Path to the Geotiff file.
    :type: str
    
    :return: zarr_path: Path to the zarr dataset.
    :rtype: str
    """
    def __init__(self, layer: 'Layer', native_product_path: str):
        self.layer = layer
        self.metadata = layer.metadata
        self.native_product_path = native_product_path
        self.converted_arco = self.layer.converted_arco

    def convert_tiff_to_zarr(self):
        """
        Converts a Geotiff file to Zarr format.

        This method opens a TIFF file specified by the native_product_path attribute using rasterio, reads the data, and applies
        variable renaming based on the metadata's 'native_arco_mapping'. It then converts the data to an xarray Dataset, ensuring
        geospatial coordinates are correctly handled. Returns path to zarr dataset

        :return: zarr_path: str
        """
        arco_native_dict = self.metadata['native_arco_mapping']
        native_var = list(arco_native_dict.keys())[0]
        arco_var = arco_native_dict[native_var]
        
        with rasterio.open(self.native_product_path) as src:
            band_count = src.count
            resolution = src.res
            band_descriptions = src.descriptions
        chunk_size = 200

        title = os.path.splitext(os.path.basename(self.native_product_path))[0]
        da = rxr.open_rasterio(self.native_product_path, chunks=chunk_size)
        da.name = arco_var
        dataset = da.to_dataset().rename({'x': 'longitude', 'y': 'latitude'}).sortby('latitude')
        dataset = dataset.chunk({'latitude': chunk_size, 'longitude': chunk_size})
        dataset[arco_var] = dataset[arco_var].chunk({'latitude': chunk_size, 'longitude': chunk_size})

        dataset.attrs.update({
            'band_count': band_count,
            'band_descriptions': band_descriptions,
            'variables': list(dataset.data_vars),
        })

        dataset = ArcoConvUtils._update_zarr_attributes(self.layer, dataset, self.native_product_path)
        zarr_path = os.path.join(self.converted_arco, f"{arco_var}.zarr")
        with dask.config.set(scheduler='threads'):
            dataset.to_zarr(zarr_path, mode='w')

        return zarr_path