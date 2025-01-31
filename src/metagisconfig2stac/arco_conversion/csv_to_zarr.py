import os
import logging
import pandas as pd
import xarray as xr
import dask.dataframe as dd
import dask.array as da
from typing import List, Dict, Any, TYPE_CHECKING

if TYPE_CHECKING:
    from metagisconfig2stac.layer import Layer
from .utils import ArcoConvUtils

logger = logging.getLogger(__name__)

class CsvToZarr:

    def __init__(self, layer: 'Layer', native_product_path: str):
        self.layer = layer
        self.metadata = layer.metadata
        self.native_product_path = native_product_path
        self.converted_arco = layer.converted_arco
        self.native_product = os.path.basename(self.native_product_path)

    def convert_raster_csv_to_zarr(self):
        """
        Converts a CSV file to Zarr format.

        This method reads a CSV file specified by the native_product_path attribute, using dask for chunked reading. It renames
        columns based on the metadata's 'native_arco_mapping', handles time conversion, and sets appropriate indices for geospatial
        data. The dataframe is then converted to an xarray Dataset and saved in Zarr format to the converted_arco. The
        dataset's attributes are updated, and the Zarr dataset is uploaded to S3.

        Returns:
        --------
        bool
            True if the conversion and upload were successful, False otherwise.
        """
        
        downloaded_product_path = self.native_product_path
        ddf = dd.read_csv(downloaded_product_path, chunksize=10000)

        ddf = ddf.rename(columns=self.metadata['native_arco_mapping'])

        if 'time' in ddf.columns:
            ddf['time'] = pd.to_datetime(ddf['time'])

        if 'depth' in ddf.columns and 'time' in ddf.columns:
            ds = xr.Dataset.from_dataframe(ddf.set_index(['latitude', 'longitude', 'depth', 'time']))

        elif 'time' in ddf.columns:
            ds = xr.Dataset.from_dataframe(ddf.set_index(['latitude', 'longitude', 'time']))

        else:
            ds = xr.Dataset.from_dataframe(ddf.set_index(['latitude', 'longitude']))
        ds = ArcoConvUtils._update_zarr_attributes(self.layer, ds, self.native_product_path)
        
        zarr_path = os.path.join(self.converted_arco, f"{self.native_product}.zarr")
        ds.to_zarr(zarr_path, mode='w', consolidated=True)
        logger.info(f"Zarr made at {zarr_path} from {self.native_product}")

        s3_url = ArcoConvUtils._upload_arco_to_s3_update_layer_assets(self.native_product, zarr_path)
        if s3_url:
            logger.info(f"ARCO conversion and Zarr uploaded completed")
            return True
        else:
            logger.error(f"ARCO conversion and Zarr upload failed")
            return False

