import dask
from dask.distributed import progress
import geopandas as gpd
import dask.dataframe as dd
import fiona
import os
import logging
import pyarrow.parquet as pq
import pandas as pd
from typing import Any, Dict, List, TYPE_CHECKING

if TYPE_CHECKING:
    from metagisconfig2stac.layer import Layer
logger = logging.getLogger(__name__)

class ParquetToEDITOParquet:
    """
    Converts a parquet file to a new parquet file with standard names that are defined in the native_arco_mapping of the layer.
    Renames file to *_edito.parquet to differentiate between the original and the new file.

    :param layer: Layer object with data product needing conversion.
    :type layer: Layer
    :param parquet_path: Path to the parquet file.
    :type parquet_path: str

    :return new_parquet_path: Path to the new parquet file.
    :rtype str
    """
    def __init__(self, layer: 'Layer', parquet_path: str):
        self.layer = layer
        self.parquet_path = parquet_path
    
    def parquet_to_parquet(self, layer: ('Layer'), parquet_path):
        """
        Converts a parquet file to a new parquet file with the same schema but with a different name.
        """
        df = dd.read_parquet(parquet_path)

        logger.info(f"Reading parquet file {parquet_path} and renaming columns to standard names from native_arco_mapping.")
        for col in df.columns:
            if col in layer.metadata['native_arco_mapping']:
                df = df.rename(columns={col: layer.metadata['native_arco_mapping'][col]})
                logger.info(f"Column {col} renamed to {layer.metadata['native_arco_mapping'][col]}")
            elif col not in layer.metadata['native_arco_mapping']:
                logger.warning(f"Column {col} not found in native_arco_mapping. Skipping column.")

        logger.info(f"Writing new parquet file with standard column names to {parquet_path.replace('.parquet', '_edito.parquet')}")
        new_parquet_path = parquet_path.replace('.parquet', '_edito.parquet')
        df.to_parquet(new_parquet_path, engine='pyarrow')
        logger.info(f"New parquet file written to {new_parquet_path}")
        return new_parquet_path