
import dask
from dask.distributed import progress
import geopandas as gpd
import dask.dataframe as dd
import fiona
import os
import logging
import pyarrow.parquet as pq
import pandas as pd
import dask_geopandas as dgpd
from typing import Any, Dict, List, TYPE_CHECKING

if TYPE_CHECKING:
    from metagisconfig2stac.layer import Layer

logger = logging.getLogger(__name__)

class CsvToParquet:
    """
    Convert a CSV file to Parquet format or GeoParquet format.

    :param layer: Layer object with data product needing conversion.
    :type layer: Layer
    :param native_product_path: Path to the CSV file.
    :type native_product_path: str

    :return parquet_file, geoparquet_file: Path to the Apache Parquet file and Geoparquet file.
    :rtype tuple
    """
    def __init__(self, layer: 'Layer', native_product_path: str):
        
        self.layer = layer
        self.metadata = layer.metadata
        self.native_product_path = native_product_path
        self.converted_arco = layer.converted_arco
        self.native_product = os.path.basename(self.native_product_path)
        self.title = os.path.splitext(os.path.basename(self.native_product_path))[0]

    def csv_to_parquet(self, layer=None, ddfchunksize=100000):
        """
        Convert a CSV file to Apache Parquet format. Read the CSV file using Dask, and save the Parquet file.

        :param layer: Layer object with data product needing conversion.
        :type layer: Layer
        :param ddfchunksize: Size of the Dask DataFrame chunks.
        :type ddfchunksize: int

        :return parquet_file: Path to the Apache Parquet file.
        :rtype str
        """
        logger.info(f"Converting {self.native_product} to Parquet")
        pq = dd.read_csv(self.native_product_path, dtype_backend='pyarrow', assume_missing=True, blocksize=ddfchunksize)


        pq_path = os.path.join(self.converted_arco, f"{self.title}.parquet")
        os.makedirs(pq_path, exist_ok=True)
        pq = pq.to_parquet(pq_path, write_index=False, engine='pyarrow')
        progress(pq)
        if os.path.exists(pq_path):
            logger.info(f"Parquet made at {pq_path} from {self.native_product}")
        
        # map to geoparquet
        geopq_path = os.path.join(self.converted_arco, f"{self.title}_geoparquet.parquet")
        os.makedirs(geopq_path, exist_ok=True)

        self.map_to_geoparquet(pq_path, geopq_path)
        return pq_path, geopq_path

        
    def map_to_geoparquet(self, parquet, geopq_path):
        """
        Converts a Parquet file to GeoParquet format using Dask distributed processing.

        :param parquet_file: Path to the Parquet file to convert.
        :type parquet_file: str
        :param geoparquet_path: Path to save the GeoParquet parquet partitions
        :type geoparquet_path: str

        :return: List of paths to the GeoParquet partitions.
        :rtype: List[str]
        """
        
        os.makedirs(geopq_path, exist_ok=True)
        ddf = dd.read_parquet(parquet)

        # Define a function to convert a partition to a GeoDataFrame
        def convert_partition_to_gdf(partition):
            partition['geometry'] = partition['geometry'].apply(wkb.loads)
            gdf = gpd.GeoDataFrame(partition, geometry='geometry')
            gdf = gdf.set_crs(epsg=4326)
            return gdf

        # Convert each partition and write to GeoParquet
        delayed_tasks = []
        for i, partition in enumerate(ddf.to_delayed()):
            def process_partition(partition, i):
                try:
                    # Convert to GeoDataFrame
                    gdf = convert_partition_to_gdf(partition)

                    # Save GeoParquet
                    partition_path = os.path.join(geopq_path, f'geoparquet_partition_{i}.parquet')
                    gdf.to_parquet(partition_path,  engine='pyarrow', compression='snappy')

                    logger.info(f'Processed partition {i} into GeoParquet format.')
                    return partition_path
                except Exception as e:
                    logger.error(f'Error processing partition {i}: {e}')
                    return None

            # Add to delayed tasks
            delayed_task = dask.delayed(process_partition)(partition, i)
            delayed_tasks.append(delayed_task)

        # Trigger computation for all tasks
        results = dask.compute(*delayed_tasks)

        # Collect successful partition paths
        successful_partitions = [res for res in results if res is not None]
        logger.info(f'Processed {len(successful_partitions)} partitions into GeoParquet format.')
        if len(dd.read_parquet(geopq_path)) != len(ddf):
            logger.error(f"Total features in the parquet file do not match the GeoParquet")
            return None
        return geopq_path