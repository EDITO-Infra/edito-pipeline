import dask
from dask.distributed import progress
import geopandas as gpd
import dask.dataframe as dd
import fiona
import os
import logging
import pyarrow.parquet as pq
import dask_geopandas as dgpd
from shapely import wkb
import pandas as pd
import psutil
import time
import gc
import json
from pyproj import CRS
from typing import Any, Dict, List, TYPE_CHECKING

if TYPE_CHECKING:
    from metagisconfig2stac.layer import Layer
logger = logging.getLogger(__name__)


class GdbToParquet:
    """
    Converts GDB layers to Parquet format using Dask distributed processing.
    
    Iterates through layers in a GDB file, processes features in chunks,
    converts each chunk to GeoDataFrame, and saves them as Parquet files
    with parallel processing using Dask. Then checks if the Parquet datasets
    have been created correctly. Then combines all chunk Parquet files into a final Parquet file.

    :param layer: Layer object containing metadata and paths.
    :type layer: Layer

    :param gdb_path: Path to the GDB file.
    :type gdb_path: str
    
    :param gdblayer: Name of the layer in the GDB file.
    :type gdblayer: str

    :return: Path to the final Parquet file or None if failed.
    :rtype: str or None
    """
    def __init__(self, layer: 'Layer', gdb_path: str, gdblayer: str):
        self.layer = layer
        self.gdb_path = gdb_path
        self.gdblayer = gdblayer
        self.native_arco_mapping = layer.metadata['native_arco_mapping']
        self.parquet_chunk_size = 150000


    def gdb_to_parquet(self):
        """
        Converts a layer from a geodatabase to parquet and geoparquet format.

        :param layer: Layer object containing metadata and paths.
        :type layer: Layer

        :param gdb_path: Path to the GDB file.
        :type gdb_path: str

        :param gdblayer: Name of the layer in the GDB file.
        :type gdblayer: str

        :return: Path to the Parquet file or None if failed.
        :rtype: str or None
        :return: Path to the GeoParquet file or None if failed.
        :rtype: str or None

        """
        arco_dir = self.layer.converted_arco
        title = os.path.splitext(os.path.basename(self.gdb_path))[0]
        chunk_dir = os.path.join(arco_dir, 'parquet_output')   
        
        # open the geodatabase layer and get the crs, bounds, and variables
        with fiona.open(self.gdb_path, layer=self.gdblayer) as src:
            crs = src.crs
            crs_wkt = crs.to_wkt() 
            self.layer.metadata['crs_wkt'] = crs_wkt
            logger.info(f"CRS: {crs}")
            total_bounds = src.bounds
            self.layer.metadata['total_bounds'] = total_bounds
            logger.info(f"Bounds: {total_bounds}")
            gdbvariables = list(src.schema['properties'].keys())
            self.layer.metadata['gdbvariables'] = gdbvariables
            logger.info(f"Variables in {self.layer}: {gdbvariables}")
            num_features = len(src)
            self.layer.metadata['gdb_num_features'] = num_features
            logger.info(f"Total features in {self.layer}: {num_features}")
        
        # create the parquet chunks
        os.makedirs(chunk_dir, exist_ok=True)
        
        # convert the geodatabase layer into parquet chunks
        parquet_path = os.path.join(arco_dir, f"{title}_parquet_partitioned.parquet")
        parquet = self.convert_to_partitioned_parquet(self.gdblayer, parquet_path)
        
        if parquet is None:
            logger.error(f"Failed to convert {self.gdblayer} to Parquet format")
            return None
        # convert the parquet file into geoparquet
        geoparquet_path = os.path.join(arco_dir, f"{title}_geoparquet_partitioned.parquet")

        os.makedirs(geoparquet_path, exist_ok=True)
        geopq = self.map_to_geoparquet(parquet, geoparquet_path)
        
        if geopq is None:
            logger.error(f"Failed to convert {parquet_path} to GeoParquet format")
            return None
        
        if len(dd.read_parquet(geopq)) != len(dd.read_parquet(parquet)):
            logger.error(f"Total features in the parquet file do not match the GeoParquet")
            return None
        
        if len(dd.read_parquet(geopq)) < 10000000:
            singlefile = os.path.join(arco_dir, f"{title}_geoparquet.parquet")
            single = self.compress_to_single_parquet(geopq, singlefile)
            if single is None:
                logger.error(f"Failed to compress {geopq} to a single file")
                return None

        return parquet, geopq, single


    def align_dtypes(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Aligns the data types, Object to string, int64 to float32, float64 to float32, and datetime to datetime64[ms].

        :param df: DataFrame to align data types.
        :type df: pd.DataFrame

        :return: DataFrame with aligned data types.
        :rtype: pd.DataFrame
        """
        # Ensure consistent data types, downcast to reduce memory usage, use float for NaN and dask compatibility
        for col in df.columns:
            if col == 'geometry':
                continue
            if df[col].dtype == 'object':
                df[col] = df[col].astype(str)
            elif df[col].dtype == 'int64' or df[col].dtype == 'int32':
                df[col] = df[col].astype('float32')
                # check for None values
                if df[col].isnull().any():
                    df[col] = df[col].astype('float32')

            elif df[col].dtype == 'float64':
                df[col] = df[col].astype('float32')
            
            # uniform datetime to datetime64[ms]
            elif 'datetime' in str(df[col].dtype):
                df[col] = pd.to_datetime(df[col], errors='coerce')
                df[col] = df[col].dt.strftime('%Y-%m-%d %H:%M:%S')
                df[col] = pd.to_datetime(df[col], errors='coerce')

        return df

    def to_wkb(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Drop null geometries, convert geometry column to WKB format

        :param df: GeoDataFrame to convert
        :type df: gpd.GeoDataFrame

        :return: GeoDataFrame with geometry column in WKB format
        :rtype: gpd.GeoDataFrame
        """
        # remove any null geometries
        null_geom = df[df['geometry'].isnull()]
        if not null_geom.empty:
            logger.info(f"Removing {len(null_geom)} features with null geometries")
            df = df.dropna(subset=['geometry'])
        # convert the geometry column to WKB format
        df['geometry'] = df['geometry'].apply(lambda geom: geom.wkb)
        
        return df

# convert each geodatabase layer into parquet chunks
    def convert_to_partitioned_parquet(self, gdblayer, parquet_path):
        """
        Converts a GDB layer to a partitioned Parquet file using Dask distributed processing. Uses other class methods
        to read the GDB layer in chunks, convert each chunk to a GeoDataFrame, and save partitions of parquet into
        a final Parquet file.

        :param gdblayer: Name of the GDB layer to convert.
        :type gdblayer: str

        :param parquet_path: Path to save the Parquet file.
        :type parquet_path: str

        :return: Path to the final Parquet file or None if failed.
        :rtype: str or None
        """
        
        with fiona.open(self.gdb_path, layer=gdblayer) as src:
            total_features = len(src)
            self.crs = src.crs
            logger.info(f"Total features in {gdblayer}: {total_features}")
            gdbvariables = list(src.schema['properties'].keys())

            # confirm that all variables in the mapping are in the GDB layer
            for var in self.native_arco_mapping:
                if var not in gdbvariables:
                    logger.error(f"Variable {var} not found in the GDB layer, remove from the mapping")
                    self.native_arco_mapping = {k: v for k, v in self.native_arco_mapping.items() if k != var}

        ddf = dgpd.read_file(self.gdb_path, layer=gdblayer, chunksize=self.parquet_chunk_size)

        logger.info(f"Reading {gdblayer} in chunks")

        logger.info(f"renaming columns {self.native_arco_mapping}")
        ddf = ddf.rename(columns={var: self.native_arco_mapping[var] for var in self.native_arco_mapping})
        ddf = ddf.map_partitions(self.align_dtypes, meta=ddf._meta)
        ddf = ddf.map_partitions(self.to_wkb, meta=ddf._meta)

        if len(ddf) != total_features:
            logger.error(f"Total features in the geodatabase layer {gdblayer} do not match the Dask GeoDataFrame")
            return None
        
        ddf.to_parquet(parquet_path, engine='pyarrow', compression='snappy', write_index=False, write_metadata_file=True)

        logger.info(f"Parquet file saved at {parquet_path}")
        return parquet_path  
    
    
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
        
        
        ddf = dd.read_parquet(parquet)

        os.makedirs(geopq_path, exist_ok=True)

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
                    partition_path = os.path.join(geopq_path, f'part_{i}.parquet')
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


    def compress_to_single_parquet(self, geopq_path, singlefile):

        """
        Compress the GeoParquet partitions into a single file

        :param geopq_outfile: Path to the GeoParquet output directory
        :type geopq_outfile: str

        :return: Path to the compressed GeoParquet file
        :rtype: str
        """
        # Read the GeoParquet partitions
        geopq_partitions = []
        for root, dirs, files in os.walk(geopq_path):
            for file in files:
                if file.endswith('.parquet'):
                    geopq_partitions.append(os.path.join(root, file))

        if not geopq_partitions:
            logger.error("No GeoParquet partitions found")
            return None
        
        try:
            # Read the partitions
            geopq_dfs = [pd.read_parquet(partition) for partition in geopq_partitions]

            # Concatenate the partitions
            geopq_df = pd.concat(geopq_dfs)

            # Save the concatenated GeoParquet
            
            geopq_df.to_parquet(singlefile, engine='pyarrow', compression='snappy')

            logger.info(f"Compressed GeoParquet saved at {singlefile}")
        except Exception as e:
            logger.error(f"error {e} compressing GeoParquet partitions")
            return None

        return singlefile