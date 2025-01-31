import dask
from dask.distributed import progress
import geopandas as gpd
import dask.dataframe as dd
import fiona
import os
import logging
import pyarrow.parquet as pq
import pandas as pd
from shapely import wkb
from typing import Any, Dict, List, TYPE_CHECKING
from pyproj import CRS
if TYPE_CHECKING:
    from metagisconfig2stac.layer import Layer
logger = logging.getLogger(__name__)

class ShpToParquet:
    """
    Convert Shp file to Apache Parquet format, and Geoparquet format.

    :param layer: Layer object with data product needing conversion.
    :type layer: Layer

    :param shp_path: Path to the shp file.
    :type shp_path: str
    
    :param shplayer: Name of the layer in the shp file.
    :type shplayer: str

    :return parquet_file, geoparquet_file: Path to the Apache Parquet file and Geoparquet file.
    :rtype tuple
    """
    def __init__(self, layer: 'Layer', shp_path: str, shplayer: int):
        self.layer = layer
        self.shp_path= shp_path
        self.shplayer = shplayer
        self.native_arco_mapping = layer.metadata['native_arco_mapping']


    def shp_to_parquet(self):
        """
        Convert Shp file to Apache Parquet format, and Geoparquet format.

        :param layer: Layer object with data product needing conversion.
        :type layer: Layer

        :param shp_path: Path to the GDB file.
        :type shp_path: str

        :param shplayer: Name of the layer in the GDB file.
        :type shplayer: str

        :return parquet_file, geoparquet_file: Path to the Apache Parquet file and Geoparquet file.
        :rtype tuple
        """
        arco_dir = self.layer.converted_arco
        title = os.path.splitext(os.path.basename(self.shp_path))[0]
        chunk_dir = os.path.join(arco_dir, 'parquet_chunks')   
        

        with fiona.open(self.shp_path, layer=self.shplayer) as src:
            crs = src.crs
            crs_wkt = crs.to_wkt() 
            self.layer.metadata['crs_wkt'] = crs_wkt
            logger.info(f"CRS: {crs}")
            total_bounds = src.bounds
            logger.info(f"Bounds: {total_bounds}")
            gdbvariables = list(src.schema['properties'].keys())
            logger.info(f"Variables in {self.layer}: {gdbvariables}")
            num_features = len(src)
            logger.info(f"Total features in {self.layer}: {num_features}")
        

        # read the shapefile in chunks
        gdf = gpd.read_file(self.shp_path, layer=self.shplayer)
        
        # filter null geometries
        logger.info(f"Filtering null geometries: {len(gdf)}")
        null_rows = gdf['geometry'].isnull()
        logger.info(f"Null geometries: {null_rows.sum()}")
        gdf = gdf[~null_rows]
        logger.info(f"Filtered geometries: {len(gdf)}")

        # rename columns using native arco mapping
        gdf = gdf.rename(columns={var: self.native_arco_mapping[var] for var in self.native_arco_mapping})
        geoparquet_file = f"{os.path.join(arco_dir, f'{title}_geoparquet.parquet')}"
        gdf.to_parquet(geoparquet_file, engine='pyarrow')

        # apache parquet
        gdf['geometry'] = gdf['geometry'].apply(lambda x: wkb.dumps(x))
        # convert to dask dataframe
        
        df = pd.DataFrame(gdf)
        parquet_file = f"{os.path.join(arco_dir, f'{title}_parquet.parquet')}"
        df.to_parquet(parquet_file, engine='pyarrow')

        return parquet_file, geoparquet_file