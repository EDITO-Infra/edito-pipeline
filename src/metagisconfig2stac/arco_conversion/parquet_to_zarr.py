
import os
import logging
import numpy as np
import pandas as pd
import geopandas as gpd
import xarray as xr
import dask.array as da
import dask
from tqdm import tqdm
import rasterio
from rasterio.windows import Window
from rasterio.features import rasterize
from affine import Affine
from shapely.geometry import box
from shapely import wkb, wkt
import dask.dataframe as dd
import dask_geopandas as dgpd
import shapely
import fiona
import gc
from .utils import ArcoConvUtils
from typing import List, Dict, Any, TYPE_CHECKING

if TYPE_CHECKING:
    from layer import Layer

logger = logging.getLogger(__name__)

class ParquetToZarr:
    """
    Convert parquet to zarr format.

    :param layer: Layer object with data product needing conversion.
    :type layer: Layer
    :param native_product_path: Path to the Apache Parquet file.
    :type native_product_path: str

    :return zarr_path: Path to the Zarr file.
    :rtype str
    """
    def __init__(self, layer: ('Layer'), geoparquet_path: str):
        self.layer = layer
        self.metadata = layer.metadata
        self.converted_arco = layer.converted_arco
        self.geoparquet_path = geoparquet_path
        self.native_product = self.layer.metadata['native_data_product']
        self.title = os.path.splitext(self.native_product)[0]
    
    def filter_polygon_geometries(self, dgdf):
        """
        Filter the GeoDataFrame to only include Polygon and MultiPolygon geometries.

        :param dgdf: Dask GeoDataFrame to filter.
        :type dgdf: dgpd.GeoDataFrame

        :return dgdf: Filtered Dask GeoDataFrame.
        :rtype dgpd.GeoDataFrame
        """
        unique_geom_types = dgdf.geometry.geom_type.unique().compute()
        logger.info(f"Found geometry types: {unique_geom_types}")

        for geom_type in unique_geom_types:
            if geom_type not in ['Polygon', 'MultiPolygon']:
                rows_to_drop = dgdf[dgdf.geometry.geom_type == geom_type].compute()
                logger.info(f"Dropping {len(rows_to_drop)} rows with geometry type {geom_type}")
        
        # only convert Polygon and MultiPolygon geometries
        dgdf = dgdf[dgdf.geometry.geom_type.isin(['Polygon', 'MultiPolygon'])]

        if dgdf.shape[0].compute() == 0:
            logger.error(f"No polygons found in the GeoDataFrame")
            return
        return dgdf
    
    def parquet_to_zarr(self, geoparquet_path: str, resolution=0.01):
        """
        Read the parquet, process each variable using parquet_var_to_zarr. Combine all the zarr datasets
        and update the dataset using ArcoConvUtils methods.

        :param resolution: Resolution of the raster.
        :type resolution: float
        :param layer: Layer object with data product needing conversion.
        :type layer: Layer
        :param ddfchunksize: Size of the Dask DataFrame chunks.
        :type ddfchunksize: int

        :return zarr_path: Path to the Zarr file.
        :rtype str
        """
        zarr_var_paths = []
        pq = dgpd.read_parquet(geoparquet_path, dtype_backend='pyarrow')

        # filter out non-polygon geometries
        pq = self.filter_polygon_geometries(pq)
        if pq is None:
            logger.error(f"no valid polygon geometries found in {geoparquet_path}")
            return
        # Loop through each variable in the parquet file, excluding geometry
        for native_var in pq.columns.drop('geometry'):
            zarr_var_path = self.parquet_var_to_zarr(geoparquet_path, self.converted_arco, native_var, resolution)
            zarr_var_paths.append(zarr_var_path)
        
        zarr_path = ArcoConvUtils.combine_update_save_dataset(self.layer, zarr_var_paths, geoparquet_path, self.title)
        return zarr_path

    def clean_categorical_data(self, data):
        """
        Clean categorical data

        :param data: Data to clean
        :type data: Any
        """
        if isinstance(data, str):
            if data in [' ', 'nan', "", " ", '']:
                data = 'None'
        return data


    def parquet_var_to_zarr(self, geopq: str, output_dir: str, native_var: str, resolution: float):
        """
        Create a single zarr file for one variable from the parquet file. 

        :param file_path: Path to the Apache Parquet file.
        :type file_path: str
        :param output_dir: Path to the output directory.
        :type output_dir: str
        :param native_var: Name of the variable to convert.
        :type native_var: str
        :param resolution: Resolution of the raster.
        :type resolution: float
        :param layer: Layer object containing metadata and paths.
        :type layer: Layer
        :param ddfchunksize: Size of the Dask DataFrame chunks.
        :type ddfchunksize: int

        :return zarr_var_path: Path to the Zarr file.
        :rtype str
        """
        # Read and repartition the Dask GeoDataFrame

        logger.info(f"Converting {native_var} to Zarr")
        
        dgdf = dgpd.read_parquet(geopq, columns=[native_var, 'geometry'], dtype_backend='pyarrow')

        dgdf = self.filter_polygon_geometries(dgdf)

        # Compute bounds
        self.total_bounds = dgdf.geometry.total_bounds.compute().tolist()
        lon_min, lat_min, lon_max, lat_max = self.total_bounds
        
        # Define raster boundaries and resolution
        width = int(np.ceil((lon_max - lon_min) / resolution))
        height = int(np.ceil((lat_max - lat_min) / resolution))
        raster_transform = rasterio.transform.from_bounds(lon_min, lat_min, lon_max, lat_max, width, height)

        # Initialize memory-mapped raster array
        raster = np.memmap(f"{output_dir}/raster.npy", dtype=np.float32, mode='w+', shape=(height, width))
        raster[:] = np.nan  # Initialize with NaN for no data

        category_mapping = {}

        logger.info("Rasterizing cleaned and encoded data")

        # Loop through each partition of the Dask GeoDataFrame
        for pt_index, pt_df in enumerate(dgdf.to_delayed()):

            pt_df = pt_df.compute()
            logger.info(f"Rasterizing chunk {pt_index} len {len(pt_df)}")
            pt_data = pt_df[native_var].apply(self.clean_categorical_data)

            # assess data series and encode categorical data if necessary
            if pd.Series(pt_data).dtype == 'object':
                pt_data = pt_data.fillna('None')
                encoded_data = []
                for value in pt_data:
                    if value not in category_mapping:
                        category_mapping[value] = len(category_mapping) + 1
                    encoded_data.append(category_mapping[value])
            else:
                encoded_data = pt_data.astype(np.float32).values

            geometries = pt_df['geometry'].values

            # Rasterize geometries with encoded data
            with tqdm(total=len(geometries), desc=f"Rasterizing partition {pt_index}") as pbar:
                rasterio.features.rasterize(
                    ((geom, value) for geom, value in zip(geometries, encoded_data)),
                    out=raster,
                    transform=raster_transform,
                    merge_alg=rasterio.enums.MergeAlg.replace,
                    dtype=np.float32,
                )
                pbar.update(len(geometries))
            logger.info(f"Rasterized partition {pt_index}")
        
        logger.info("Rasterization complete, making xarray Dataset")
        dataset = xr.Dataset(coords={
            'latitude': np.round(np.linspace(lat_max, lat_min, height, dtype=float), decimals=4),
            'longitude': np.round(np.linspace(lon_min, lon_max, width, dtype=float), decimals=4)
        })
        dataset[native_var] = (['latitude', 'longitude'], raster)
        dataset[native_var].attrs['standard_name'] = native_var
        dataset = dataset.sortby('latitude')

        # add categorical encoding to variable attributes
        if category_mapping:
            dataset[native_var].attrs['categorical_encoding'] = category_mapping

        # Write to Zarr
        zarr_var_path = f"{output_dir}/{self.title}_{native_var}.zarr"
        dataset.to_zarr(zarr_var_path, mode='w', consolidated=True)
        logger.info(f"Zarr dataset saved at {zarr_var_path}")

        return zarr_var_path