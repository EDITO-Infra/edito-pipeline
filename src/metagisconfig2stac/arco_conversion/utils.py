import os
import shutil
from datetime import date
import dask.dataframe
import dask.distributed
import numpy as np
import pandas as pd
import dask
import dask.array as da
import fiona
import geopandas as gpd
import rasterio
import shapely
import xarray as xr
import pyarrow.parquet as pq
import pyarrow as pa
import json
from affine import Affine
from dask.delayed import delayed
from rasterio.features import rasterize
from rasterio.windows import Window
from shapely.geometry import box
from shapely import wkb
from tqdm import tqdm
import logging
import s3fs
import dotenv
import dask
from metagisconfig2stac.utils.s3 import S3Utils, EMODnetS3
from metagisconfig2stac.utils.core import CoreUtils
from datetime import datetime, date
import dask_geopandas as dgpd
import dask.dataframe as dd
from pyproj import CRS
from typing import List, Dict, Any, TYPE_CHECKING, Union

if TYPE_CHECKING:
    from metagisconfig2stac.layer import Layer  
logger = logging.getLogger(__name__)


class ArcoConvUtils():
    def __init__(self):
        pass
    
    @staticmethod
    def _rename_zarr_native_variables(layer: ('Layer'), dataset: xr.Dataset):
        """
        renames variables in dataset to match native arco mapping
        :param layer: The Layer object.
        :type layer: Layer
        :param dataset: The dataset to rename the variables of.
        :type dataset: xr.Dataset
        return: The dataset with the variables renamed.
        :rtype: xr.Dataset
        """
        if 'native_arco_mapping' not in layer.metadata:
            logging.error("Native ARCO mapping not found in layer metadata.")
            return dataset
        for var, arco_var in layer.metadata['native_arco_mapping'].items():
            if var in dataset.data_vars:
                dataset = dataset.rename_vars({var: arco_var})
            elif arco_var in dataset.data_vars:
                logger.info(f'Variable {var} already renamed to {arco_var}')
        return dataset


    @staticmethod
    def _add_categorical_encodings(layer: ('Layer'), dataset: xr.Dataset):
        """
        Adds categorical encodings to the dataset attributes and layer metadata.
        
        :param layer: The Layer object.
        :type layer: Layer
        :param dataset: The dataset to add the categorical encodings to.
        :type dataset: xr.Dataset
        :return: The dataset with the categorical encodings added.
        :rtype: xr.Dataset
        """
        categorical_encodings_dict = {}
        for var in dataset.data_vars:
            if 'categorical_encoding' in dataset[var].attrs:
                categorical_encodings_dict[var] = dataset[var].attrs['categorical_encoding']
        if categorical_encodings_dict:
            dataset.attrs['categorical_encoding'] = categorical_encodings_dict
            layer.metadata['categorical_encoding'] = categorical_encodings_dict
        return dataset

    @staticmethod
    def _add_variable_extent(dataset: xr.Dataset):
        """
        add geospatial extent to dataset variables
        :param dataset: The dataset to add the variable extent to.
        :type dataset: xr.Dataset
        :return: The dataset with the variable extent added.
        :rtype: xr.Dataset
        """
        for var in dataset.data_vars:
            dataset[var].attrs['geospatial_lat_min'] = dataset[var].latitude.min().item()
            dataset[var].attrs['geospatial_lat_max'] = dataset[var].latitude.max().item()
            dataset[var].attrs['geospatial_lon_min'] = dataset[var].longitude.min().item()
            dataset[var].attrs['geospatial_lon_max'] = dataset[var].longitude.max().item()
        return dataset


    @staticmethod
    def add_crs(layer: ('Layer'), native_product_path: str, dataset: xr.Dataset):
        """
        Add crs to each variable in the dataset. 
        :param layer: The Layer object.
        :type layer: Layer
        :param vector_file: The path to the vector file.
        :type vector_file: str
        :param dataset: The dataset to add the CRS to.
        :type dataset: xr.Dataset

        :return: The dataset with the CRS added.
        :rtype: xr.Dataset
        """
        # if crs in wkt format from parquet conversion
        if 'crs_wkt' in layer.metadata:
            crs_wkt = layer.metadata['crs_wkt']
        
        # elif crs from other metadata
        elif 'crs' in layer.metadata and isinstance(layer.metadata['crs'], dict):
            crs_wkt = CRS.from_dict(layer.metadata['crs']).to_wkt()

        # or read crs
        elif os.path.exists(native_product_path):
            if native_product_path.endswith(tuple(['.gdb', '.shp', '.csv', '.parquet'])):
                try:
                    gdf0 = gpd.read_file(native_product_path, rows=1)
                    crs = gdf0.crs
                    crs_wkt = crs.to_wkt()
                except Exception as e:
                    logger.error(f"geopandas failed, no crs in vars: {e}")
                    return dataset
            if native_product_path.endswith('.tif'):
                try:
                    with rasterio.open(native_product_path) as src:
                        crs_wkt = src.crs.to_wkt()
                except Exception as e:
                    logger.error(f"rasterio failed, no crs in vars: {e}")
                    return dataset
        else:
            return dataset
        # add crs in wkt form to each variable attributes, helps some packages read zarr properly
        for var in dataset.data_vars:
            dataset[var].attrs['_CRS'] = {"wkt": crs_wkt}
        return dataset


    @staticmethod
    def update_dataset_attributes(layer: ('Layer'), native_product_path: str, combined_dataset: xr.Dataset, title: str):
        """
        Update xr.Dataset using update_crs, add_variable_extent, rename_native_variables, add_categorical_encodings, and update_zarr_attributes.
        Returns the updated dataset.

        :param layer: The Layer object.
        :type layer: Layer
        :param native_product_path: The path to the native product file.
        :type native_product_path: str
        :param combined_dataset: The combined dataset as an xarray Dataset object.
        :type combined_dataset: xr.Dataset
        
        :return: The updated dataset.
        :rtype: xr.Dataset
        """
        logger.info("updating crs")
        combined_dataset = ArcoConvUtils.add_crs(layer, native_product_path, combined_dataset)
        logger.info('updating variable extent')
        combined_dataset = ArcoConvUtils._add_variable_extent(combined_dataset)
        logger.info('renaming variables to arco variables(standard names)')
        combined_dataset = ArcoConvUtils._rename_zarr_native_variables(layer, combined_dataset)
        logger.info('updating categorical encodings if available')
        combined_dataset = ArcoConvUtils._add_categorical_encodings(layer, combined_dataset)
        logger.info('updating zarr dataset attributes')
        combined_dataset = ArcoConvUtils._update_zarr_attributes(layer, combined_dataset, title)

        logger.info('dataset attributes updated')
        return combined_dataset

    @staticmethod
    def combine_update_save_dataset(layer: ('Layer'), zarr_paths: list, native_product_path: str, title: str):
        """
        Combines zarr files created for each variable, updates dataset attributes, returns final zarr file path.
        :param layer: The Layer object.
        :type layer: Layer
        :param zarr_paths: The list of paths to the Zarr files.
        :type zarr_paths: list
        :param native_product_path: The path to the native product file.
        :type native_product_path: str
        :param title: The title of the dataset.
        :type title: str
        :return: The path to the final Zarr file.
        :rtype: str
        """
        logger.info('combining final zarr dataset')
        combined_dataset = xr.Dataset()
        try:
            with dask.config.set(scheduler='single-threaded'):
                for path in zarr_paths:
                    dataset = xr.open_zarr(path, chunks={})
                    combined_dataset = xr.merge([combined_dataset, dataset], compat='override', join='outer')
        except Exception as e:
            logger.error(f"Error combining zarr files: {e}")
            return None
        
        final_dataset = ArcoConvUtils.update_dataset_attributes(layer, native_product_path, combined_dataset, title)

        zarr_path = os.path.join(layer.converted_arco, f"{title}.zarr")
        with dask.config.set(scheduler='single-threaded'):
            try:
                logger.info('rechunking and saving final zarr dataset')    
                final_dataset = final_dataset.chunk({'latitude': 'auto', 'longitude': 'auto'})  # for var in dataset.variables:
                final_dataset.to_zarr(zarr_path, mode='w')
                if os.path.exists(zarr_path):
                    logger.info(f"Zarr made at {zarr_path} from {os.path.basename(native_product_path)}")
                    return zarr_path
                
            except Exception as e:
                logger.error(f"final zarr dataset did not save error: {e}")
                return None


    @staticmethod
    def _update_zarr_attributes(layer: ('Layer'), dataset: xr.Dataset, title: str):
        """
        Updates attributes of xr.Dataset for Zarr file. Adds proj:epsg, geospatial extent, resolution, history, title, and sources.
        :param layer: The Layer object.
        :type layer: Layer
        :param dataset: The dataset to update the attributes of.
        :type dataset: xr.Dataset
        :param title: The title of the dataset.
        :type title: str
        :return: The updated dataset.
        :rtype: xr.Dataset
        """

        lat_step = (dataset.latitude.values.max() - dataset.latitude.values.min()) / dataset.latitude.values.shape[0]
        lon_step = (dataset.longitude.values.max() - dataset.longitude.values.min()) / dataset.longitude.values.shape[0]
        
        dataset.latitude.attrs.update({
            '_CoordinateAxisType': 'Lat', 'axis': 'Y', 'long_name': 'latitude',
            'max': dataset.latitude.values.max(), 'min': dataset.latitude.values.min(),
            'standard_name': 'latitude', 'step': lat_step,
            'units': 'degrees_north'
        })
        dataset.longitude.attrs.update({
            '_CoordinateAxisType': 'Lon', 'axis': 'X', 'long_name': 'longitude',
            'max': dataset.longitude.values.max(), 'min': dataset.longitude.values.min(),
            'standard_name': 'longitude', 'step': lon_step,
            'units': 'degrees_east'
        })
        
        resolution = min(lat_step, lon_step)
        dataset.attrs.update({
             'proj:epsg': 4326, 'resolution': resolution, 
            **{f'geospatial_{axis}_min': dataset[axis].min().item() for axis in ['latitude', 'longitude']},
            **{f'geospatial_{axis}_max': dataset[axis].max().item() for axis in ['latitude', 'longitude']},
            'history': f'Converted on {date.today()}', 'title': title,
            'Comment': f"Converted from data product {layer.metadata['native_asset']} on {datetime.today().strftime('%Y-%m-%d')}"
        })
        if 'metadata_links' in layer.metadata:
            dataset.attrs['sources'] = layer.metadata['xml_asset']
        else:
            dataset.attrs['sources'] = layer.metadata['thematic_lot']
        return dataset


    @staticmethod
    def upload_arco_to_s3_update_assets(layer: ('Layer'), arco_file: str):
        """
        Upload ARCO data product to s3, update layer metadata with s3 url. If file exists, overwrite.
        :param layer: The Layer object.
        :type layer: Layer
        :param arco_file: The path to the ARCO file.
        :type arco_file: str
        :return: The S3 URL of the uploaded file.
        :rtype: str
        """
        from metagisconfig2stac.utils.core import CoreUtils
        from metagisconfig2stac.utils.s3 import S3Utils, EMODnetS3
        s3_dir = f"{CoreUtils.custom_slugify(layer.metadata['thematic_lot'])}/{layer.metadata['id']}"
        
        # create s3 connection via boto3
        emods3 = EMODnetS3(layer.pipeline_config)
        botoclient, bucket, host = emods3.botoclient, emods3.bucket, emods3.host
        logging.info(f"transferring {arco_file} to emodnet/{s3_dir}")

        # if arco file or directory(.zarr, .parquet/), remove existing
        if S3Utils.check_for_existing_objects(botoclient, bucket, f"{s3_dir}/{os.path.basename(arco_file)}"):
            S3Utils.delete_from_s3_recursive(botoclient, bucket, f"{s3_dir}/{os.path.basename(arco_file)}")

        # otherwise upload single file to s3 overwriting existing
        s3_url = S3Utils.upload_to_s3(botoclient, bucket, host, arco_file, s3_dir)
        
        if not s3_url:
            logging.error(f"Failed to upload {arco_file} to S3")
            return
        
        layer.metadata['converted_arco_assets'].append(s3_url)
        logger.info(f"uploaded to S3, layer metadata updated")
        return s3_url

    @staticmethod
    def _rename_zarr_variables(layer: ('Layer'), zarr_path: str):
        """
        Renames the variables in the Zarr file to match the native ARCO mapping.
        :param layer: The Layer object.
        :type layer: Layer
        :param zarr_path: The path to the Zarr file.
        :type zarr_path: str
        :return: The path to the renamed Zarr file.
        :rtype: str
        """
        try:
            ds = xr.open_dataset(zarr_path, engine='zarr')
            logger.info(f"Renaming variables in Zarr file {os.path.basename(zarr_path)}")
            ds = ArcoConvUtils._rename_native_variables(layer, ds)
            # add edito suffix to differentiate from original zarr
            edito_zarr_path = os.path.join(layer.converted_arco, f"{os.path.basename(zarr_path)[0:-5]}_edito.zarr")
            try:
                ds.to_zarr(edito_zarr_path, mode='w')
            except Exception as e:
                logging.error(f"Error saving Zarr file: {e}")
                return None
            return zarr_path
        except Exception as e:
            logging.error(f"Error renaming variables in Zarr file: {e}")
            return None


    @staticmethod
    def _rename_geodataframe_variables(layer: ('Layer'), gdf: gpd.GeoDataFrame):
        """
        Renames Geodataframe columns using native_arco_mapping from layer metadata.
        :param layer: The Layer object.
        :type layer: Layer
        :param gdf: The Geodataframe to rename the columns of.
        :type gdf: gpd.GeoDataFrame
        :return: The Geodataframe with the renamed columns.
        :rtype: gpd.GeoDataFrame
        """
        arco_native_dict = layer.metadata['native_arco_mapping']
        if set(arco_native_dict.keys()) - set(gdf.columns) - {'geometry'}:
            logging.error(f"ARCO variable error, {set(arco_native_dict.keys()) - set(gdf.columns) - {'geometry'}} "
                          f"not found in {os.path.basename(layer.native_product_path)}.")
            return None
        rename_mapping = {native_var: arco_native_dict[native_var] for native_var in arco_native_dict if native_var in gdf.columns}
        gdf = gdf.rename(columns=rename_mapping)
        arco_values = set(rename_mapping.values())
        gdf = gdf[[col for col in gdf.columns if col in arco_values or col == 'geometry']]
        
        return gdf
    

    def rename_zarr_variables_edito(layer: ('Layer'), zarr_path: str):
        """
        Renames the variables in the Zarr file to match the native ARCO mapping. The file is then uploaded to S3.

        :param layer: The Layer object.
        :type layer: Layer
        :param zarr_path: The path to the Zarr file.
        :type zarr_path: str
        """
        ds = xr.open_dataset(zarr_path, engine='zarr')
        ds = ArcoConvUtils._rename_native_variables(layer, ds)
        edito_zarr_path = os.path.join(layer.converted_arco, f"{os.path.basename(zarr_path)[0:-5]}_edito.zarr")
        ds.to_zarr(edito_zarr_path, mode='w')
        return edito_zarr_path
    

    @staticmethod
    def push_parquet_partition_to_emodnet_s3(layer: ('Layer'), partition: Union[pd.DataFrame, gpd.GeoDataFrame] , s3_key: str, partition_index: int):
        
        """
        Pushes a partition of a Dask dataframe to EMODnet S3, using the s3fs library.

        :param layer: The Layer object.
        :type layer: Layer
        :param partition: The partition to push to S3.
        :type partition: Union[pd.DataFrame, gpd.GeoDataFrame]
        :param s3_key: The key of the S3 object.
        :type s3_key: str
        :param partition_index: The index of the partition.
        :type partition_index: int
        """
        emod_s3 = EMODnetS3(layer.pipeline_config)
        access_key = emod_s3.access_key
        secret_access_key = emod_s3.secret_key
        host = emod_s3.host
        bucket = emod_s3.bucket
        # Initialize s3fs with the boto3 client
        s3_fs = s3fs.S3FileSystem(endpoint_url=f"{host}",
                                key=access_key,
                                secret=secret_access_key)
        
        s3_uri = f's3://{bucket}/{s3_key}partition_{partition_index}.parquet'
        
        # ensure geodataframe written with geometry column, and crs
        if isinstance(partition, gpd.GeoDataFrame):
            partition = gpd.GeoDataFrame(partition, geometry='geometry')
            partition.crs = CRS.from_epsg(4326)
        try:
            with s3_fs.open(s3_uri, 'wb') as f:
                partition.to_parquet(f, engine='pyarrow')
            print(f"Partition {partition_index} uploaded successfully.")
        except Exception as e:
            print(f"Error uploading partition {partition_index}: {e}")
    

    @staticmethod
    def rename_cols_update_partitions(layer: ('Layer'), asset_url: str, ddf: dask.dataframe, mapping, s3_key, client: dask.distributed.Client):
        """
        Rename columns of dask dataframe or geodataframe and update partitions directly to s3.

        :param layer: The Layer object.
        :type layer: Layer
        :param asset_url: The URL of the Parquet file.
        :type asset_url: str
        :param ddf: The Dask dataframe or geodataframe to rename the columns of.
        :type ddf: dask.dataframe
        :param mapping: The mapping of the column names to rename.
        :type mapping: dict
        :param s3_key: The key of the S3 object.
        :type s3_key: str

        :return: The list of successful partitions.
        :rtype: list
        """
        delayed_tasks = []
        for i, partition in enumerate(ddf.to_delayed()):
            def process_partition(partition, mapping, s3_key, i):
                partition = partition.rename(columns=mapping)
                ArcoConvUtils.push_parquet_partition_to_emodnet_s3(layer, partition, s3_key, i)
                return f"s3://{s3_key}partition_{i}.parquet"

            delayed_task = dask.delayed(process_partition)(partition, mapping, s3_key, i)
            delayed_tasks.append(delayed_task)
        results = dask.compute(*delayed_tasks, scheduler=client)
        successful_partitions = [res for res in results if res is not None]
        if len(successful_partitions) == len(delayed_tasks):
            logger.info(f"Columns renamed and partitions updated for {asset_url}, uploaded to storage {s3_key}")
        return successful_partitions


    @staticmethod
    def rename_s3_parquet_columns_edito(layer: ('Layer'), asset_url: str, pqtype: str):
        """
        Renames the columns of a subsettable parquet dataset on s3 to match the native ARCO mapping.

        :param layer: The Layer object.
        :type layer: Layer
        :param asset_url: The URL of the Parquet file.
        :type asset_url: str
        :param pqtype: The type of parquet file, either 'geoparquet' or 'parquet'.
        :type pqtype: str

        """
        # reading geoparquet return dask geodataframe
        if pqtype == 'geoparquet':
            ddf = S3Utils.read_geoparquet_s3(asset_url)

        if pqtype == 'parquet':
            ddf = S3Utils.read_parquet_from_s3(asset_url)

        cluster = dask.distributed.LocalCluster(n_workers=1, threads_per_worker=1, memory_limit='8GB')
        client1 = dask.distributed.Client(cluster)

        
        ddf = ddf.persist()
        mapping = layer.metadata['native_arco_mapping']
        pq_title = os.path.splitext(os.path.basename(asset_url))[0]
        s3_key = f"{CoreUtils.custom_slugify(layer.metadata['thematic_lot'])}/{layer.metadata['id']}/{pq_title}_edito.parquet/"
        renamed_pts = ArcoConvUtils.rename_cols_update_partitions(layer, asset_url, ddf, mapping, s3_key, client1)

        if len(renamed_pts) == ddf.npartitions:
            logger.info(f"Columns renamed and partitions updated for {asset_url}, {layer.metadata['native_arco_mapping']} uploaded to storage {s3_key}")
            host = EMODnetS3(layer.pipeline_config).host
            layer.metadata['converted_arco_assets'].append(f"{host}/{s3_key}")
            logger.info(f"Layer metadata converted arco assets updated {host}/{s3_key}")
            cluster.close()
            return f"{host}/{s3_key}"
        else:
            logger.error(f"Error renaming columns and updating partitions for {asset_url}")
            cluster.close()
            return None


    @staticmethod
    def rename_parquet_columns_edito(layer: ('Layer'), local_parquet_path: str):
        """
        Renames the columns of a local parquet with native ARCO mapping, uploads to s3.
        :param layer: The Layer object.
        :type layer: Layer
        :param asset_url: The URL of the Parquet file.
        :type asset_url: str
        :return: The URL of the uploaded file.
        :rtype: str
        """
        
        ddf = dd.read_parquet(local_parquet_path)
        mapping = layer.metadata['native_arco_mapping']
        
        for pt in ddf.to_delayed():
            pt = pt.rename(columns=mapping)
            pt = pt.compute()
        
        edito_parquet_path = os.path.join(layer.converted_arco, f"{os.path.basename(local_parquet_path)[0:-8]}_edito.parquet")
        ddf.to_parquet(edito_parquet_path, write_index=False)

        logger.info(f"Columns renamed and partitions updated")
        return edito_parquet_path
    
    @staticmethod
    def _add_arco_mapping(layer: ('Layer')):
        """
        Adds ARCO mapping and variable descriptions to the layer's metadata.

        Checks the layer's metadata for 'native_arco_mapping' and 'native_variable_descriptions' attributes.
        If found, adds these attributes to the layer's metadata.
        """

        logger.info(f"Creating ARCO mapping for {layer.metadata['id']} {layer.metadata['name']}")
        native_arco_dict = {nativevar: CoreUtils.custom_slugify(layer.metadata['attributes'][nativevar].get('name_custom', nativevar))
                        for nativevar in layer.metadata['attributes']}
        
        logger.info(f"Adding ARCO variable description for {layer.metadata['id']} {layer.metadata['name']}")
        arco_variable_description = {CoreUtils.custom_slugify(layer.metadata['attributes'][nativevar].get('name_custom', nativevar)): description
                                 for nativevar, description in ((nativevar, layer.metadata['attributes'][nativevar].get('description', ''))
                                 for nativevar in layer.metadata['attributes']) if description}
        
        layer.metadata["native_arco_mapping"] = native_arco_dict
        layer.metadata["arco_variable_description"] = arco_variable_description
        return layer


