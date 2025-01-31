import os
import dask
import dask.dataframe as dd
import pandas as pd
import geopandas as gpd
import pyarrow as pa
import dask_geopandas as dgpd
import s3fs
import urllib
from dask.distributed import Client, LocalCluster
import matplotlib.pyplot as plt
import cartopy.crs as ccrs
from shapely import wkb


# Read a parquet file
host = 'https://s3.waw3-1.cloudferro.com/'
bucket = 'emodnet'
key = 'emodnet_biology/12639/eurobis_obisenv_view_07-01-2025.parquet'
# key = 'emodnet_geology/13662/EMODnet_Seabed_Substrate_1M.parquet'

url = "https://s3.waw3-1.cloudferro.com/emodnet/emodnet_biology/12639/eurobis_obisenv_view_07-01-2025.parquet"
def read_parquet_from_s3(url):
    """
    Read a Parquet file from a specified S3 URL. The URL should be in the format 's3://bucket/key'.

    :param download_url: The URL of the S3 asset to read.
    :type download_url: str
    :return: A Dask dataframe containing the Parquet data.
    :rtype: dask.dataframe.DataFrame
    """
    parsed_url = urllib.parse.urlparse(url)
    net_loc = parsed_url.netloc
    object_key = parsed_url.path.lstrip('/')
    s3_uri = f's3://{object_key}'
    endpoint_url = f'https://{net_loc}'
    fs = s3fs.S3FileSystem(anon=True, use_ssl=True, client_kwargs={'endpoint_url': endpoint_url})
    ddf = dd.read_parquet(
        s3_uri,
        storage_options={'anon': True, 'client_kwargs' : {'endpoint_url': endpoint_url}},
        engine='pyarrow',
        #ignore_metadata_file=True
    )
    return ddf


if __name__ == '__main__':
    cluster = LocalCluster(n_workers=1, threads_per_worker=2, memory_limit='9GB')
    client1 = Client(cluster)

    url = 'https://s3.waw3-1.cloudferro.com/emodnet/emodnet_seabed_habitats/12549/EUSeaMap_2023_geoparquet.parquet'
    url = "https://s3.waw3-1.cloudferro.com/emodnet/emodnet_seabed_habitats/12549/EUSeaMap_2023_geoparquet_partitioned.parquet"
    #url = "https://s3.waw3-1.cloudferro.com/emodnet/emodnet_seabed_habitats/12549/EUSeaMap_2023_parquet_partitioned.parquet/"
    # url = "https://s3.waw3-1.cloudferro.com/emodnet/emodnet_geology/13662/EMODnet_Seabed_Substrate_1M.parquet"
    # url = "https://s3.waw3-1.cloudferro.com/emodnet/emodnet_geology/13662/EMODnet_Seabed_Substrate_1M_parquet_partitioned.parquet"
    
    # url = "https://s3.waw3-1.cloudferro.com/emodnet/emodnet_geology/13662/EMODnet_Seabed_Substrate_1M_geoparquet_partitioned.parquet"
    url = "https://s3.waw3-1.cloudferro.com/emodnet/biology/eurobis_occurrence_data/eurobis_occurrences_geoparquet_2024-10-01.parquet"
    url = "https://s3.waw3-1.cloudferro.com/emodnet/emodnet_biology/12639/eurobis_obisenv_full_geopq-09-01-2025.parquet"
    ddf = read_parquet_from_s3(url)

    print(ddf)
    total_rows = len(ddf)
    # Print the first few rows of the dataframe
    for pt in ddf.partitions:
        print(len(pt))
        print(pt.head())
        pt.compute()
        pt.persist()
        print(pt.columns)
    # Plot the data
    ddf = ddf.persist()

    def load_geometry(x):
        return wkb.loads(x)
    if 'geometry' in ddf.columns:
        ddf['geometry'] = ddf.map_partitions(lambda df: df['geometry'].apply(load_geometry), meta=('geometry', 'object'))
        ddf.compute()
        gdf = gpd.GeoDataFrame(ddf, columns=ddf.columns)
        gdf = gdf.set_geometry('geometry')
        fig = plt.figure(figsize=(10, 10))

        ax = plt.axes(projection=ccrs.PlateCarree())
        col = 'folk_5_seabed_substrate_class'
        gdf.plot(column=col, legend=True)
        plt.show()
            

    client1.close()
