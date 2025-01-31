import os
import dask
import xarray as xr
import zarr
import matplotlib.pyplot as plt
import numpy as np
import cartopy.crs as ccrs

# Read a zarr file

zarrurl = 'https://s3.waw3-1.cloudferro.com/emodnet/emodnet_geology/13662/EMODnet_Seabed_Substrate_1M.zarr/'

zarrurl = "https://s3.waw3-1.cloudferro.com/emodnet/emodnet_seabed_habitats/12658/seabed_par_all_europe_2005_2009.zarr"
ds = xr.open_zarr(zarrurl)
print(ds)


var = 'seabed_par_all_europe_2005_2009'
dsplot = ds[var].sel(latitude=slice(-10, 60), longitude=slice(-10, 40))
fig = plt.figure(figsize=(10, 10))

ax = plt.axes(projection=ccrs.PlateCarree())
dsplot.plot(ax=ax, transform=ccrs.PlateCarree(), cmap='viridis', robust=True)
ax.coastlines()
plt.savefig('seabed_par_all_europe_2005_2009.png')