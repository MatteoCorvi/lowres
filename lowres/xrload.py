from pyhdf.SD import SD, SDC
import rioxarray as rxr # load .rio accessor
import xarray as xr
import xoak
import numpy as np
import pandas as pd
import geopandas as gpd
from shapely.geometry import box



def _get_geolocation_slices(lon: xr.DataArray, lat: xr.DataArray, bbox: tuple[float], buffer: int) -> tuple[slice, slice]:

    lon_condition = (lon >= bbox[0]) & (lon <= bbox[2])
    lat_condition = (lat >= bbox[1]) & (lat <= bbox[3])
    condition = lon_condition & lat_condition

    x_matches = np.where(condition.any(dim='y'))[0]
    y_matches = np.where(condition.any(dim='x'))[0]

    x_slice = slice(max(x_matches[0] - buffer, 0), x_matches[-1] + 1 + buffer)
    y_slice = slice(max(y_matches[0] - buffer, 0), y_matches[-1] + 1 + buffer)

    return x_slice, y_slice


def _overlap_to_nan(x):
    x_ = x.copy()
    diff = x_.dropna().diff()
    while (diff < 0).any():
        x_[diff[diff < 0].index] = np.nan
        diff = x_.dropna().diff()
    return x_


def load_viirs_to_raster(data: list[str, str], bbox: list[float], resolution: float, *, viirs_bands: tuple[int] = (1, 2, 3),
                   epsg_code: str = 'EPSG:4326', buffer: int = 20, **reproj_kwargs) -> xr.DataArray:

    """
    Load VIIRS geolocation and optical 375m data to xarray DataArray clipped to provided bounding box.
    WARNING: Antimeridian crossing not covered.
    """

    spectral_data_path, geolocation_data_path = data

    xds = xr.open_dataset(geolocation_data_path, group='geolocation_data', engine='netcdf4', decode_coords='all')
    xds = xds.rename({'number_of_lines': 'y', 'number_of_pixels': 'x'})

    x_slice, y_slice = _get_geolocation_slices(xds.longitude, xds.latitude, bbox, buffer)

    lon = xds.longitude.isel(x=x_slice, y=y_slice).values
    lat = xds.latitude.isel(x=x_slice, y=y_slice).values

    hdf = SD(str(spectral_data_path), SDC.READ)

    bands = [f"I{b}" for b in viirs_bands]
    data = [hdf.select(f'375m Surface Reflectance Band {b}') for b in bands]

    nodata = set(ds.attributes()['_FillValue'] for ds in data)
    assert len(nodata) == 1, 'Multiple `_FillValue` in band datasets'
    nodata = list(nodata)[0]

    scale_factor = set(ds.attributes()['scale_factor'] for ds in data)
    assert len(scale_factor) == 1, 'Multiple `scale_factor` in band in datasets'
    scale_factor = list(scale_factor)[0]

    add_offset = set(ds.attributes()['add_offset'] for ds in data)
    assert len(add_offset) == 1, 'Multiple `add_offset` in band datasets'
    add_offset = list(add_offset)[0]

    xda_sr = (xr
        .DataArray(data, dims=('band', 'y', 'x'), coords={'band': bands})
        .astype(np.float32)
        .isel(x=x_slice, y=y_slice)
    )
    
    xda_sr = (xda_sr.where(xda_sr != nodata) * scale_factor + add_offset).rio.write_nodata(np.nan, encoded=True)

    xda_flags = xds.land_water_mask.isel(x=x_slice, y=y_slice).astype(np.float32)

    nan_mask = np.isnan(xda_sr.data).any(axis=0)
    xda_flags.data[nan_mask] = np.nan

    lon[nan_mask] = np.nan
    lat[nan_mask] = np.nan

    lat = pd.DataFrame(lat).apply(_overlap_to_nan, axis=0).values.copy()
    nan_mask = np.isnan(lat)
    lon[nan_mask] = np.nan

    xda_sr = xda_sr.where(~np.expand_dims(nan_mask, axis=0))
    xda_flags = xda_flags.where(~nan_mask)

    xds_src = xr.Dataset(dict(SDR=xda_sr, LW_flags=xda_flags))

    xds_dst = xds_src.rio.write_crs('EPSG:4326').rio.reproject(
        dst_crs=epsg_code, 
        resolution=resolution, 
        src_geoloc_array=(lon, lat), 
        georeferencing_convention='PIXEL_CENTER',
        **reproj_kwargs
    )

    bounds = gpd.GeoSeries([box(*bbox)], crs='EPSG:4326').to_crs(epsg_code).total_bounds
    xds_dst = xds_dst.rio.clip_box(*bounds)

    mask = np.isnan(xds_dst.LW_flags.data)
    grid = np.meshgrid(xds_dst['x'].data, xds_dst['y'].data)
    nan_p = gpd.GeoSeries.from_xy(grid[0][mask].ravel(), grid[1][mask].ravel(), crs=xds_dst.rio.crs).to_crs('EPSG:4326')

    lat[np.isnan(lat)] = -9999
    lon[np.isnan(lon)] = -9999

    xds_src['lat'] = (('y', 'x'), lat)
    xds_src['lon'] = (('y', 'x'), lon)

    xds_src = xds_src.set_coords(('lat', 'lon'))
    xds_src.xoak.set_index(('lat', 'lon'), 'sklearn_balltree')

    nan_p_sel = xds_src.xoak.sel(lat=xr.DataArray(nan_p.y.values, dims='z'), lon=xr.DataArray(nan_p.x.values, dims='z'))

    xds_dst.LW_flags.data[mask] = nan_p_sel.LW_flags.data
    xds_dst.SDR.data[:, mask] = nan_p_sel.SDR.data

    return xds_dst



def load_sen3_syn_to_raster(data_dir_path: str, bbox: list[float], resolution: float, *, 
                   syn_sdr_bands: tuple[int] = (1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 16, 17, 18, 21),
                   syn_flags: tuple[str] = ('CLOUD_flags', 'OLC_flags'),
                   epsg_code: str = 'EPSG:4326', buffer: int = 20, **reproj_kwargs) -> xr.Dataset:

    """
    Load Sentinel-3 SYN-OLCI geolocation and optical data to xarray Dataset clipped to provided bounding box.
    WARNING: Antimeridian crossing not covered.
    """

    xds_geo = xr.open_dataset(data_dir_path + '/geolocation.nc', engine='netcdf4', decode_coords='all')[['lat', 'lon']]
    xds_geo = xds_geo.rename({'rows': 'y', 'columns': 'x'})

    x_slice, y_slice = _get_geolocation_slices(xds_geo.lon, xds_geo.lat, bbox, buffer)

    lon = xds_geo.lon.isel(x=x_slice, y=y_slice).values
    lat = xds_geo.lat.isel(x=x_slice, y=y_slice).values

    bands = [f'Oa{b:02d}' for b in syn_sdr_bands]
    data = [xr.open_dataset(data_dir_path + f'/Syn_{b}_reflectance.nc', engine='netcdf4', decode_coords='all')['SDR_'+b] for b in bands]

    #nodata = np.nan
    #scale_factor = 1e-4
    #add_offset = 0

    xda_sdr = (xr
        .DataArray(data, dims=('band', 'y', 'x'), coords={'band': bands})
        .astype(np.float32)
        .isel(x=x_slice, y=y_slice)
    )
    #xda = (xda
    #    .where(xda != nodata)
    xda_sdr = xda_sdr.rio.write_nodata(np.nan, encoded=True)
    #) * scale_factor + add_offset

    xds_flags = xr.open_dataset(data_dir_path + '/flags.nc', engine='netcdf4', decode_coords='all')[list(syn_flags)]
    xds_flags = xds_flags.rename({'rows': 'y', 'columns': 'x'}).isel(x=x_slice, y=y_slice)

    xds = xr.Dataset(dict(SDR=xda_sdr, **xds_flags))

    xds = xds.rio.write_crs('EPSG:4326').rio.reproject(
        dst_crs=epsg_code, 
        resolution=resolution, 
        src_geoloc_array=(lon, lat), 
        georeferencing_convention='PIXEL_CENTER',
        **reproj_kwargs
    )

    #xda = xda.rio.interpolate_na(method=interp_method)
    
    bounds = gpd.GeoSeries([box(*bbox)], crs='EPSG:4326').to_crs(epsg_code).total_bounds
    xds = xds.rio.clip_box(*bounds)

    return xds