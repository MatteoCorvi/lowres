import os
from pyhdf.SD import SD, SDC
import numpy as np
import geopandas as gpd
import rioxarray as rxr # load .rio accessor
import xarray as xr
from geocube.api.core import make_geocube
from shapely.geometry import box



def _get_geolocation_slices(lon: xr.DataArray, lat: xr.DataArray, bbox: tuple[float], buffer: int) -> tuple[slice, slice]:

    lon_condition = (lon >= bbox[0]) & (lon <= bbox[2])
    lat_condition = (lat >= bbox[1]) & (lat <= bbox[3])
    condition = lon_condition & lat_condition

    x_matches = np.where(condition.any(dim='y'))[0]
    y_matches = np.where(condition.any(dim='x'))[0]

    x_slice = slice(x_matches[0] - buffer, x_matches[-1] + 1 + buffer)
    y_slice = slice(y_matches[0] - buffer, y_matches[-1] + 1 + buffer)

    return x_slice, y_slice



def load_viirs(data: list[str, str], bbox: list[float], resolution: float, *, 
                   epsg_code: str = 'EPSG:4326', buffer: int = 20, interp_method: str = 'linear') -> xr.Dataset:

    """
    Load VIIRS geolocation and optical 375m data to xarray DataArray clipped to provided bounding box.
    WARNING: Antimeridian crossing not covered.
    """

    spectral_data_path, geolocation_data_path = data

    geo_xds = xr.open_dataset(geolocation_data_path, group='geolocation_data', engine='netcdf4', decode_coords='all')
    geo_xds = geo_xds.rename({'number_of_lines': 'y', 'number_of_pixels': 'x'})

    x_slice, y_slice = _get_geolocation_slices(geo_xds.longitude, geo_xds.latitude, bbox, buffer)

    lon = geo_xds.longitude.isel(x=x_slice, y=y_slice).values
    lat = geo_xds.latitude.isel(x=x_slice, y=y_slice).values

    hdf = SD(spectral_data_path, SDC.READ)

    bands = ['I1', 'I2', 'I3']
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

    angles_map = {'sensor_azimuth': 'vaa', 'sensor_zenith': 'vza', 'solar_azimuth': 'saa', 'solar_zenith': 'sza'}
    xds = geo_xds.rename(angles_map)[['vaa', 'vza']].isel(x=x_slice, y=y_slice)

    sr = (xr
        .DataArray(data, dims=('band', 'y', 'x'), coords={'band': bands})
        .isel(x=x_slice, y=y_slice)
    )
    sr = (sr
        .where(sr != nodata)
        .rio.write_nodata(np.nan, encoded=True)
    ) * scale_factor + add_offset

    xds['sr'] = sr
    
    bounds = gpd.GeoSeries([box(*bbox)], crs='EPSG:4326').to_crs(epsg_code).total_bounds
    
    xds = (xds
        .rio.write_crs('EPSG:4326').rio.reproject(
            dst_crs=epsg_code, 
            resolution=resolution, 
            src_geoloc_array=(lon, lat), 
            georeferencing_convention='PIXEL_CENTER'
        )
        .rio.interpolate_na(method=interp_method)
        .rio.clip_box(*bounds))

    return xds



def load_sen3_syn(data_dir_path: str, bbox: list[float], resolution: float, *, 
                   epsg_code: str = 'EPSG:4326', buffer: int = 20, interp_method: str = 'linear') -> xr.Dataset:

    """
    Load Sentinel-3 OLCI geolocation and optical data to xarray DataArray clipped to provided bounding box.
    WARNING: Antimeridian crossing not covered.
    """

    geo_xds = xr.open_dataset(data_dir_path + '/geolocation.nc', engine='netcdf4', decode_coords='all')
    geo_xds = geo_xds.rename({'rows': 'y', 'columns': 'x'})

    x_slice, y_slice = _get_geolocation_slices(geo_xds.lon, geo_xds.lat, bbox, buffer)

    lon = geo_xds.lon.isel(x=x_slice, y=y_slice).values
    lat = geo_xds.lat.isel(x=x_slice, y=y_slice).values

    bands = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 16, 17, 18, 21]
    bands = [f'Oa{b:02d}' for b in bands]
    data = [xr.open_dataset(os.path.join(data_dir_path, f'Syn_{b}_reflectance.nc'), engine='netcdf4', decode_coords='all')['SDR_'+b] for b in bands]

    sr = (xr
        .DataArray(data, dims=('band', 'y', 'x'), coords={'band': bands})
        .isel(x=x_slice, y=y_slice)
    )

    #nodata = np.nan
    #scale_factor = 1e-4
    #add_offset = 0

    #xds = (xds
    #    .where(xds != nodata)
    sr = sr.rio.write_nodata(np.nan, encoded=True)
    #) * scale_factor + add_offset

    sr = sr.rio.write_crs('EPSG:4326').rio.reproject(
        dst_crs=epsg_code, 
        resolution=resolution, 
        src_geoloc_array=(lon, lat), 
        georeferencing_convention='PIXEL_CENTER'
    )

    sr = sr.rio.interpolate_na(method=interp_method)

    bounds = gpd.GeoSeries([box(*bbox)], crs='EPSG:4326').to_crs(epsg_code).total_bounds
    buf_bounds = gpd.GeoSeries([box(*bounds)], crs=epsg_code).buffer(1e5, join_style='mitre')
    sr = sr.rio.clip_box(*bounds)
    
    var_map = {'OLC_TP_lon': 'lon', 'OLC_TP_lat': 'lat', 'OLC_VAA': 'vaa', 'OLC_VZA': 'vza', 'SAA': 'saa', 'SZA': 'sza'}
    ang = xr.open_dataset(data_dir_path + '/tiepoints_olci.nc', engine='netcdf4', decode_coords='all').rename(var_map)[['vaa', 'vza']]
    gs = gpd.GeoSeries.from_xy(ang.lon.values, ang.lat.values, crs='EPSG:4326')

    ang = ang.drop_vars(['lon', 'lat'])
    gdf = gpd.GeoDataFrame(ang.to_dataframe(), geometry=gs).to_crs(epsg_code)
    gdf = gdf[gdf.geometry.within(buf_bounds.values[0])]

    xds = make_geocube(vector_data=gdf, resolution=(-resolution, resolution), interpolate_na_method=interp_method)
    xds = xds.rio.reproject_match(sr)

    xds['sr'] = sr

    return xds