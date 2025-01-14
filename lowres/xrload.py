from pyhdf.SD import SD, SDC
import rioxarray as rxr # load .rio accessor
import xarray as xr
import numpy as np
import geopandas as gpd
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
                   epsg_code: str = 'EPSG:4326', buffer: int = 20, interp_method: str = 'linear') -> xr.DataArray:

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

    xda = (xr
        .DataArray(data, dims=('band', 'y', 'x'), coords={'band': bands})
        .isel(x=x_slice, y=y_slice)
    )
    xda = (xda
        .where(xda != nodata)
        .rio.write_nodata(np.nan, encoded=True)
    ) * scale_factor + add_offset

    xda = xda.rio.write_crs('EPSG:4326').rio.reproject(
        dst_crs=epsg_code, 
        resolution=resolution, 
        src_geoloc_array=(lon, lat), 
        georeferencing_convention='PIXEL_CENTER'
    )

    xda = xda.rio.interpolate_na(method=interp_method)
    
    bounds = gpd.GeoSeries([box(*bbox)], crs='EPSG:4326').to_crs(epsg_code).total_bounds
    xda = xda.rio.clip_box(*bounds)

    return xda



def load_sen3_syn(data_dir_path: str, bbox: list[float], resolution: float, *, 
                   epsg_code: str = 'EPSG:4326', buffer: int = 20, interp_method: str = 'linear') -> xr.DataArray:

    """
    Load Sentinel-3 OLCI geolocation and optical data to xarray DataArray clipped to provided bounding box.
    WARNING: Antimeridian crossing not covered.
    """

    xds = xr.open_dataset(data_dir_path + '/geolocation.nc', engine='netcdf4', decode_coords='all')
    xds = xds.rename({'rows': 'y', 'columns': 'x'})

    x_slice, y_slice = _get_geolocation_slices(xds.lon, xds.lat, bbox, buffer)

    lon = xds.lon.isel(x=x_slice, y=y_slice).values
    lat = xds.lat.isel(x=x_slice, y=y_slice).values

    bands = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 16, 17, 18, 21]
    bands = [f'Oa{b:02d}' for b in bands]
    data = [xr.open_dataset(data_dir_path + f'/Syn_{b}_reflectance.nc', engine='netcdf4', decode_coords='all')['SDR_'+b] for b in bands]

    #nodata = np.nan
    #scale_factor = 1e-4
    #add_offset = 0

    xda = (xr
        .DataArray(data, dims=('band', 'y', 'x'), coords={'band': bands})
        .isel(x=x_slice, y=y_slice)
    )
    #xda = (xda
    #    .where(xda != nodata)
    xda = xda.rio.write_nodata(np.nan, encoded=True)
    #) * scale_factor + add_offset

    xda = xda.rio.write_crs('EPSG:4326').rio.reproject(
        dst_crs=epsg_code, 
        resolution=resolution, 
        src_geoloc_array=(lon, lat), 
        georeferencing_convention='PIXEL_CENTER'
    )

    xda = xda.rio.interpolate_na(method=interp_method)
    
    bounds = gpd.GeoSeries([box(*bbox)], crs='EPSG:4326').to_crs(epsg_code).total_bounds
    xda = xda.rio.clip_box(*bounds)

    return xda