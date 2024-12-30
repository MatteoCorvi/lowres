import warnings
from pyhdf.SD import SD, SDC
import rioxarray as rxr
import xarray as xr
import numpy as np
from rasterio.errors import NotGeoreferencedWarning
import geopandas as gpd
from shapely.geometry import box



def load_viirs_nrt(spectral_data: str, geolocation_data: str, bbox: list[float], resolution: float, *, 
                   epsg_code: str = 'EPSG:4326', buffer: int = 20, interp_method: str = 'linear'):

    """
    Load spectral geolocation and spectral data to xarray dataset.
    WARNING: Antimeridian case not covered.
    """

    with warnings.catch_warnings():
        warnings.simplefilter("ignore", category=NotGeoreferencedWarning)
        xds = xr.open_dataset(geolocation_data, group='geolocation_data', engine='netcdf4', decode_coords='all')
        xds = xds.rename({'number_of_lines': 'y', 'number_of_pixels': 'x'})

    lon_condition = (xds.longitude >= bbox[0]) & (xds.longitude <= bbox[2])
    lat_condition = (xds.latitude >= bbox[1]) & (xds.latitude <= bbox[3])
    condition = lon_condition & lat_condition

    x_matches = np.where(condition.any(dim='y'))[0]
    y_matches = np.where(condition.any(dim='x'))[0]

    x_slice = slice(x_matches[0] - buffer, x_matches[-1] + 1 + buffer)
    y_slice = slice(y_matches[0] - buffer, y_matches[-1] + 1 + buffer)

    lon = xds.longitude.isel(x=x_slice, y=y_slice).values
    lat = xds.latitude.isel(x=x_slice, y=y_slice).values

    hdf = SD(spectral_data, SDC.READ)

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



def load_s3_olci_nrt():
    pass



def load_s3_syn():
    pass