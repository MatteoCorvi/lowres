from pathlib import Path

import earthaccess
from earthaccess.results import DataGranule

import numpy as np

from lowres import products

__all__ = ['EarthDataLoader']


###################################################################################################################

class EarthDataLoader:
    """
    Loader class for satellite data and associated geo encoding

    Examples:

    VIIRS Near Real Time
    edl = EarthDataLoader("VNP09_NRT", geo_prod_id="VNP03IMG")

    Sentinel-3A Synergy Level-2
    edl = EarthDataLoader("S3A_SY_2_SYN")
    """

    _PRODUCTS = {
        "VNP09_NRT": products.VIIRSProduct, 
        "S3*_SY_2_SYN": products.Sentinel3SYNProduct,
        "S3A_SY_2_SYN": products.Sentinel3ASYNProduct,
        "S3B_SY_2_SYN": products.Sentinel3BSYNProduct,
    }
    
    def __init__(self, prod_id: str) -> None:
        """
        Init Loader with prod_id (es `VNP09_NRT`, `S3*_SY_2_SYN`)
        
        Parameters:
        - prod_id: str
        """

        self.product = self._PRODUCTS.get(prod_id)
        if self.product is None:
            raise ValueError(f"`{prod_id}` not available for lowres.EarthDataLoader.")

        self._setup_auth()
        

    def _setup_auth(self) -> None:
        """Set up NASA Earthdata authentication"""

        auth = earthaccess.login()
        if not auth.authenticated:
            raise Exception("Authentication to NASA Earthdata failed.")
        self.auth = auth
    

    def search(self, start_date: str, end_date: str, bounding_box: list[float]) -> list[tuple[DataGranule]]:
        """
        Search for granules
        
        Parameters:
        - start_date: datetime object
        - end_date: datetime object
        - bounding_box: list [west, south, east, north]
        
        Returns:
        - list of tuples of EarthAccess DataGranules
        """
        
        search_params = {
            "short_name": self.product.PROD_ID,
            "temporal": (start_date, end_date),
            "bounding_box": bounding_box,
        }
            
        granules = earthaccess.search_data(**search_params)

        if not self.product.GEO_ID:
            remote_data = [(g,) for g in granules]

        else:
            search_params.update(short_name=self.product.GEO_ID)
                
            geo_locations = earthaccess.search_data(**search_params)
            geo_locations_dict = {self.product.parse_func(g): g for g in geo_locations}

            remote_data = []
            for granule in granules:
                tstamp = self.product.parse_func(granule)
                geo_location = geo_locations_dict.get(tstamp)
                if geo_location:
                    remote_data.append((granule, geo_location))
                elif self.product.GEO_ID:
                    print(f"skip granule at {tstamp}, no geo_location found")
            
        self.remote_data = remote_data

        return remote_data
    

    def pull(self, output_dir: str|Path, threads: int = 4) -> list[tuple[str]]:
        """
        Download granules to specified directory
        
        Parameters:
        - output_dir: directory to save downloaded files to
        
        Returns:
        - list of tuples of downloaded file paths
        """

        output_path = Path(output_dir)
        output_path.mkdir(parents=True, exist_ok=True)
    
        local_data = earthaccess.download(
            np.array(self.remote_data).ravel().tolist(),
            output_dir,
            threads=threads
        )
        if self.product.GEO_ID:
            local_data = [self.product.unzip_func(tuple(a)) for a in np.array(local_data).reshape(-1, 2).tolist()]
        else:
            local_data = [(self.product.unzip_func(f),) for f in local_data]

        self.local_data = local_data

        return local_data


    def load_optical(self, bounding_box: list[float], resolution: float, epsg_code: str = 'EPSG:4326', **kwargs) -> list:
        """
        Load granules into list of xarray DataArrays

        Parameters:
        - bounding_box: list[float]
        - resolution: float
        - epsg_code: str = 'EPSG:4326'
        - **kwargs: othar keyword arguments to be provided to load functions
        
        Returns:
        - list of xarray DataArrays

        """

        timeseries = []
        for data_path in self.local_data:

            xda = self.product.load_func(*data_path, bounding_box, resolution, epsg_code=epsg_code, **kwargs)
            timeseries.append(xda)

        return timeseries