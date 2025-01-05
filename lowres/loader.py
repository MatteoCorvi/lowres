from pathlib import Path

import earthaccess
from earthaccess.results import DataGranule

from lowres.products import match_products

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
    
    def __init__(self, *short_names: str | list[str]) -> None:
        """
        Init Loader with product shortnames (es `VNP09_NRT`, `S3*_SY_2_SYN`)
        
        Parameters:
        - shortnames: str | list[str]
        """

        self.products = match_products(short_names)
        if not self.products:
            raise ValueError(f"`{short_names}` not available for lowres.EarthDataLoader.")

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

        self.granules = []

        for product in self.products:
            
            search_params = {
                "short_name": product.PROD_ID,
                "temporal": (start_date, end_date),
                "bounding_box": bounding_box,
            }
                
            granules = earthaccess.search_data(**search_params)

            if not product.GEO_ID:
                remote_data = [(product.register(g),) for g in granules]

            else:
                search_params.update(short_name=product.GEO_ID)
                    
                geo_locations = earthaccess.search_data(**search_params)
                geo_locations_dict = {product.PARSE(g): g for g in geo_locations}

                remote_data = []
                for granule in granules:
                    tstamp = product.PARSE(granule)
                    geo_location = geo_locations_dict.get(tstamp)
                    if geo_location:
                        remote_data.append((
                            product.register(granule), 
                            product.register(geo_location)
                        ))
                    elif product.GEO_ID:
                        print(f"skip granule at {tstamp}, no geo_location found")
                
            self.granules += remote_data

            self.granules.sort(key=lambda g: g[0]['umm']['TemporalExtent']['RangeDateTime']['BeginningDateTime'])

        return self.granules
    

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
    
        granules = [g for granules in self.granules for g in granules]

        local_data = earthaccess.download(granules, output_dir, threads=threads)

        for granule, local_path in zip(granules, local_data):
            granule.local_data = granule.product.UNZIP(local_path)

        return [tuple(g.local_data for g in t) for t in self.granules]


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
        for granule in self.granules:

            data = tuple(g.local_data for g in granule)
            xda = granule[0].product.LOAD(*data, bounding_box, resolution, epsg_code=epsg_code, **kwargs)
            timeseries.append(xda)

        return timeseries