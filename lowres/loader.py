from pathlib import Path

import earthaccess
from earthaccess.results import DataGranule

from lowres.products import match_products
from lowres.extract import assign_downloads



class EarthDataLoader:
    """
    Loader class for satellite data and associated geo encoding

    Examples:

    Sentinel-3A Synergy and VIIRS Level-2
    loader = EarthDataLoader("Sentinel3*", "VIIRS*STD*")
    """
    
    def __init__(self, *short_names: str | list[str]) -> None:
        """
        Init Loader with class names or earthdata search product shortnames)
        
        Parameters:
        - shortnames: str | list[str]
        """

        self.products = [product() for product in match_products(short_names)]
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

            if product.GEO_ID:
                search_params.update(short_name=product.GEO_ID)
                    
                geo_locations = earthaccess.search_data(**search_params)
                geo_locations_dict = {product.parse(g): g for g in geo_locations}

                for granule in granules:
                    tstamp = product.parse(granule)
                    geo_location = geo_locations_dict.get(tstamp)
                    if geo_location:
                        granule['umm']['RelatedUrls'] += geo_location['umm']['RelatedUrls']
                    else:
                        print(f"skip granule at {tstamp}, no geo_location found")
                
            product.granules = granules

        return self
    


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
    
        granules = [g for p in self.products for g in p.granules]

        local_data = earthaccess.download(granules, output_dir, threads=threads)

        assign_downloads(self.products, local_data)

        return self


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

        for product in self.products:

            product.timeseries = []

            for data in product.local_data:

                print(data)

                xda = product.load(data, bounding_box, resolution, epsg_code=epsg_code, **kwargs)
                product.timeseries.append(xda)

        return self