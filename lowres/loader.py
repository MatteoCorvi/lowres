from pathlib import Path

import earthaccess
from earthaccess.results import DataGranule

from lowres.products import match_products
from lowres.extract import assign_downloads

import xarray as xr


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


    def load_optical(self, bounding_box: list[float], resolution: float, *, 
                     viirs_bands: tuple[int] = (1, 2, 3),
                     sen3_bands: tuple[int] = (1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 16, 17, 18, 21),
                     epsg_code: str = 'EPSG:4326', 
                     buffer: int = 20, 
                     interp_method: str = 'linear',
                     ) -> list[xr.DataArray]:
        """
        Load granules into list of xarray DataArrays
    
        Parameters
        - bounding_box : list[float]
            Geographic coordinates defining the area of interest [min_lon, min_lat, max_lon, max_lat].
        - resolution : float
            Spatial resolution in the units of the specified coordinate system.
        
        Keyword Arguments
        - viirs_bands : tuple[int], optional
            VIIRS satellite bands to retrieve, by default (1, 2, 3).
        - sen3_bands : tuple[int], optional
            Sentinel-3 satellite bands to retrieve, by default 
            (1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 16, 17, 18, 21).
        - epsg_code : str, optional
            Target coordinate reference system code, by default 'EPSG:4326' (WGS84).
        - buffer : int, optional
            index buffering (over row col dimensions) when slicing around the bounding box, by default 20.
        - interp_method : str, optional
            Method used for interpolation during resampling, by default 'linear'.
            Other options may include 'nearest', 'cubic', etc.
    
        Returns
        - List containing the loaded xarray data arrays for the specified bands.

        """

        kwargs = dict(
            viirs_bands=viirs_bands,
            sen3_bands=sen3_bands,
            epsg_code=epsg_code, 
            buffer=buffer,
            interp_method=interp_method,
        )

        for product in self.products:

            product.timeseries = []

            for data in product.local_data:

                print(data)
                try:
                    xda = product.load(data, bounding_box, resolution, **kwargs)
                    product.timeseries.append(xda)
                except Exception as e:
                    print(f"{type(e).__name__}: {e}")

        return self