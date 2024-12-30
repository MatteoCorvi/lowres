from pathlib import Path

import earthaccess
from earthaccess.results import DataGranule

import numpy as np

from lowres import parse
from lowres import xrload

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

    Sentinel-3A Olci Level-2 Near Real Time
    edl = EarthDataLoader("OLCIS3A_L2_EFR_IOP_NRT")
    """

    AVAILABLE_PRODUCTS = [
        "VNP09_NRT", 
        "OLCIS3*_L2_EFR_IOP_NRT", 
        "S3*_SY_2_SYN"
    ]

    _TSTAMP_PARSERS = {
        "VNP09_NRT": parse.viirs_nrt,
        "OLCIS3*_L2_EFR_IOP_NRT": parse.s3_olci_nrt,
        "S3*_SY_2_SYN": parse.s3_syn,
    }

    _LOAD_FUNCS = {
        "VNP09_NRT": xrload.load_viirs_nrt,
        "OLCIS3*_L2_EFR_IOP_NRT": xrload.load_s3_olci_nrt,
        "S3*_SY_2_SYN": xrload.load_s3_syn,
    }
    
    def __init__(self, prod_id: str, geo_prod_id: str|None = None) -> None:
        """
        Init Loader with prod_id (es `VNP09_NRT`, `) and geo_prod_id (es `VNP03IMG`, optional) 
        
        Parameters:
        - prod_id: str
        - geo_prod_id: str (optional)
        """

        self.prod_id = prod_id
        self.geo_prod_id = geo_prod_id

        self.normalized_prod_id = prod_id.replace("S3A_", "S3*_").replace("S3B_", "S3*_")

        assert self.normalized_prod_id in self.AVAILABLE_PRODUCTS, f"Date parsing not available for product `{prod_id}`."

        self._tstamp = self._TSTAMP_PARSERS.get(self.normalized_prod_id)
        self._load = self._LOAD_FUNCS.get(self.normalized_prod_id)

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
            "short_name": self.prod_id,
            "temporal": (start_date, end_date),
            "bounding_box": bounding_box,
        }
            
        granules = earthaccess.search_data(**search_params)

        if not self.geo_prod_id:
            remote_data = [(g,) for g in granules]

        else:
            search_params.update(short_name=self.geo_prod_id)
                
            geo_locations = earthaccess.search_data(**search_params)
            geo_locations_dict = {self._tstamp(g): g for g in geo_locations}

            remote_data = []
            for granule in granules:
                tstamp = self._tstamp(granule)
                geo_location = geo_locations_dict.get(tstamp)
                if geo_location:
                    remote_data.append((granule, geo_location))
                elif self.geo_prod_id:
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
        if self.geo_prod_id:
            local_data = [tuple(a) for a in np.array(local_data).reshape(-1, 2).tolist()]
        else:
            local_data = [(f,) for f in local_data]

        self.local_data = local_data

        return local_data


    def load_spectral_bands(self, bounding_box: list[float], resolution: float, epsg_code: str = 'EPSG:4326') -> list:

        timeseries = []
        for files in self.local_data:

            xds = self._LOAD_FUNCS.get(self.normalized_prod_id)(*files, bounding_box, resolution, epsg_code=epsg_code)
            timeseries.append(xds)

        return timeseries