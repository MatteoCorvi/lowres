from pathlib import Path

import earthaccess
from earthaccess.results import DataGranule

__all__ = ['ViirsDownloader']


###################################################################################################################

class ViirsDownloader:
    """
    Downloader class for VIIRS satellite data and associated geo encoding files
    """
    
    def __init__(self, prod_id: str, geo_prod_id: str = "VNP03IMG_NRT") -> None:
        """
        Init Downloader with prod_id (es `VNP09_NRT`) and geo_prod_id (es `VNP03IMG_NRT`, optional) 
        
        Parameters:
        - prod_id: str
        - geo_prod_id: str (optional)

        """

        self.prod_id = prod_id
        self.geo_prod_id = geo_prod_id
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
            
        _tstamp = lambda g: ''.join(g.data_links()[0].split('/')[-1].split('.')[1:3])[1:]

        search_params = {
            "short_name": self.prod_id,
            "temporal": (start_date, end_date),
            "bounding_box": bounding_box,
        }
            
        granules = earthaccess.search_data(**search_params)

        search_params.update(short_name=self.geo_prod_id)
            
        geo_locations = earthaccess.search_data(**search_params)
        geo_locations_dict = {_tstamp(g): g for g in geo_locations}

        remote_data = []
        for granule in granules:
            remote_data.append(
                (granule, geo_locations_dict.get(_tstamp(granule)))
            )
            
        self.remote_data = remote_data

        return remote_data
    

    def fetch(self, output_dir: str|Path) -> list[tuple[str]]:
        """
        Download granules to specified directory
        
        Parameters:
        - output_dir: directory to save downloaded files to
        
        Returns:
        - list of tuples of downloaded file paths
        """

        output_path = Path(output_dir)
        output_path.mkdir(parents=True, exist_ok=True)
    
        local_data = []
        for granules in self.remote_data:
            downloads = earthaccess.download(
                list(granules),
                output_dir,
                threads=2
            )
            if downloads:
                local_data.append(tuple(downloads))

        self.local_data = local_data

        return local_data

