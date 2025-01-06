import sys
from typing import Callable, ClassVar
from zipfile import ZipFile
from pathlib import Path
from fnmatch import fnmatch

from lowres import parse, xrload



class SatelliteProduct:
    """Base class for satellite data products"""
    PROD_ID: ClassVar[None] = None
    GEO_ID: ClassVar[None] = None
    PARSE: ClassVar[Callable] = lambda x: x
    LOAD: ClassVar[Callable] = lambda x: x
    UNZIP: ClassVar[Callable] = lambda x: x

    @classmethod
    def register(self, g):
        """monkey patch product as attribute to earthaccess DataGranule"""
        g.product = self
        return g



def _available_products() -> list[str]:
    """retrieve all product ids from Product classes with non None PROD_ID"""
    module = sys.modules[__name__]
    products = []
    for name in dir(module):
        obj = getattr(module, name)
        try:
            if issubclass(obj, SatelliteProduct) and obj.PROD_ID:
                products.append(obj)
        except TypeError:
            pass
    return products



def match_products(patterns: str | list[str]) -> list[str]:
    """
    matches patterns [provided ids] against targets [available ids] strings
    returns: list[SatelliteProduct]
    """
    matches = []
    targets = _available_products()
    for p in patterns if not isinstance(patterns, str) else [patterns]:
        matches += [t for t in targets if fnmatch(t.PROD_ID, p)]
    return matches                



class VIIRSProduct(SatelliteProduct):
    PROD_ID: ClassVar[str] = "VNP09_NRT"
    GEO_ID: ClassVar[str] = "VNP03IMG"
    PARSE: ClassVar[Callable] = parse.viirs_nrt
    LOAD: ClassVar[Callable] = xrload.load_viirs_nrt



def unzip_sen3_syn(zip_file):
    out_dir = Path(zip_file).with_suffix('.SEN3')
    if not out_dir.exists():
        with ZipFile(zip_file, 'r') as zip_ref:
            zip_ref.extractall(out_dir.parent)
    return str(out_dir)



class Sentinel3SYNProduct(SatelliteProduct):
    PARSE: ClassVar[Callable] = parse.sen3_syn
    LOAD: ClassVar[Callable] = xrload.load_sen3_syn
    UNZIP: ClassVar[Callable] = unzip_sen3_syn



class Sentinel3ASYNProduct(Sentinel3SYNProduct):
    PROD_ID: ClassVar[str] = "S3A_SY_2_SYN"



class Sentinel3BSYNProduct(Sentinel3SYNProduct):
    PROD_ID: ClassVar[str] = "S3B_SY_2_SYN"

