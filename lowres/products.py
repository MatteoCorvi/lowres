import sys
from typing import ClassVar
from fnmatch import fnmatch

from lowres import extract, xrload



class SatelliteProduct:
    """Base class for satellite data products"""
    PROD_ID: ClassVar[str|None] = None
    GEO_ID: ClassVar[str|None] = None
    


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

    @property
    def parse(self):
        return extract.tstamp_viirs_nrt

    @property
    def unzip(self):
        return lambda x: x

    @property
    def load(self):
        return xrload.load_viirs_nrt



class Sentinel3SYNProduct(SatelliteProduct):

    @property
    def parse(self):
        return extract.tstamp_sen3_syn

    @property
    def unzip(self):
        return extract.unzip_sen3_syn

    @property
    def load(self):
        return xrload.load_sen3_syn



class Sentinel3ASYNProduct(Sentinel3SYNProduct):
    PROD_ID: ClassVar[str] = "S3A_SY_2_SYN"



class Sentinel3BSYNProduct(Sentinel3SYNProduct):
    PROD_ID: ClassVar[str] = "S3B_SY_2_SYN"

