import sys
from typing import ClassVar
from fnmatch import fnmatch

from lowres import extract, xrload



class SatelliteProduct:
    """Base class for satellite data products"""
    PROD_ID: ClassVar[str|None] = None
    GEO_ID: ClassVar[str|None] = None
    


def _available_products(module_name: str = __name__) -> list[str]:
    """retrieve all product ids from Product classes with non None PROD_ID"""
    module = sys.modules[module_name]
    products = []
    for name in dir(module):
        obj = getattr(module, name)
        try:
            if issubclass(obj, SatelliteProduct) and obj.PROD_ID:
                products.append(obj)
        except TypeError:
            pass
    return products



def match_products(patterns: str | list[str], module_name: str = __name__) -> list[str]:
    """
    matches patterns [provided ids] against targets [available ids] strings
    search patterns are normalized
    returns: list[SatelliteProduct]
    """
    def check(ins):
        if ins is None: raise TypeError('`None` is not valid pattern input')
        if not ins: raise ValueError('Empty list or string is not valid pattern input')

    check(patterns)

    matches = []
    targets = _available_products(module_name)
    targets = [(t, t.PROD_ID.upper(), t.__name__.upper()) for t in targets]

    for p in patterns if not isinstance(patterns, str) else [patterns]:
        check(p)
        p_norm = p.upper()
        for t, id_norm, n_norm in targets:
            if fnmatch(id_norm, p_norm) or fnmatch(n_norm, p_norm):
                matches.append(t)
                
    return matches                



class VIIRS_Product(SatelliteProduct):

    @property
    def parse(self):
        return extract.tstamp_viirs

    @property
    def unzip(self):
        return lambda x: x

    @property
    def load(self):
        return xrload.load_viirs


class VIIRS_NPP_NRT_Product(VIIRS_Product):
    """
    VIIRS/NPP Atmospherically Corrected Surface Reflectance 
    6-Min L2 Swath 375m, 750m - Near Real Time
    """
    PROD_ID: ClassVar[str] = "VNP09_NRT"
    GEO_ID: ClassVar[str] = "VNP03IMG_NRT"


class VIIRS_JPSS1_NRT_Product(VIIRS_Product):
    """
    VIIRS/JPSS1 Atmospherically Corrected Surface Reflectance 
    6-Min L2 Swath 375m, 750m - Near Real Time
    """
    PROD_ID: ClassVar[str] = "VJ109_NRT"
    GEO_ID: ClassVar[str] = "VJ103IMG_NRT"


class VIIRS_NPP_STD_Product(VIIRS_Product):
    """
    VIIRS/NPP Atmospherically Corrected Surface Reflectance 
    6-Min L2 Swath 375m, 750m - Standard Science Product
    """
    PROD_ID: ClassVar[str] = "VNP09"
    GEO_ID: ClassVar[str] = "VNP03IMG"


class VIIRS_JPSS1_STD_Product(VIIRS_Product):
    """
    VIIRS/JPSS1 Atmospherically Corrected Surface Reflectance 
    6-Min L2 Swath 375m, 750m - Standard Science Product
    """
    PROD_ID: ClassVar[str] = "VJ109"
    GEO_ID: ClassVar[str] = "VJ103IMG"


class Sentinel3_SYN_Product(SatelliteProduct):

    @property
    def parse(self):
        return extract.tstamp_sen3_syn

    @property
    def unzip(self):
        return extract.unzip_sen3_syn

    @property
    def load(self):
        return xrload.load_sen3_syn


class Sentinel3A_SYN_Product(Sentinel3_SYN_Product):
    PROD_ID: ClassVar[str] = "S3A_SY_2_SYN"


class Sentinel3B_SYN_Product(Sentinel3_SYN_Product):
    PROD_ID: ClassVar[str] = "S3B_SY_2_SYN"

