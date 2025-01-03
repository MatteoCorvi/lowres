from dataclasses import dataclass
from typing import Callable, ClassVar
from zipfile import ZipFile
from pathlib import Path

from lowres import parse, xrload



__all__ = [
    'VIIRSProduct',
    'Sentinel3SYNProduct',
    'Sentinel3ASYNProduct',
    'Sentinel3BSYNProduct',
    'Sentinel3CSYNProduct',
    'Sentinel3DSYNProduct',
]




@dataclass
class SatelliteProduct:
    """Base class for satellite data products"""
    PROD_ID: ClassVar[None] = None
    GEO_ID: ClassVar[None] = None
    parse_func: ClassVar[Callable] = lambda x: x
    load_func: ClassVar[Callable] = lambda x: x
    unzip_func: ClassVar[Callable] = lambda x: x




class VIIRSProduct(SatelliteProduct):
    PROD_ID: ClassVar[str] = "VNP09_NRT"
    GEO_ID: ClassVar[str] = "VNP03IMG"
    parse_func: ClassVar[Callable] = parse.viirs_nrt
    load_func: ClassVar[Callable] = xrload.load_viirs_nrt




def unzip_sen3_syn(zip_file):
    out_dir = Path(zip_file).with_suffix('.SEN3')
    with ZipFile(zip_file, 'r') as zip_ref:
        zip_ref.extractall(out_dir)
    return str(out_dir)


class Sentinel3SYNProduct(SatelliteProduct):
    PROD_ID: ClassVar[str] = "S3*_SY_2_SYN"
    parse_func: ClassVar[Callable] = parse.sen3_syn
    load_func: ClassVar[Callable] = xrload.load_sen3_syn
    unzip_func: ClassVar[Callable] = unzip_sen3_syn


class Sentinel3ASYNProduct(Sentinel3SYNProduct):
    PROD_ID: ClassVar[str] = "S3A_SY_2_SYN"


class Sentinel3BSYNProduct(Sentinel3SYNProduct):
    PROD_ID: ClassVar[str] = "S3B_SY_2_SYN"


class Sentinel3CSYNProduct(Sentinel3SYNProduct):
    PROD_ID: ClassVar[str] = "S3C_SY_2_SYN"


class Sentinel3DSYNProduct(Sentinel3SYNProduct):
    PROD_ID: ClassVar[str] = "S3D_SY_2_SYN"