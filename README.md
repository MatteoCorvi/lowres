# lowres
Python utilities for data extraction and manipulation of low resolution/high revisit time satellite products like ESA's Sentinel-3 and NASA's VIIRS.

# example
``` python
import datetime as dt
from lowres.loader import EarthDataLoader

# Initialize loader
loader = EarthDataLoader("VNP09_NRT", geo_prod_id="VNP03IMG")
#loader = EarthDataLoader("S3B_SY_2_SYN")
#loader = EarthDataLoader("OLCIS3B_L2_EFR_IOP_NRT")

# Set search parameters
bounding_box = 7.8536809883066230, 44.6328246340051535, 12.0651228253536811, 45.5482371795341479
end = dt.datetime.now(dt.timezone.utc)
start = end - dt.timedelta(days=3)

# Search and download
loader.search(start, end, bounding_box)
loader.pull('./lowres_data')

# load list of xrray DataArrays
xda_list = loader.load_spectral_bands(bounding_box, 300, epsg_code='EPSG:32632')
```
