# lowres
Python utilities for data extraction and manipulation of low resolution/high revisit time satellite products like ESA's Sentinel-3 and NASA's VIIRS.

# example
``` python
import datetime as dt
from lowres.loader import EarthDataLoader

# Initialize loader
loader = EarthDataLoader("Sentinel3*", "VIIRS*STD*")

# Set search parameters
bounding_box = 7.8536809883066230, 44.6328246340051535, 12.0651228253536811, 45.5482371795341479
end = dt.datetime.now(dt.timezone.utc)
start = end - dt.timedelta(days=3)

# Search and download
loader.search(start, end, bounding_box)
loader.pull('./lowres_data')

# load list of xrray DataArrays
xda_list = loader.load_optical(bounding_box, 300, epsg_code='EPSG:32632')
```
