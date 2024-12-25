# lowres
Python utilities for data extraction and manipulation of low resolution/high revisit time satellite products like ESA's Sentinel-3 and NASA's VIIRS.

# example
``` python
from datetime import datetime, timedelta, timezone
from lowres.download import EarthDataDownloader

# Initialize downloader
edd = EarthDataDownloader("VNP09_NRT", geo_prod_id="VNP03IMG")
#edd = EarthDataDownloader("S3B_SY_2_SYN")
#edd = EarthDataDownloader("OLCIS3B_L2_EFR_IOP_NRT")

# Set search parameters
bounding_box = 7.8536809883066230, 44.6328246340051535, 12.0651228253536811, 45.5482371795341479
end = datetime.now(timezone.utc)
start = end - timedelta(days=3)

# Search and download
edd.search(start, end, bounding_box)
edd.download('./data')
```
