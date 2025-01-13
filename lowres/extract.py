from zipfile import ZipFile
from pathlib import Path
from earthaccess.results import DataGranule



def tstamp_viirs_nrt(g: DataGranule) -> str: 
    return ''.join(g.data_links()[0].split('/')[-1].split('.')[1:3])[1:]


def tstamp_sen3_olci_nrt(g: DataGranule) -> str: 
    return g.data_links()[0].split('/')[-1].split('.')[1]


def tstamp_sen3_syn(g: DataGranule) -> str: 
    return g.data_links()[0].split('/')[-1].split('_')[7]


def unzip_sen3_syn(zip_file):
    out_dir = Path(zip_file).with_suffix('.SEN3')
    if not out_dir.exists():
        with ZipFile(zip_file, 'r') as zip_ref:
            zip_ref.extractall(out_dir.parent)
    return str(out_dir)


def assign_downloads(products: list, downloads: list) -> None:

    i = 0
    for p in products:
        p.local_data = []
        for g in p.granules:
            l = len([url for url in g['umm']['RelatedUrls'] if url['Type'] == 'GET DATA'])
            idx = slice(i, i+l) if l>1 else i
            p.local_data.append(p.unzip(downloads[idx]))
            i += l