from earthaccess.results import DataGranule

__all__ = ['viirs_nrt', 'sen3_olci_nrt', 'sen3_syn']


def viirs_nrt(g: DataGranule) -> str: 
    return ''.join(g.data_links()[0].split('/')[-1].split('.')[1:3])[1:]


def sen3_olci_nrt(g: DataGranule) -> str: 
    return g.data_links()[0].split('/')[-1].split('.')[1]


def sen3_syn(g: DataGranule) -> str: 
    return g.data_links()[0].split('/')[-1].split('_')[7]