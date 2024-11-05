import pandas as pd
import pathlib

from SatPathParser import SatPathParser

"""
Possible noms de fichiers:

- PRE_REGRID_15min_hdf_FILE:
GOES-0750:      GEO_L1B-GOES1[23]_YYYY-MM-DDTHH-mm-ss_[N-S]_IR107_V1-0[4-6].hdf
GOESNG-0750:    GEO_L1B-GOES16_YYYY-MM-DDTHH-mm-ss_G_IR103_V1-06.hdf
GOESNG-1370:    GEO_L1B-GOES1[6-7]_YYYY-MM-DDTHH-mm-ss_G_IR103_V1-06.hdf
- PRE_REGRID_1h_nc_FILE:
directory: YYYY_MM_DD dans lequel je stocke:
GOES-0750:      GEO_L1B-GOES1[2-3]_YYYY-MM-DD_HH1-HH2.nc
GOESNG-0750:    GEO_L1B-GOES16_YYYY-MM-DD_HH1-HH2.nc
GOESNG-1370:    GEO_L1B-GOES1[7-8]_YYYY-MM-DD_HH1-HH2.nc
- REGRID_1h_FILE:
GOES-0750:      05deg_GEO_L1B-GOES1[2-3]_YYYY-MM-DD_HH1-HH2.nc
GOESNG-0750:    05deg_GEO_L1B-GOES16_YYYY-MM-DD_HH1-HH2.nc
GOESNG-1370:    05deg_GEO_L1B-GOES1[7-8]_YYYY-MM-DD_HH1-HH2.nc
"""


class ABIPathParser(SatPathParser):

    def __init__(self, file_url, regrid, hourly, year=None, month=None, day=None, start_hour=None, end_hour=None,
                 version=None, regrid_res_str=None, satellite=None):
        self.url = pathlib.Path(file_url)
        self.hourly = hourly
        self.regrid = regrid
        self.regrid_res = regrid_res_str
        self.year = int(year) if year is not None else year
        self.month = int(month) if month is not None else month
        self.day = int(day) if day is not None else day
        self.start_hour = int(start_hour) if start_hour is not None else start_hour
        self.end_hour = int(end_hour) if end_hour is not None else end_hour
        self.start_date = None
        self.end_date = None
        self.version = version
        self.satellite = satellite
        # if missing at elast 1 date info --> extract it from filename
        if any(val is None for val in [self.year, self.month, self.day, self.start_hour, self.start_date]):
            self.extract_date()
        if self.version is None:
            self.extract_version()
        if self.regrid and self.regrid_res is None:
            self.extract_regrid_res()
        if self.satellite is None:
            self.extract_satellite()

    def extract_date(self):
        filename = self.url.stem
        filename_split = filename.split('_')
        if not self.hourly:  # GEO_L1B-GOES1x_YYYY-MM-DDTHH-mm-ss_[NSG]_IR10x_V1-0[4-6].hdf
            start_date = pd.Timestamp(filename_split[2], tz='UTC')
            end_date = None
        else:  # GEO_L1B-GOES16_YYYY-MM-DD_HH1-HH2.nc or 05deg_GEO_L1B-GOES16_YYYY-MM-DD_HH1-HH2.nc
            hours = filename_split[-1].split('-')
            start_date = pd.Timestamp(f"{filename_split[-2]}T{hours[0]}")
            end_date = pd.Timestamp(f"{filename_split[-2]}T{hours[1]}")

        self.year = start_date.year
        self.month = start_date.month
        self.day = start_date.day
        self.start_hour = start_date.hour
        self.start_date = start_date
        self.end_date = end_date
        if end_date is not None:
            self.end_hour = end_date.hour

    def extract_version(self):
        if not self.hourly:  # GEO_L1B-GOES1x_YYYY-MM-DDTHH-mm-ss_[NSG]_IR10x_V1-0[4-6].hdf
            self.version = self.url.stem.split('_')[-1]
        else:
            self.version = None

    def extract_regrid_res(self):
        if 'deg' in self.url.stem:
            self.regrid_res = self.url.stem.split('_')[0]
            self.regrid = True
        else:
            self.regrid = False
            self.regrid_res = None

    def extract_satellite(self):
        filename_split = self.url.stem.split('_')
        if not self.hourly:  # GEO_L1B-GOES1x_YYYY-MM-DDTHH-mm-ss_[NSG]_IR10x_V1-0[4-6].hdf
            self.satellite = filename_split[1].split('-')[1]
        else:  # GEO_L1B-GOES16_YYYY-MM-DD_HH1-HH2.nc or 05deg_GEO_L1B-GOES16_YYYY-MM-DD_HH1-HH2.nc
            self.satellite = filename_split[-3].split('-')[1]

    def get_start_date_pdTimestamp(self):
        return pd.Timestamp(self.start_date)

    def print(self):
        for attr_key, attr_val in vars(self).items():
            print(f'{attr_key}: {attr_val}')