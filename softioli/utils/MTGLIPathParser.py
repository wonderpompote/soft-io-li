import pandas as pd
import pathlib

from .PathParser import PathParser

"""
Filenames:
- PRE_REGRID_10sec_nc_FILE:
W_XX-EUMETSAT-Darmstadt,IMG+SAT,MTI1+LI-2-LFL--FD--CHK-BODY---NC4E_C_EUMT_YYYYMMDDHHmmss_L2PF_OPE_YYYYMMDDHHmmss1_YYYYMMDDHHmmss2_N__T_xxxx_xxxx.nc

- PRE_REGRID_1h_nc_FILE:
directory: MTG_I1_LI_YYYYMMDD #TODO: change to MTG_I1_LI_YYYY_MM_DD
MTG_I1_LI_YYYY_MM_DD_HH1-HH2.nc

- REGRID_1h_FILE:
directory: xxdeg_MTG_I1_LI_YYYYMMDD
xxdeg_MTG_I1_LI_YYYY_MM_DD_HH1-HH2.nc
"""


class MTGLIPathParser(PathParser):

    def __init__(self, file_url, regrid, hourly=True, directory=False, year=None, month=None, day=None, start_hour=None, start_minute=None, end_hour=None,
                 file_version=None, regrid_res_str=None, satellite='', naming_convention=None):
        self.url = pathlib.Path(file_url)
        self.hourly = hourly
        self.regrid = regrid
        self.regrid_res = regrid_res_str
        self.directory = directory
        self.year = int(year) if year is not None else year
        self.month = int(month) if month is not None else month
        self.day = int(day) if day is not None else day
        self.start_hour = int(start_hour) if start_hour is not None else start_hour
        self.start_minute = int(start_minute) if start_minute is not None else start_minute
        self.end_hour = int(end_hour) if end_hour is not None else end_hour
        self.start_date = None
        self.end_date = None
        self.file_version = file_version
        self.satellite_version = satellite
        # if missing at least 1 date info --> extract it from filename
        if any(val is None for val in [self.year, self.month, self.day, self.start_hour, self.start_date]):
            self.extract_date_from_filename()
        if self.file_version is None:
            self.extract_file_version()
        if self.regrid and self.regrid_res is None:
            self.extract_regrid_res()
        if self.satellite_version is None:
            self.extract_satellite()

    def extract_date_from_filename(self):
        filename = self.url.stem
        filename_split = filename.split('_')
        if self.directory: # (xxdeg_)MTG_I1_LI_YYYYMMDD
            start_date = pd.Timestamp(f'{filename_split[-1][:4]}-{filename_split[-1][4:6]}-{filename_split[-1][-2:]}')
            end_date = None
        elif not self.hourly:  # W_XX-EUMETSAT-Darmstadt,IMG+SAT,MTI1+LI-2-LFL--FD--CHK-BODY---NC4E_C_EUMT_YYYYMMDDHHmmss_L2PF_OPE_YYYYMMDDHHmmss1_YYYYMMDDHHmmss2_N__T_xxxx_xxxx.nc
            start_date = pd.Timestamp(filename_split[7], tz='UTC')
            end_date = pd.Timestamp(filename_split[8], tz='UTC')
        else:  # (xxdeg_)MTG_I1_LI_YYYY_MM_DD_HH1-HH2.nc
            hours = filename_split[-1].split('-')
            start_date = pd.Timestamp(f'{filename_split[-4]}-{filename_split[-3]}-{filename_split[-2]}T{hours[0]}00')
            if hours[0] == '23':
                end_date = pd.Timestamp(f'{filename_split[-4]}-{filename_split[-3]}-{filename_split[-2]}T{hours[0]}59')
            else:
                end_date = pd.Timestamp(f'{filename_split[-4]}-{filename_split[-3]}-{filename_split[-2]}T{hours[1]}00')

        self.year = start_date.year
        self.month = start_date.month
        self.day = start_date.day
        self.start_hour = start_date.hour
        self.start_date = start_date
        self.end_date = end_date if not self.directory else None
        if end_date is not None:
            self.end_hour = end_date.hour

    def extract_file_version(self):
        self.file_version = None

    def extract_regrid_res(self):
        if 'deg' in self.url.stem:
            self.regrid_res = self.url.stem.split('_')[0]
            self.regrid = True
        else:
            self.regrid = False
            self.regrid_res = None

    def extract_satellite(self):
        self.satellite_version = None

    def get_start_date_pdTimestamp(self, ignore_missing_start_hour=False):
        return pd.Timestamp(self.start_date)

    def print(self):
        for attr_key, attr_val in vars(self).items():
            print(f'{attr_key}: {attr_val}')
