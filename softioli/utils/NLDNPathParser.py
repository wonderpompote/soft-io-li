import pathlib
import pandas as pd

from .PathParser import PathParser


class NLDNPathParser(PathParser):

    def __init__(self, file_url, regrid, hourly=True, directory=False, year=None, month=None, day=None, start_hour=None,
                 end_hour=None, regrid_res_str=None, ):
        self.url = pathlib.Path(file_url)
        self.hourly = hourly
        self.regrid = regrid
        self.regrid_res = regrid_res_str
        self.directory = directory
        self.year = int(year) if year is not None else year
        self.month = int(month) if month is not None else month
        self.day = int(day) if day is not None else day
        self.start_hour = int(start_hour) if start_hour is not None else start_hour
        self.end_hour = int(end_hour) if end_hour is not None else end_hour
        self.start_date = None
        self.end_date = None
        # if missing at elast 1 date info --> extract it from filename
        if any(val is None for val in [self.year, self.month, self.day, self.start_hour, self.start_date]):
            self.extract_date()
        if self.regrid and self.regrid_res is None:
            self.extract_regrid_res()

    def extract_date(self):
        filename = self.url.stem
        filename_split = filename.split('_')
        if self.directory:  # (05deg_)NLDN_YYYY_MM_DD
            start_date = pd.Timestamp(f'{filename_split[-3]}-{filename_split[-2]}-{filename_split[-1]}')
            end_date = None
        elif self.hourly:  # (05deg_)NLDN_YYYY_MM_DD_HH1-HH2.nc
            hours = filename_split[-1].split('-')
            start_date = pd.Timestamp(f"{filename_split[-4]}-{filename_split[-3]}-{filename_split[-2]}T{hours[0]}00")
            if hours[0] == 23:
                end_date = pd.Timestamp(f"{filename_split[-4]}-{filename_split[-3]}-{filename_split[-2]}T{hours[0]}59")
            else:
                end_date = pd.Timestamp(f"{filename_split[-4]}-{filename_split[-3]}-{filename_split[-2]}T{hours[1]}00")
        else:
            raise ValueError(f'Unsupported NLDN path {self.url}')

        self.year = start_date.year
        self.month = start_date.month
        self.day = start_date.day
        self.start_hour = start_date.hour
        self.start_date = start_date
        self.end_date = end_date if not self.directory else None
        if end_date is not None:
            self.end_hour = end_date.hour

    def extract_regrid_res(self):
        if 'deg' in self.url.stem:
            self.regrid_res = self.url.stem.split('_')[0]
            self.regrid = True
        else:
            self.regrid = False
            self.regrid_res = None

    def extract_satellite(self):
        pass

    def get_start_date_pdTimestamp(self):
        return pd.Timestamp(self.start_date)

    def print(self):
        for attr_key, attr_val in vars(self).items():
            print(f'{attr_key}: {attr_val}')
