import datetime
import pandas as pd
import pathlib

from .PathParser import PathParser

class GLMPathParser(PathParser):
    """
    expecting url of the form (default notation):
        FILES:
        - OR_GLM-L2-LCFA_G16_sYYYYDDDHHMMSSS_eYYYYDDDHHMMSSS_cYYYYDDDHHMMSSS.nc (raw 20sec)
        - OR_GLM-L2-LCFA_Gxx_YYYY_MM_DD_HH1-HH2.nc (raw hourly)
        - xxdeg_OR_GLM-L2-LCFA_Gxx_YYYY_MM_DD_HH1-HH2.nc (regrid hourly)
        DIRECTORIES:
        - OR_GLM-L2-LCFA_YYYY_MM_DD (<!> NO satellite nb, in pre_regrid_hourly_glm dir)
        - xxdeg_OR_GLM-L2-LCFA_YYYY_MM_DD (<!> NO satellite nb, in regrid_hourly_glm dir)
    """

    def __init__(self, file_url, regrid, hourly=True, year=None, month=None, day=None, day_of_year=None, start_hour=None, end_hour=None, regrid_res_str=None, satellite_version=None, directory=False):
        """

        @param file_url: str or pathlib object
        @param regrid: <bool>
        @param hourly: <bool>
        @param year: <int> or <str>
        @param month: <int> or <str>
        @param day: <int> or <str>
        @param start_hour: <int> or <str>
        @param end_hour: <int> or <str>
        @param regrid_res_str: <str> usually '05deg'
        @param satellite_version: <str>
        @param directory: <bool>
        """
        self.url = pathlib.Path(file_url)  # pathlib.Path object
        self.hourly = hourly
        self.regrid = regrid
        self.regrid_res = regrid_res_str
        self.satellite_version = satellite_version
        # file/dir name related attributes
        self.directory = directory
        # date attributes
        self.year = int(year) if year is not None else year
        self.month = int(month) if month is not None else month
        self.day = int(day) if day is not None else day
        self.day_of_year = int(day_of_year) if day_of_year is not None else day_of_year
        self.start_hour = int(start_hour) if start_hour is not None else start_hour
        self.end_hour = int(end_hour) if end_hour is not None else end_hour
        self.start_date = None
        # if we're missing at least 1 date info --> extract date from filename
        if any(val is None for val in [self.year, self.start_hour, self.month, self.day, self.day_of_year]):
            self.extract_date_from_filename()
        # extract missing values
        if end_hour is None and self.hourly and start_hour is not None:
            self.end_hour = start_hour + 1
        if self.regrid and self.regrid_res is None:
            self.extract_regrid_res()
        if self.satellite_version is None:
            self.extract_satellite()

    def extract_date_from_filename(self):
        filename = self.url.stem
        filename_split = filename.split('_')
        # if directory
        if self.directory:
            # (xxdeg_)OR_GLM-L2-LCFA_YYYY_MM_DD
            date = pd.Timestamp(f'{filename_split[-3]}-{filename_split[-2]}-{filename_split[-1]}')
            start_hour = 0
            end_hour = None
        # if file
        elif not self.hourly:
            # if not hourly --> raw 20 sec file
            # filename: OR_GLM-L2-LCFA_G16_sYYYYDDDHHMMSSS_eYYYYDDDHHMMSSS_cYYYYDDDHHMMSSS.nc
            # don't get the end_hour in this case for now
            start_date = filename_split[-3]  # recup sYYYYDDDHHMMSSS part
            date = pd.Timestamp(datetime.datetime.strptime(f'{start_date[1:5]}_{start_date[5:8]}_{start_date[8:10]}', '%Y_%j_%H'))
            start_hour = start_date[8:10]
            end_hour = None
        else:
            # (xxdeg_)OR_GLM-L2-LCFA_Gxx_YYYY_MM_DD_HH1-HH2.nc
            hour_split = filename_split[-1].split('-')
            date = pd.Timestamp(f'{filename_split[-4]}-{filename_split[-3]}-{filename_split[-2]}T{hour_split[0]}')
            start_hour = int(hour_split[0])
            end_hour = int(hour_split[1])

        if self.year is None:
            self.year = date.year
        if self.month is None:
            self.month = date.month
        if self.day is None:
            self.day = date.day
        if self.start_hour is None:
            self.start_hour = start_hour
        if self.end_hour is None:
            self.end_hour = end_hour
        self.start_date = date

    def extract_regrid_res(self):
        if 'deg' in self.url.stem:
            filename_split = self.url.stem.split('_')
            # xxdeg_OR_GLM-L2-LCFA_Gxx_YYYY_MM_DD_HH1-HH2.nc
            self.regrid_res = filename_split[0]
        else:
            self.regrid_res = None

    def extract_satellite(self):
        filename_split = self.url.stem.split('_')
        if self.directory: # (xxdeg_)OR_GLM-L2-LCFA_YYYY_MM_DD
            self.satellite_version = None
        elif self.hourly: # (xxdeg_)OR_GLM-L2-LCFA_Gxx_YYYY_MM_DD_HH1-HH2.nc
            self.satellite_version = filename_split[-5]
        else:
            self.satellite_version = filename_split[-4]

    def get_start_date_pdTimestamp(self, ignore_missing_start_hour=False):
        """
        Returns pd.Timestamp object of the start date of the GLM file / directory
        @param ignore_missing_start_hour: <bool> if we need timestamp for directory
        @return: <pandas.Timestamp> object
        """
        if self.start_date is None:
            start_hour_str = f'{self.start_hour:02d}' if self.start_hour is not None else "00"
            return pd.Timestamp(f'{self.year}-{self.month:02d}-{self.day:02d}T{start_hour_str}00')
        else:
            return pd.Timestamp(self.start_date)


    def print(self):
        for attr_key, attr_val in vars(self).items():
            print(f'{attr_key}: {attr_val}')

