import datetime
import pandas as pd
import pathlib


# naming conventions
OLD_GLM_NOTATION = 'OLD'  # GLM_array(_05deg)_DDD for dirs and # GLM_array(_xxdeg)_DDD_HH1-HH2.nc for files
OLD_GLM_PRE_REGRID_TEMP_FILENAME = 'OLD_TEMP'  # GLM_array_DDD_temp_HH.nc
OLD_GLM_MACC_PRE_REGRID_DIRNAME = 'MACC_DIR'  # OR_GLM-L2-LCFA_Gxx_sYYYYDDD

class GLMPathParser:
    """
    expecting url of the form (default notation):
        FILES:
        - OR_GLM-L2-LCFA_G16_sYYYYDDDHHMMSSS_eYYYYDDDHHMMSSS_cYYYYDDDHHMMSSS.nc (raw 20sec)
        - OR_GLM-L2-LCFA_Gxx_YYYY_DDD_HH1-HH2.nc (raw hourly)
        - 05deg_OR_GLM-L2-LCFA_Gxx_YYYY_DDD_HH1-HH2.nc (regrid hourly)
        DIRECTORIES:
        - OR_GLM-L2-LCFA_YYYY_DDD (<!> NO satellite nb, in pre_regrid_hourly_glm dir)
        - 05deg_OR_GLM-L2-LCFA_YYYY_DDD (<!> NO satellite nb, in regrid_hourly_glm dir)
        - OR_GLM-L2-LCFA_Gxx_sYYYYDDD (raw hourly macc for now) <!>
    """

    def __init__(self, file_url, regrid, hourly=True, year=None, day_of_year=None, start_hour=None, end_hour=None,
                 regrid_res_str=None, satellite_version=None, directory=False, naming_convention=None):
        """

        @param file_url: str or pathlib object
        @param regrid: <bool>
        @param hourly: <bool>
        @param year: <int> or <str>
        @param day_of_year: <int> or <str>
        @param start_hour: <int> or <str>
        @param end_hour: <int> or <str>
        @param regrid_res_str: <str> usually '05deg'
        @param satellite_version: <str>
        @param directory: <bool>
        @param naming_convention: <str>, Describes the file/directory naming convention. Supported values: 'OLD', 'OLD_TEMP', 'MACC_DIR' or None (if default notation)
        """
        self.url = pathlib.Path(file_url)  # pathlib.Path object
        self.hourly = hourly
        self.regrid = regrid
        self.regrid_res = regrid_res_str
        self.satellite_version = satellite_version
        # file/dir name related attributes
        self.directory = directory
        if naming_convention not in {OLD_GLM_NOTATION, OLD_GLM_PRE_REGRID_TEMP_FILENAME, OLD_GLM_MACC_PRE_REGRID_DIRNAME, None}:
            raise ValueError(f'Naming convention {naming_convention} NOT supported. Expecting {OLD_GLM_NOTATION}, {OLD_GLM_PRE_REGRID_TEMP_FILENAME}, {OLD_GLM_MACC_PRE_REGRID_DIRNAME} or None')
        self.naming_convention = naming_convention
        # date attributes
        self.year = int(year) if year is not None else year
        self.day_of_year = int(day_of_year) if day_of_year is not None else day_of_year
        self.start_hour = int(start_hour) if start_hour is not None else start_hour
        self.end_hour = int(end_hour) if end_hour is not None else end_hour
        # if we're missing at least 1 date info --> extract date from filename
        if any(val is None for val in [self.year, self.day_of_year, self.start_hour]):
            self.extract_missing_date()
        # extract missing values
        if end_hour is None and self.hourly and start_hour is not None:
            self.end_hour = start_hour + 1
        if self.regrid and self.regrid_res is None:
            self.extract_regrid_res()
        if self.satellite_version is None:
            self.extract_satellite()
        self.start_datetime = self.get_start_date_pdTimestamp()



    def extract_missing_date(self):
        filename = self.url.stem
        filename_split = filename.split('_')
        # if directory
        if self.directory:
            if self.naming_convention == OLD_GLM_NOTATION: # GLM_array(_05deg)_DDD
                date = {
                    "year": None,
                    "day_of_year": int(filename_split[-1]),
                    "start_hour": None,
                    "end_hour": None
                }
            elif self.naming_convention == OLD_GLM_MACC_PRE_REGRID_DIRNAME: # OR_GLM-L2-LCFA_Gxx_sYYYYDDD
                date = {
                    "year": filename_split[-1][1:5], # YYYY part of sYYYYDDD
                    "day_of_year": filename_split[-1][5:8], # DDD part of sYYYYDDD
                    "start_hour": None,
                    "end_hour": None
                }
            else: # (xxdeg_)OR_GLM-L2-LCFA_YYYY_DDD
                date = {
                    "year": int(filename_split[-2]),
                    "day_of_year": int(filename_split[-1]),
                    "start_hour": None,
                    "end_hour": None
                }
        # if file
        elif not self.hourly:
            # if not hourly --> raw 20 sec file
            # filename: OR_GLM-L2-LCFA_G16_sYYYYDDDHHMMSSS_eYYYYDDDHHMMSSS_cYYYYDDDHHMMSSS.nc
            # don't get the end_hour in this case for now
            start_date = filename_split[-3]  # recup sYYYYDDDHHMMSSS part
            date = {
                "year": int(start_date[1:5]), # YYYY part of sYYYYDDDHHMMSSS
                "day_of_year": int(start_date[5:8]), # DDD part of sYYYYDDDHHMMSSS
                "start_hour": int(start_date[8:10]), # HH part of sYYYYDDDHHMMSSS
                "end_hour": None
            }
        elif self.naming_convention == OLD_GLM_NOTATION:  # GLM_array(_xxdeg)_DDD_HH1-HH2.nc
            # if old_glm_filename --> year = 2018
            hour_split = filename_split[-1].split('-')
            date = {
                "year": 2018,
                "day_of_year": int(filename_split[-2]),
                "start_hour": int(hour_split[0]),
                "end_hour": int(hour_split[1])
            }
        elif self.naming_convention == OLD_GLM_PRE_REGRID_TEMP_FILENAME: # GLM_array_DDD_temp_HH.nc
            # if old_glm_filename --> year = 2018
            date = {
                "year": 2018,
                "day_of_year": int(filename_split[-3]),
                "start_hour": int(filename_split[-1]),
                "end_hour": int(filename_split[-1]) + 1
            }
        else:  # OR_GLM-L2-LCFA_Gxx_YYYY_DDD_HH1-HH2.nc or xxdeg_OR_GLM-L2-LCFA_Gxx_YYYY_DDD_HH1-HH2.nc
            hour_split = filename_split[-1].split('-')
            date = {
                "year": int(filename_split[-3]),
                "day_of_year": int(filename_split[-2]),
                "start_hour": int(hour_split[0]),
                "end_hour": int(hour_split[1])
            }

        if self.year is None:
            self.year = date['year']
        if self.day_of_year is None:
            self.day_of_year = date['day_of_year']
        if self.start_hour is None:
            self.start_hour = date['start_hour']
        if self.end_hour is None:
            self.end_hour = date['end_hour']

    def extract_regrid_res(self):
        if 'deg' in self.url.stem:
            filename_split = self.url.stem.split('_')
            if self.naming_convention == OLD_GLM_NOTATION: # GLM_array_xxdeg_DDD_HH1-HH2.nc
                self.regrid_res = filename_split[-3]
            else: # xxdeg_OR_GLM-L2-LCFA_Gxx_YYYY_DDD_HH1-HH2.nc
                self.regrid_res = filename_split[0]
        else:
            self.regrid_res = None

    def extract_satellite(self):
        filename = self.url.stem
        filename_split = filename.split('_')
        if self.naming_convention == OLD_GLM_NOTATION or self.naming_convention == OLD_GLM_PRE_REGRID_TEMP_FILENAME:
            # old_glm_filename --> usually only for 05-2018 or 06-2018 files so 'G16' satellite
            self.satellite_version = 'G16'
        elif self.directory:
            self.satellite_version = None
        else:
            self.satellite_version = filename_split[-4]

    def get_start_date_pdTimestamp(self, ignore_missing_start_hour=False):
        """
        Returns pd.Timestamp object of the start date of the GLM file / directory
        @param ignore_missing_start_hour: <bool> if we need timestamp for directory
        @return: <pandas.Timestamp> object
        """
        if self.directory:
            # if ignore missing start hour --> create timestamp with hour == 00:00 (only OK if directory)
            if ignore_missing_start_hour and not any(val is None for val in [self.year, self.day_of_year]):
                dtime = datetime.datetime.strptime(f'{self.year}_{self.day_of_year}_00', '%Y_%j_%H')
            else:
                return None
        else:
            # if missing value (year, hour, start_hour)--> can't create timestamp
            if any(val is None for val in [self.year, self.day_of_year, self.start_hour]):
                raise ValueError(f'Cannot get datetime object, one of more start date value missing (year={self.year}, '
                                 f'day_of_year={self.day_of_year}, start_hour={self.start_hour})')
            else:
                dtime = datetime.datetime.strptime(f'{self.year}_{self.day_of_year}_{self.start_hour}', '%Y_%j_%H')
        return pd.Timestamp(dtime)

    def print(self):
        for attr_key, attr_val in vars(self).items():
            print(f'{attr_key}: {attr_val}')
