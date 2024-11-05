import SatPathParser

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

    def __int__(self, file_url, regrid, hourly, year=None, month=None, day=None, start_hour=None, end_hour=None,
                version=None, regrid_res_str=None):
        self.url = file_url
        self.hourly = hourly
        self.regrid = regrid
        self.regrid_res = regrid_res_str
        self.year = int(year) if year is not None else year
        self.month = int(month) if month is not None else month
        self.day = int(day) if day is not None else day
        self.start_hour = int(start_hour) if start_hour is not None else start_hour
        self.end_hour = int(end_hour) if end_hour is not None else end_hour
        self.version = version
        # if missing at elast 1 date info --> extract it from filename
        if any(val is None for val in [self.year, self.day_of_year, self.start_hour]):
            self.extract_start_date()


    def extract_start_date(self):
        filename = self.url.stem
        filename_split = filename.split('_')
        if not self.hourly: #GEO_L1B-GOES1x_YYYY-MM-DDTHH-mm-ss_[NSG]_IR10x_V1-0[4-6].hdf
            full_date_str = filename_split[2]
            date = {
                "year":
            }
