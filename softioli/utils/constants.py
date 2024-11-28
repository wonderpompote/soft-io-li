import pathlib

#----- output -----
OUTPUT_ROOT_DIR = pathlib.Path('/o3p/patj/SOFT-IO-LI_output')
TIMESTAMP_FORMAT = "%Y-%m-%d_%H%M"
ARRIVALTIME_FORMAT_CSV_FILENAME = '%Y%m%d-%H%M'

#----- part 1 -----
IAGOSv3_CAT_PATH = pathlib.Path('/o3p/iagos/catalogues_v3/iagos_L2.parquet')
IAGOSv3_PV_PATH = pathlib.Path('/o3p/iagos/iagosv3/L4/')
IAGOSv3_PV_FILE_PREFIX = 'IAGOS_ECMWF'
IAGOSv3_PV_PATH = '/o3p/iagos/iagosv3/L4/'

# as found from IAGOS v3 catalogue in August 2024 --> <!> might need to be updated if new flights to new airports are added to the database
SOFTIOLI_AIRPORTS = ['SCL, Santiago, Chile',
 'MIA, Miami, United States',
 'PTY, Panama City, Panama',
 'BOG, Bogota, Colombia',
 'JED, Jeddah, Saudi Arabia',
 'DFW, Dallas, United States',
 'JFK, New York, United States',
 'YVR, Vancouver, Canada',
 'GRU, Sao Paulo, Brazil',
 'CCS, Caracas, Venezuela',
 'SAN, San Diego, United States',
 'MUC, Munich, Germany',
 'DUS, Dusseldorf, Germany',
 'ATL, Atlanta, United States',
 'CUN, Cancun, Mexico',
 'FRA, Frankfurt, Germany',
 'EWR, Newark, United States',
 'SJO, San Jose, Costa Rica',
 'ORD, Chicago, United States',
 'LIS, Lisbon, Portugal',
 'SFO, San Francisco, United States',
 'CGN, Cologne, Germany',
 'TXL, Berlin, Germany',
 'DEN, Denver, United States',
 'LAX, Los Angeles, United States',
 'YYZ, Toronto, Canada',
 'IAH, Houston, United States',
 'DTW, Detroit, United States',
 'PHL, Philadelphia, United States']

Q3_DS_PATH = '/home/patj/SOFT-IO-LI/q3_ds/CO_NOx_q3_ds_NONEreg_2024-07-02_1839.nc' #TODO: suppr if we don't use it
CO_O3_BACKGROUND_DS_PATH = '/home/patj/SOFT-IO-LI/q3_ds/CO_O3_bckg_q3_by_region_month_year_2024-09-20_1730.nc'

PROGRAM_ATTR = 'program'
DEPARTURE_UTC_TIME_ATTR = 'departure_UTC_time'
IAGOS = 'IAGOS'
CARIBIC = 'CARIBIC'
CORE = 'CORE'
MOZAIC ='MOZAIC'

PV_VARNAME = 'PV'
AIRPRESS_VARNAME = 'air_press_AC'
CO_VARNAME = 'CO_P1'
O3_VARNAME = 'O3_P1'
RHL_VARNAME = 'RHL_P1'
NOx_PLUME_ID_VARNAME = 'NOx_plume_id'

CORE_NO_VARNAME = 'NO_P2b'
CORE_NO2_VARNAME = 'NO2_P2b'
CORE_NOx_VARNAME = 'NOx_P2b'

CARIBIC_CO_VARNAME = 'CO_PC2' 
CARIBIC_O3_VARNAME = 'O3_PC2'
CARIBIC_NO_VARNAME = 'NO_PC2'
CARIBIC_NO2_VARNAME = 'NO2_PC2'
CARIBIC_NOx_VARNAME = 'NOx_PC'

NOx_SMOOTHED_VARNAME = 'NOx_smoothed'
NOx_SMOOTHED_TROPO_VARNAME = 'NOx_smoothed_tropo'
NOx_FILTERED_VARNAME = 'NOx_filtered'

CO_SMOOTHED_VARNAME = 'CO_smoothed'
CO_SMOOTHED_TROPO_VARNAME = 'CO_smoothed_tropo'
O3_TROPO_VARNAME = 'O3_tropo'

AIRCRAFT_SPIKE_VARNAME = 'aircraft_spike'

# window used to smooth NOx and CO values, window size = min plume length (100 seconds)
WINDOW_SIZE = {
    f'{IAGOS}-{CORE}': 25, # 25 * 4sec intervals
    f'{IAGOS}-{CARIBIC}': 10 # 10 * 10sec intervals
}
MIN_PLUME_LENGTH = 100 # in seconds, ~= 27.5 km

NOx_MEDIAN = 0.161 # calculated from all L2 IAGOS NOx cruise values in the troposphere to date (03 July 2024)
NOx_Q3 = 0.283 # calculated from all L2 IAGOS NOx cruise values in the troposphere to date (03 July 2024)
CO_Q3 = 110 # calculated from all L2 IAGOS CO cruise values in the troposphere to date (03 July 2024)

FLIGHT_PROGRAM_KEYERROR_MSG = f'flight program NOT supported yet, supported values so far: "{IAGOS}-{CORE}" or "{CORE}" or "{IAGOS}-{CARIBIC}" or "{CARIBIC}"'


#----- part 2 -----
FP_LOUTSTEP = '1h' # flexpart timestep
FP_DURATION = 10 #days
FP_OUTHEIGHT_MIN = 500 #m
FP_OUTHEIGHT_STEP = 500
FP_OUTHEIGHT_MAX = 18000

MIN_NPARTS = 50000

#----- part 3 -----
DEFAULT_LOGDIR = pathlib.Path('/home/patj/logs/softioli/')

GOES_SATELLITE_GLM = 'GOES_GLM'
GLM_ROOT_DIR = pathlib.Path('/o3p/patj/glm')
REGRID_GLM_DIRNAME = 'regrid_hourly_glm'
PRE_REGRID_GLM_DIRNAME = 'pre_regrid_glm'
REGRID_GLM_ROOT_DIR = pathlib.Path(f'{GLM_ROOT_DIR}/{REGRID_GLM_DIRNAME}')
PRE_REGRID_GLM_ROOT_DIR = pathlib.Path(f'{GLM_ROOT_DIR}/{PRE_REGRID_GLM_DIRNAME}')
GLM_PATH_PREFIX = 'OR_GLM-L2-LCFA'
GLM_Gxx_PATTERN = 'G1[6-8]' # TODO: update if older/newer versions available

GOES_SATELLITE_ABI = 'GOES_ABI'
ABI_ROOT_DIR = pathlib.Path('/o3p/patj/ABI')
REGRID_ABI_DIRNAME = 'regrid_hourly_ABI'
PRE_REGRID_ABI_DIRNAME = 'pre_regrid_ABI'
ABI_PATH_PREFIX = 'ABI_GEO_L1B'
ABI_GOESXX_PATTERN = 'GOES1[236-8]'
ABI_GOES_WEST_SAT_VERSION = ['GOES17', 'GOES18']
ABI_GOES_EAST_SAT_VERSION = ['GOES16']

ABI_COORDS_DIRPATH = '/o3p/patj/ABI/coords_files'
GOES_0750_C0_COORDS_FILE_HDF = 'GOES-0750.C0.4km.hdf'
GOES_0750_C1_COORDS_FILE_HDF = 'GOES-0750.C1.4km.hdf'
GOESNG_0750_COORDS_FILE_HDF = 'GOESNG-0750.2km.hdf'
GOESNG_1370_COORDS_FILE_HDF = 'GOESNG-1370.2km.hdf'
GOES_0750_C0_COORDS_FILE = 'GOES-0750.C0.4km.nc'
GOES_0750_C1_COORDS_FILE = 'GOES-0750.C1.4km.nc'
GOESNG_0750_COORDS_FILE = 'GOESNG-0750.2km.nc'
GOESNG_1370_COORDS_FILE = 'GOESNG-1370.2km.nc'

GRID_RESOLUTION_STR = '05deg'
GRID_RESOLUTION = 0.5
FPOUT_LAT_MIN = -89.75
FPOUT_LAT_MAX = 89.75
FPOUT_LON_MIN = -179.25
FPOUT_LON_MAX = 180.25

YYYY_pattern = "[0-2][0-9][0-9][0-9]" # year
DDD_pattern = "[0-3][0-9][0-9]" # day of year
DD_pattern = "[0-3][0-9]" # day
MM_pattern = "[0-1][0-9]" # month
HH_pattern = "[0-2][0-9]" # hour
mm_pattern = "[0-5][0-9]" # seconds

# TODO: update when other satellites OK
SAT_VALUE_ERROR = f' not supported yet. Supported satellites so far: "{GOES_SATELLITE_GLM}", "{GOES_SATELLITE_ABI}"'


# sat settings dict keys
flash_energy_varname = "flash_energy_varname"
flash_area_varname = "flash_area_varname"
raw_lat_cname = "pre_regrid_lat_coordname"
raw_lon_cname = "pre_regrid_lon_coordname"
attrs_to_keep = "attrs_to_keep"

# hist parameters
f_en_min_bin = -15 # log
f_en_max_bin = -10
f_en_hist_step = 0.1
f_ar_min_bin = 1.5 # log
f_ar_max_bin = 4.5
f_ar_hist_step = 0.1

# TODO: complete with other satellite data + add dataset_name (mais là pas OK parce que nom fichier 20sec, PAS hourly)
SAT_SETTINGS = {
    GOES_SATELLITE_GLM: {
        flash_energy_varname: "flash_energy",
        flash_area_varname: "flash_area",
        raw_lat_cname: "flash_lat", # latitude coordinate name in pre regrid dataset
        raw_lon_cname: "flash_lon", # longitude coordinate name in pre regrid dataset
        attrs_to_keep: ['production_site', 'orbital_slot', 'platform_ID', 'instrument_type', 'instrument_ID',
                        'spatial_resolution', 'processing_level']
    },
    # <OTHER_SATELLITE>: { ... }
}




