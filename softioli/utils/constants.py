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

#TODO: je vais pas m'en servir en fait T.T
Q3_DS_PATH = '/home/patj/SOFT-IO-LI/q3_ds/CO_NOx_q3_ds_NONEreg_2024-07-02_1839.nc'

PROGRAM_ATTR = 'program'
IAGOS = 'IAGOS'
CARIBIC = 'CARIBIC'
CORE = 'CORE'
MOZAIC ='MOZAIC'

PV_VARNAME = 'PV'
AIRPRESS_VARNAME = 'air_press_AC'
CO_VARNAME = 'CO_P1'
O3_VARNAME = 'O3_P1'
NOx_PLUME_ID_VARNAME = 'NOx_plume_id'

CORE_NO_VARNAME = 'NO_P2b'
CORE_NO2_VARNAME = 'NO2_P2b'
CORE_NOx_VARNAME = 'NOx_P2b'

CARIBIC_CO_VARNAME = 'CO_PC2' #TODO: store caribic suffix instead ?
CARIBIC_O3_VARNAME = 'O3_PC2'
CARIBIC_NO_VARNAME = 'NO_PC2'
CARIBIC_NO2_VARNAME = 'NO2_PC2'
CARIBIC_NOx_VARNAME = 'NOx_PC'
NOx_PLUME_ID_VARNAME = 'NOx_plume_id'

WINDOW_SIZE = {
    f'{IAGOS}-{CORE}': 25, # 25 * 4sec intervals
    f'{IAGOS}-{CARIBIC}': 10 # 10 * 10sec intervals
}

FLIGHT_PROGRAM_KEYERROR_MSG = f'flight program NOT supported yet, supported values so far: "{IAGOS}-{CORE}" or "{CORE}" or "{IAGOS}-{CARIBIC}" or "{CARIBIC}"'

NOx_MEDIAN = 0.161 # calculated from all L2 IAGOS NOx cruise values in the troposphere to date (03 July 2024)
NOx_Q3 = 0.283 # calculated from all L2 IAGOS NOx cruise values in the troposphere to date (03 July 2024)

#----- part 3 -----
DEFAULT_LOGDIR = pathlib.Path('/home/patj/logs/softioli/')

# TODO: update when other satellites OK
SAT_VALUE_ERROR = f'satellite not supported yet. Only "GOES_GLM" satellite for now'

GLM_ROOT_DIR = pathlib.Path('/o3p/patj/glm')
REGRID_GLM_DIRNAME = 'regrid_hourly_glm'
PRE_REGRID_GLM_DIRNAME = 'pre_regrid_hourly_glm'

REGRID_GLM_ROOT_DIR = pathlib.Path(f'{GLM_ROOT_DIR}/{REGRID_GLM_DIRNAME}')
PRE_REGRID_GLM_ROOT_DIR = pathlib.Path(f'{GLM_ROOT_DIR}/{PRE_REGRID_GLM_DIRNAME}')

GOES_SATELLITE_GLM = 'GOES_GLM'
GLM_PATH_PREFIX = 'OR_GLM-L2-LCFA'
Gxx_PATTERN = 'G1[6-7]' # TODO: update if newer versions available

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
    }
    # <OTHER_SATELLITE>: { ... }
}

FPOUT_LAT_MIN = -89.75
FPOUT_LAT_MAX = 89.75
FPOUT_LON_MIN = -179.25
FPOUT_LON_MAX = 180.25

GRID_RESOLUTION_STR = '05deg'
GRID_RESOLUTION = 0.5

YYYY_pattern = "[0-2][0-9][0-9][0-9]" # year
DDD_pattern = "[0-3][0-9][0-9]" # day of year
HH_pattern = "[0-2][0-9]" # hour

############ old version
"""# "original" GLM files
OG_GLM_FILES_PATH = '/o3p/macc/glm'
GLM_DIR_NAME = "OR_GLM-L2-LCFA_G16_s" # OR_GLM-L2-LCFA_G16_sYYYYDDD
GLM_HOURLY_FILE_NAME = "GLM_array_" # GLM_array_DDD_HH1-HH2.nc
# regrid
GLM_REGRID_DIR_PATH = '/o3p/patj/glm/GLM_array_05deg/'
CONCAT_GLM_REGRID_DIR_NAME = "daily_GLM_array_05deg"
GLM_REGRID_DIR_NAME = "GLM_array_05deg_"

# glob patterns
#HH_pattern = "[0-2][0-9]" # hour
#DDD_pattern = "[0-3][0-6][0-9]" # day of year
MM_pattern = "[0-1][1-9]" # month
#YYYY_pattern = "[0-2][0-9][0-9][0-9]" # year
YYYYDDD_PATTERN = YYYY_pattern+DDD_pattern

REGRID_STR = "05deg_"

# histograms
HIST_STEP = 0.1
FLASH_ENERGY_BINS = 50
FLASH_ENERGY_LOG10_RANGE = [-15, -10] # <!> log10
FLASH_AREA_BINS = 30
FLASH_AREA_LOG10_RANGE = [1.5, 4.5] # <!> log10

# miscellaneous
DUMMY_VALUE = -999"""