import pathlib
import socket

# constants part 3 new version
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