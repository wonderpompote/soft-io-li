import pathlib
import socket

# constants part 3 new version
DEFAULT_LOGDIR = pathlib.Path('/home/patj/logs/softioli/')

SAT_VALUE_ERROR = f'satellite not supported yet. Only GOES satellite supported for now' # TODO: update when other satellites OK

REGRID_GLM_ROOT_DIR = pathlib.Path('/o3p/patj/glm/regrid_hourly_glm/')
PRE_REGRID_GLM_ROOT_DIR = pathlib.Path('/o3p/patj/glm/pre_regrid_hourly_glm/')

GLM_PATH_PREFIX = 'OR_GLM-L2-LCFA'
GOES_SATELLITE = 'GOES'
Gxx_PATTERN = 'G1[6-7]' # TODO: update if newer versions available

DEFAULT_GLM_DATA_VARS_TO_REGRID = {
    "flash_energy": {
        "operation": ['histogram', 'count'],
        "histogram": {
            "min_bin_edge": -15,
            "max_bin_edge": -10,
            "step": 0.1,
            "res_var_name": "flash_energy_log_hist"
        },
        "count": {
            "res_var_name": "flash_count"
        }
    },
    "flash_area": {
        "operation": ['histogram'],
        "histogram": {
            "min_bin_edge": 1.5,
            "max_bin_edge": 4.5,
            "step": 0.1,
            "res_var_name": "flash_area_log_hist"
        }
    }
}

FPOUT_LAT_MIN = -89.75
FPOUT_LAT_MAX = 89.75
FPOUT_LON_MIN = -179.25
FPOUT_LON_MAX = 180.25

GRID_RESOLUTION_STR = '05deg'
GRID_RESOLUTION = 0.5

YYYY_pattern = "[0-2][0-9][0-9][0-9]" # year
DDD_pattern = "[0-3][0-6][0-9]" # day of year
HH_pattern = "[0-2][0-9]" # hour

############ old version
# "original" GLM files
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
DUMMY_VALUE = -999