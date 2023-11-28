# "original" GLM files
OG_GLM_FILES_PATH = '/o3p/macc/glm'
GLM_DIR_NAME = "OR_GLM-L2-LCFA_G16_s" # OR_GLM-L2-LCFA_G16_sYYYYDDD
GLM_HOURLY_FILE_NAME = "GLM_array_" # GLM_array_DDD_HH1-HH2.nc
# regrid
GLM_REGRID_DIR_PATH = '/o3p/patj/glm/GLM_array_05deg/'
CONCAT_GLM_REGRID_DIR_NAME = "daily_GLM_array_05deg"
GLM_REGRID_DIR_NAME = "GLM_array_05deg_"
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
# glob patterns
HH_pattern = "[0-2][0-9]" # hour
DDD_pattern = "[0-3][0-6][0-9]" # day of year
MM_pattern = "[0-1][1-9]" # month
YYYY_pattern = "[0-2][0-9][0-9][0-9]" # year
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