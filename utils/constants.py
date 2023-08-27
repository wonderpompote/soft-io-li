GLM_DIR_NAME = "OR_GLM-L2-LCFA_G16_s" # OR_GLM-L2-LCFA_G16_sYYYYDDD
GLM_HOURLY_FILE_NAME = "GLM_array_" # GLM_array_DDD_HH1-HH2.nc
GLM_REGRID_DIR_PATH = '/o3p/patj/test-test-glm/GLM_array_05deg/'
CONCAT_GLM_REGRID_DIR_NAME = "concat_GLM_array_05deg"
GLM_REGRID_DIR_NAME = "GLM_array_05deg_"
OG_GLM_FILES_PATH = '/o3p/macc/glm'
DEFAULT_GLM_DATA_VARS_TO_REGRID = {
    "flash_energy": {
        "operation": "sum",
        "operation_dims": None
    },
    "flash_count": {
        "operation": "count",
        "operation_dims": None
    }
}

HH_pattern = "[0-2][0-9]"
DDD_pattern = "[0-3][0-6][0-9]"
YYYY_pattern = "[0-2][0-9][0-9][0-9]"
YYYYDDD_PATTERN = YYYY_pattern+DDD_pattern
