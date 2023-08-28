from pathlib import Path

from .constants import GLM_DIR_NAME, GLM_HOURLY_FILE_NAME, HH_pattern, DDD_pattern


def get_glm_hourly_file_date_from_filename(glm_filename, year=None):
    """

    :param glm_filename:
    :param year:
    :return:
    """
    # expecting str with format GLM_array_DDD_HH1-HH2.nc with DDD: day number, HH1: start hour, HH2: end hour
    # --> faire un pattern check (utiliser une fonction qui sera mise dans utils pour faire ça ?)
    #     ---> comme ça si on change le format on peut changer le check easy peasy (mais y aura quand même des pbms en fait)
    if isinstance(glm_filename, Path) and len(glm_filename.parts) > 1:
        glm_filename = glm_filename.parts[-1]
    glm_filename = str(glm_filename).split(".")[0]  # remove .nc part of the path
    glm_filename_split = glm_filename.split("_")
    glm_hour_split = glm_filename_split[-1].split('-')
    date_dic = {
        "day_of_year": glm_filename_split[-2],
        "start_hour": glm_hour_split[0],
        "end_hour": glm_hour_split[1]
    }
    if year is not None:
        date_dic["year"] = year
    return date_dic


def get_glm_daily_dir_date(glm_dirname):
    """

    :param glm_dirname:
    :return:
    """
    # expecting str with format OR_GLM-L2-LCFA_G16_sYYYYDDD with YYYY: year, DDD: day number
    # --> faire un pattern check (utiliser une fonction qui sera mise dans utils pour faire ça ?)
    #     ---> comme ça si on change le format on peut changer le check easy peasy (mais y aura quand même des pbms en fait)
    if isinstance(glm_dirname, Path):
        glm_dirname = str(glm_dirname)
    glm_dirname = glm_dirname.split("_")[-1]  # get last part of the dir name (sYYYYDDD)
    glm_dirname_split = glm_dirname[1:]  # remove the s
    return {
        "year": glm_dirname_split[:4],
        "day_of_year": glm_dirname_split[-3:]
    }


def get_day_of_year_from_glm_daily_regrid_filename(daily_filename):
    """

    :param daily_filename:
    :return:
    """
    # expecting filename GLM_array_05deg_DDD.nc
    if isinstance(daily_filename, Path) and len(daily_filename.parts) > 1:
        daily_filename = daily_filename.parts[-1]
    daily_filename = str(daily_filename).split('.')[0] # remove .nc
    return daily_filename.split("_")[-1]
"""could return get_glm_hourly_file_date_from_filename['day_of_year']"""


def get_start_hour_from_glm_hourly_regrid_filename(hourly_filename):
    """

    :param hourly_filename:
    :return:
    """
    # expecting filename GLM_array_05deg_DDD.nc
    if isinstance(hourly_filename, Path) and len(hourly_filename.parts) > 1:
        hourly_filename = hourly_filename.parts[-1]
    hourly_filename = hourly_filename.split('.')[0] # remove .nc
    hourly_filename_split = hourly_filename.split('_')[-1]
    return hourly_filename_split.split("-")[-2]
"""could return get_glm_hourly_file_date_from_filename['start_hour']"""



def generate_glm_hourly_nc_file_pattern(day_of_year=DDD_pattern, start_hour=HH_pattern, end_hour=HH_pattern,
                                        regrid_str="", glm_hourly_filename=GLM_HOURLY_FILE_NAME):
    """

    :param day_of_year:
    :param start_hour:
    :param end_hour:
    :param regrid_str:
    :param glm_hourly_filename:
    :return:
    """
    return glm_hourly_filename + regrid_str + str(day_of_year) + "_" + str(start_hour) + "-" + str(end_hour) + ".nc"


def generate_glm_regrid_hourly_nc_file_path(glm_dir_path_root, day_of_year, start_hour, end_hour, regrid_str="05deg_"):
    """

    :param glm_dir_path_root:
    :param day_of_year:
    :param start_hour:
    :param end_hour:
    :param regrid_str:
    :return:
    """
    hourly_filename = generate_glm_hourly_nc_file_pattern(day_of_year=day_of_year, start_hour=start_hour,
                                                          end_hour=end_hour, regrid_str=regrid_str)
    return Path(f'{glm_dir_path_root}{day_of_year}/{hourly_filename}')


