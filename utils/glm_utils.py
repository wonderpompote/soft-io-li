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
    if isinstance(glm_filename, Path):
        glm_filename = str(glm_filename)
    glm_filename = glm_filename.split(".")[0]  # remove .nc part of the path
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


def get_glm_hourly_file_date(glm_filename):
    """

    :param glm_filename:
    :return:
    """
    # expecting str with format GLM_array_DDD_HH1-HH2.nc with DDD: day number, HH1: start hour, HH2: end hour
    # --> faire un pattern check (utiliser une fonction qui sera mise dans utils pour faire ça ?)
    #     ---> comme ça si on change le format on peut changer le check easy peasy (mais y aura quand même des pbms en fait)
    glm_filename = glm_filename.split(".")[0]  # remove .nc part of the path
    glm_filename_split = glm_filename.split("_")
    glm_hour_split = glm_filename_split[-1].split('-')
    return {
        "day_number": glm_filename_split[-2],
        "start_hour": glm_hour_split[0],
        "end_hour": glm_hour_split[1]
    }


def get_glm_hourly_nc_file_pattern(day_number, start_hour=HH_pattern, end_hour=HH_pattern):
    """

    :param day_number:
    :param start_hour:
    :param end_hour:
    :return:
    """
    return GLM_HOURLY_FILE_NAME + str(day_number) + "_" + str(start_hour) + "-" + str(end_hour) + ".nc"


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


def generate_glm_05deg_hourly_nc_file_path(glm_dir_path_root, day_of_year, start_hour, end_hour):
    """

    :param glm_dir_path_root:
    :param day_of_year:
    :param start_hour:
    :param end_hour:
    :return:
    """
    return Path(f'{glm_dir_path_root}{day_of_year}/GLM_array_05deg_{day_of_year}_{start_hour}-{end_hour}.nc')


