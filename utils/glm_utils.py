from pathlib import Path

from constants import GLM_DIR_NAME, GLM_HOURLY_FILE_NAME

def get_split_glm_format_date(date_to_split):
    """
    Funtion returning a dictionary containing year, day_number and hour from a datetime object
    Used for glm file manipulations (with day of year)
    :param date_to_split: <datetime.datetime> date to split
    :return: <dict> { "year": <str>, "day_of_year": <str>, "hour": <str> }
    """
    # convert datetime object to str with format YYYY-DDD-HH with YYYY: year, DDD: day of year, HH: hour
    date_str = date_to_split.strftime('%Y-%j-%H')
    date_split = date_str.split('-')
    return {
        "year": date_split[0],
        "day_number": date_split[1],
        "hour": date_split[2]
    }


def get_min_max_glm_daily_dir_path_list(glm_dir_path, min_date_dic, max_date_dic):
    """
    Function to get
    :param glm_dir_path:
    :param min_date_dic:
    :param max_date_dic:
    :return:
    """
    all_dir_list = sorted(glm_dir_path.glob(GLM_DIR_NAME+min_date_dic["year"]+"[0-3][0-6][0-9]"))
    dir_list = []
    # <!> glm_dir_path doit être un Path
    min_date_dir_path = glm_dir_path / Path(GLM_DIR_NAME+min_date_dic["year"]+min_date_dic["day_number"])
    max_date_dir_path = glm_dir_path / Path(GLM_DIR_NAME+max_date_dic["year"]+max_date_dic["day_number"])
    for dir_name in all_dir_list:
        if min_date_dir_path <= dir_name <= max_date_dir_path:
            dir_list.append(dir_name)
    return sorted(dir_list)

""" !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
                            COMMENTS TO BE ADDED
 !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!! """
""" on veut return un dic {"day_number": DDD, "start_hour": HH1, "end_hour": HH2} """
def get_glm_hourly_file_date(glm_filename):
    # expecting str with format GLM_array_DDD_HH1-HH2.nc with DDD: day number, HH1: start hour, HH2: end hour
    # --> faire un pattern check (utiliser une fonction qui sera mise dans utils pour faire ça ?)
    #     ---> comme ça si on change le format on peut changer le check easy peasy (mais y aura quand même des pbms en fait)
    glm_filename = glm_filename.split(".")[0] # remove .nc part of the path
    glm_filename_split = glm_filename.split("_")
    glm_hour_split = glm_filename_split[-1].split('-')
    return {
        "day_number": glm_filename_split[-2],
        "start_hour": glm_hour_split[0],
        "end_hour": glm_hour_split[1]
    }


def get_glm_daily_dir_date(glm_dirname):
    # expecting str with format OR_GLM-L2-LCFA_G16_sYYYYDDD with YYYY: year, DDD: day number
    # --> faire un pattern check (utiliser une fonction qui sera mise dans utils pour faire ça ?)
    #     ---> comme ça si on change le format on peut changer le check easy peasy (mais y aura quand même des pbms en fait)
    glm_dirname = glm_dirname.split("_")[-1] # get last part of the dir name (sYYYYDDD)
    glm_dirname_split = glm_dirname[1:] # remove the s
    return {
        "year": glm_dirname_split[:4],
        "day_number": glm_dirname_split[-3:]
    }

def get_glm_hourly_nc_file_pattern(day_number, start_hour="[0-2][0-9]", end_hour="[0-2][0-9]"):
    return GLM_HOURLY_FILE_NAME+str(day_number)+"_"+str(start_hour)+"-"+str(end_hour)+".nc"

