import pathlib
from pandas import Timedelta
from re import fullmatch

from .utils_functions import date_to_pd_timestamp, str_to_path
from . import constants as cts
from . import GLMPathParser, OLD_GLM_PRE_REGRID_TEMP_NOTATION, OLD_GLM_NOTATION


def generate_sat_hourly_filename_pattern(sat_name, regrid, regrid_res_str=cts.GRID_RESOLUTION_STR, naming_convention=None):
    """
    Generate filename pattern for a specific satellite, naming convention and regrid resolution (to be used with pathlib glob function)
    :param sat_name: <str> name of the satellite (only 'GOES_GLM' supported for now)
    :param regrid: <bool>
    :param regrid_res_str: <str> grid resolution str (to be added to the resulting filename)
    :param naming_convention: <str> file naming convention (mostly for backward compatibility). Supported values: 'OLD_TEMP', 'OLD', None (default)
    :return: <str> filename pattern for the satellite
    """
    if sat_name == cts.GOES_SATELLITE_GLM:
        if naming_convention is None:
            # OR_GLM-L2-LCFA_Gxx_YYYY_DDD_HH-HH.nc
            filename_pattern = f'{cts.GLM_PATH_PREFIX}_{cts.Gxx_PATTERN}_{cts.YYYY_pattern}_{cts.DDD_pattern}_{cts.HH_pattern}-{cts.HH_pattern}.nc'
        elif naming_convention == OLD_GLM_PRE_REGRID_TEMP_NOTATION:
            # GLM_array_DDD_temp_HH.nc
            filename_pattern = f'GLM_array_{cts.DDD_pattern}_temp_{cts.HH_pattern}.nc'
        elif naming_convention == OLD_GLM_NOTATION:
            # GLM_array(_xxdeg)_DDD_HH1-HH2.nc
            if regrid:
                regrid_pattern = f'{regrid_res_str}_'
            else:
                regrid_pattern = ''
            filename_pattern = f'GLM_array_{regrid_pattern}{cts.DDD_pattern}_{cts.HH_pattern}-{cts.HH_pattern}.nc'
        else:
            raise ValueError(f'Usupported naming convention for {sat_name} satellite. Supported values: "{OLD_GLM_PRE_REGRID_TEMP_NOTATION}", "{OLD_GLM_NOTATION}" or None')
    else:
        raise ValueError(f'{sat_name} NOT supported yet. Supported satellite so far: "GOES_GLM"')

    if regrid and naming_convention is None:
        return f'{regrid_res_str}_{filename_pattern}'
    else:
        return filename_pattern


def generate_sat_dirname_pattern(sat_name, regrid, regrid_res_str=cts.GRID_RESOLUTION_STR, naming_convention=None):
    """
    Generate directory name pattern for a specific satellite and regrid resolution (to be used with pathlib glob function)
    :param sat_name: <str>
    :param regrid: <bool>
    :param regrid_res_str: <str>
    :param naming_convention: <str> file naming convention (mostly for backward compatibility). Supported values: 'OLD', 'OLD_TEMP', None (default)
    :return: <str> directory name pattern for the satellite
    """
    if sat_name == cts.GOES_SATELLITE_GLM:
        if naming_convention is None:
            # OR_GLM-L2-LCFA_YYYY_DDD
            dirname_pattern = f'{cts.GLM_PATH_PREFIX}_{cts.YYYY_pattern}_{cts.DDD_pattern}'
        elif naming_convention == OLD_GLM_PRE_REGRID_TEMP_NOTATION:
            #OR_GLM-L2-LCFA_Gxx_sYYYYDDD
            dirname_pattern = f'{cts.GLM_PATH_PREFIX}_{cts.Gxx_PATTERN}_s{cts.YYYY_pattern}{cts.DDD_pattern}'
        elif naming_convention == OLD_GLM_NOTATION:
            # GLM_array(_05deg)_DDD
            if regrid:
                regrid_pattern = f'{regrid_res_str}_'
            else:
                regrid_pattern = ''
            dirname_pattern = f'GLM_array_{regrid_pattern}{cts.DDD_pattern}'
        else:
            raise ValueError(f'Usupported naming convention for {sat_name} satellite. Supported values: "{OLD_GLM_PRE_REGRID_TEMP_NOTATION}", "{OLD_GLM_NOTATION}" or None')
    else:
        raise ValueError(f'{sat_name} NOT supported yet. Supported satellite so far: "GOES_GLM"')

    if regrid and naming_convention is None:
        return f'{regrid_res_str}_{dirname_pattern}'
    else:
        return dirname_pattern


def generate_sat_dir_path(date, sat_name, regrid, regrid_res_str=cts.GRID_RESOLUTION_STR, target_dir=None):
    """
    Generate the absolute path to the directory containing the satellite data for a specific date (regridded or not)
    <!> The path does not necessarily point to an existing directory, if it does not exist it will need to be created and filled with the correct data files
    :param date: <pandas.Timestamp> or <numpy.datetime64> or <datetime.datetime> or <GLMPathParser>
    :param sat_name: <str> satellite name
    :param regrid: <bool> indicates if the directory contains regridded files
    :param regrid_res_str: <str> regrid resolution (if regrid == True)
    :param target_dir: <str> or <pathlib.Path> root directory path (if different from default (/o3p/patj/glm), mostly used for testing)
    :return: <pathlib.Path> object pointing to satellite data directory for a specific date
    """
    # check date
    date = date_to_pd_timestamp(date)
    # now that we have the pandas.Timestamp we can generate the path
    if sat_name == cts.GOES_SATELLITE_GLM:
        root_dir_path = target_dir if target_dir is not None else cts.GLM_ROOT_DIR
        if regrid:
            return pathlib.Path(
                f'{root_dir_path}/{cts.REGRID_GLM_DIRNAME}/{date.year}/{regrid_res_str}_{cts.GLM_PATH_PREFIX}_{date.year}_{date.dayofyear:03d}')
        else:
            return pathlib.Path(
                f'{root_dir_path}/{cts.PRE_REGRID_GLM_DIRNAME}/{date.year}/{cts.GLM_PATH_PREFIX}_{date.year}_{date.dayofyear:03d}')
    else:
        raise ValueError(f'{sat_name} {cts.SAT_VALUE_ERROR}')


def generate_sat_hourly_file_path(date, satellite, sat_version, regrid, regrid_res_str=cts.GRID_RESOLUTION_STR,
                                  dir_path=None):
    """
    Generate absolute path to a satellite hourly file (regridded or not)
    <!> The path does not necessarily point to an existing file, it might point to a file that has yet to be created
    :param date: <pandas.Timestamp> or <numpy.datetime64> or <datetime.datetime> or <GLMPathParser>
    :param satellite: <str> satellite name
    :param sat_version: <str> satellite version e.g.: 'G16' for GOES satellite
    :param regrid: <bool> indicates if the file is regridded
    :param regrid_res_str: <str> regrid resolution (if regrid == True)
    :param dir_path: <str> or <pathlib.Path> mostly used for testing purposes, if == None the default directory path is used
    :return: <pathlib.Path> object pointing to satellite hourly data file
    """
    date = date_to_pd_timestamp(date)
    dir_path = generate_sat_dir_path(date=date, sat_name=satellite, regrid=regrid, regrid_res_str=regrid_res_str,
                                     target_dir=dir_path)
    if not dir_path.exists():
        dir_path.mkdir(parents=True)
    if satellite == cts.GOES_SATELLITE_GLM:
        filename = f'{cts.GLM_PATH_PREFIX}_{sat_version}_{date.year}_{date.dayofyear:03d}_{date.hour:02d}-{(date + Timedelta(hours=1)).hour:02d}.nc'
        if regrid:
            return dir_path / pathlib.Path(f'{regrid_res_str}_{filename}')
        else:
            return dir_path / pathlib.Path(filename)
    else:
        raise ValueError(f'{satellite} {cts.SAT_VALUE_ERROR}')


def get_list_of_dates_from_list_of_sat_path(path_list, directory, satellite, regrid, date_str, date_format='%Y-%j'):
    """
    Takes a list of satellite data paths (directories or files) and returns the corresponding dates
    extracted from the file/directory names
    @param path_list: <list> [ <pathlib.Path> or <str>, ... ]
    @param directory: <bool> indicates if the paths point to directories
    @param satellite: <str> satellite name
    @param regrid: <bool> indicates if the paths point to regridded files or directories
    @param date_str: <bool> indicates if we want the dates as str
    @return: <list> [ <pd.Timestamp> or <str>, ... ] list of all the dates as pd.Timestamps or str
    """
    date_list = []
    if satellite == cts.GOES_SATELLITE_GLM:
        SatPathParser = GLMPathParser
    else:
        raise ValueError(f'{satellite} {cts.SAT_VALUE_ERROR}')
    for p in path_list:
        date = SatPathParser(p, regrid=regrid, directory=directory) \
            .get_start_date_pdTimestamp(ignore_missing_start_hour=True)
        if date_str:
            date = date.strftime(date_format)
        date_list.append(date)

    return date_list


def get_list_of_sat_files(sat_dir_path, parent_dir, sat_name, regrid, regrid_res_str=cts.GRID_RESOLUTION_STR):
    """
    Function returning a list of all satellite data files in a given directory (or in the subdirectories of a parent directory)
    @param sat_dir_path: <list> [ <pathlib.Path>, ... ] or <pathlib.Path> or <str>
    @param parent_dir: <bool> indicates if sat_dir_path points to a parent directory
    @param sat_name: <str> satellite name (supported so far: 'GOES_GLM')
    @param regrid: <bool> indicates if sat files to be listed are regridded
    @param regrid_res_str: <str> grid resolution if regrid=True
    @return: <list> [ <pathlib.Path>, ... ]
    """
    # check if sat_dir_path is a single path (puts it in list, easier to loop through)
    if isinstance(sat_dir_path, (pathlib.PurePath, str)):
        sat_dir_path = [str_to_path(sat_dir_path)]
    # if not single path AND not list --> TypeError
    elif not isinstance(sat_dir_path, list):
        raise TypeError('Expecting list of pathlib.Path (or str) objects or single pathlib.Path (or str) object')

    # if parent directory --> get path of all subdirectories containing sat files
    if parent_dir:
        dirname_pattern = generate_sat_dirname_pattern(sat_name=sat_name, regrid=regrid, regrid_res_str=regrid_res_str)
        dir_list = []
        for parent_dir_path in sat_dir_path:
            # check that parent_dir_path is a pathlib.Path object (needed for glob function)
            parent_dir_path = str_to_path(parent_dir_path)
            dir_list.extend(parent_dir_path.glob(dirname_pattern))
        sat_dir_path = dir_list
    # get list of files
    filename_pattern = generate_sat_hourly_filename_pattern(sat_name=sat_name, regrid=regrid, regrid_res_str=regrid_res_str)
    file_list = []
    for dir_path in sat_dir_path:
        if not parent_dir: # if not parent dir, check if dir_path is a pathlib object
            dir_path = str_to_path(dir_path)
        file_list.extend(dir_path.glob(filename_pattern))

    return sorted(file_list)
