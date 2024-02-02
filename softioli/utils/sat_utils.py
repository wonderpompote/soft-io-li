import pathlib

from .utils_functions import date_to_pd_timestamp, str_to_path
from . import constants as cts
from . import GLMPathParser

def generate_sat_hourly_filename_pattern(sat_name, regrid, regrid_res=cts.GRID_RESOLUTION_STR):
    """
    Generate filename pattern for a specific satellite and regrid resolution (to be used with pathlib glob function)
    :param sat_name: <str>
    :param regrid: <bool>
    :param regrid_res: <str>
    :return: <str> filename pattern for the satellite
    """
    if sat_name == cts.GOES_SATELLITE_GLM:
        # OR_GLM-L2-LCFA_Gxx_YYYY_DDD_HH-HH.nc
        filename_pattern = f'{cts.GLM_PATH_PREFIX}_{cts.Gxx_PATTERN}_{cts.YYYY_pattern}_{cts.DDD_pattern}_{cts.HH_pattern}-{cts.HH_pattern}.nc'
    else:
        raise ValueError(f'{sat_name} NOT supported yet. Supported satellite so far: "GOES_GLM"')
    if regrid:
        return f'{regrid_res}_{filename_pattern}'
    else:
        return filename_pattern

def generate_sat_dirname_pattern(sat_name, regrid, regrid_res=cts.GRID_RESOLUTION_STR):
    """
    Generate directory name pattern for a specific satellite and regrid resolution (to be used with pathlib glob function)
    :param sat_name: <str>
    :param regrid: <bool>
    :param regrid_res: <str>
    :return: <str> directory name pattern for the satellite
    """
    if sat_name == cts.GOES_SATELLITE_GLM:
        # OR_GLM-L2-LCFA_Gxx_YYYY_DDD
        dirname_pattern = f'{cts.GLM_PATH_PREFIX}_{cts.Gxx_PATTERN}_{cts.YYYY_pattern}_{cts.DDD_pattern}'
    else:
        raise ValueError(f'{sat_name} NOT supported yet. Supported satellite so far: "GOES_GLM"')
    if regrid:
        return f'{regrid_res}_{dirname_pattern}'
    else:
        return dirname_pattern


def generate_sat_dir_path(date, satellite, regrid, regrid_res_str=cts.GRID_RESOLUTION_STR):
    """
    Generate the path to the directory containing the satellite data for a specific date (regridded or not)
    <!> The path does not necessarily point to an existing directory, if it does not exist it will need to be created and filled with the correct data files
    :param date: <pandas.Timestamp> or <numpy.datetime64> or <datetime.datetime> or <GLMPathParser>
    :param satellite: <str> satellite name
    :param regrid: <bool> indicates if the directory contains regridded files
    :param regrid_res_str: <str> regrid resolution (if regrid == True)
    :return: <pathlib.Path> object pointing to satellite data directory for a specific date
    """
    # check date
    date = date_to_pd_timestamp(date)
    # now that we have the pandas.Timestamp we can generate the path
    if satellite == cts.GOES_SATELLITE_GLM:
        if regrid:
            return pathlib.Path(
                f'{cts.REGRID_GLM_ROOT_DIR}/{date.year}/{regrid_res_str}_{cts.GLM_PATH_PREFIX}_{date.year}_{date.dayofyear:03d}')
        else:
            return pathlib.Path(
                f'{cts.PRE_REGRID_GLM_ROOT_DIR}/{date.year}/{cts.GLM_PATH_PREFIX}_{date.year}_{date.dayofyear:03d}')
    else:
        raise ValueError(f'{satellite} {cts.SAT_VALUE_ERROR}')


def generate_sat_hourly_file_path(date, satellite, sat_version, regrid, regrid_res=cts.GRID_RESOLUTION_STR, dir_path=None):
    """
    Generate absolute path to a satellite hourly file (regridded or not)
    <!> The path does not necessarily point to an existing file, it might point to a file that has yet to be created
    :param date: <pandas.Timestamp> or <numpy.datetime64> or <datetime.datetime> or <GLMPathParser>
    :param satellite: <str> satellite name
    :param sat_version: <str> satellite version e.g.: 'G16' for GOES satellite
    :param regrid: <bool> indicates if the file is regridded
    :param regrid_res: <str> regrid resolution (if regrid == True)
    :param dir_path: <str> or <pathlib.Path> mostly used for testing purposes, if == None the default directory path is used
    :return: <pathlib.Path> object pointing to satellite hourly data file
    """
    date = date_to_pd_timestamp(date)
    if dir_path is None:
        dir_path = generate_sat_dir_path(date=date, satellite=satellite, regrid=regrid, regrid_res_str=regrid_res)
    else: # mostly used for testing purposes
        dir_path = str_to_path(dir_path)
        if not dir_path.exists():
            dir_path.mkdir()
    if satellite == cts.GOES_SATELLITE_GLM:
        filename = f'{cts.GLM_PATH_PREFIX}_{sat_version}_{date.year}_{date.dayofyear:03d}_{date.hour:02d}-{(date.hour + 1):02d}.nc'
        if regrid:
            return dir_path / pathlib.Path(f'{regrid_res}_{filename}')
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
        for p in path_list:
            date = GLMPathParser(p, regrid=regrid, directory=directory) \
                        .get_start_date_pdTimestamp(ignore_missing_start_hour=True)
            if date_str:
                date = date.strftime(date_format)
            date_list.append(date)
    else:
        raise ValueError(f'{satellite} {cts.SAT_VALUE_ERROR}')

    return date_list
