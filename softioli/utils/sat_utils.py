import pathlib

import pandas as pd
from pandas import Timedelta

from .utils_functions import date_to_pd_timestamp
from . import constants as cts
from . import GLMPathParser, OLD_GLM_PRE_REGRID_TEMP_NOTATION, OLD_GLM_NOTATION
from .ABIPathParser import ABIPathParser

def generate_sat_filename_pattern(sat_name, regrid, regrid_res_str=cts.GRID_RESOLUTION_STR, hourly=True, naming_convention=None, sat_version_pattern=None,
                                  YYYY=cts.YYYY_pattern, DDD=cts.DDD_pattern, MM=cts.MM_pattern, DD=cts.DD_pattern, start_HH=cts.HH_pattern, end_HH=cts.HH_pattern, mm=cts.mm_pattern, sss=cts.sss_pattern):
    """
    Generate filename pattern for a specific satellite, naming convention and regrid resolution (to be used with pathlib glob function)
    :param sat_name: <str> name of the satellite (only 'GOES_GLM' supported for now)
    :param regrid: <bool>
    :param regrid_res_str: <str> grid resolution str (to be added to the resulting filename)
    :param naming_convention: <str> file naming convention (mostly for backward compatibility). Supported values: 'OLD_TEMP', 'OLD', None (default)
    :param YYYY: <str> or <int> year
    :param DDD: <str> or <int> day of the year
    :param start_HH: <str> or <int> start hour
    :param end_HH:  <str> or <int> end hour
    :return: <str> filename pattern for the satellite
    """
    # GLM
    if sat_name == cts.GOES_SATELLITE_GLM:
        sat_version_pattern = cts.GLM_Gxx_PATTERN if sat_version_pattern is None else sat_version_pattern
        if not hourly: # OR_GLM-L2-LCFA_Gxx_sYYYYDDDHHmmsss_e2YYYYDDDHHmmsss_cYYYYDDDHHmmsss.nc
            filename_pattern = f'{cts.GLM_PATH_PREFIX}_{sat_version_pattern}_s{YYYY}{DDD}{start_HH}{mm}{sss}_e{YYYY}{DDD}{end_HH}{cts.mm_pattern}{cts.sss_pattern}_c{YYYY}{DDD}{cts.HH_pattern}{cts.mm_pattern}{cts.sss_pattern}.nc'
        elif naming_convention is None:
            # OR_GLM-L2-LCFA_Gxx_YYYY_DDD_HH-HH.nc
            filename_pattern = f'{cts.GLM_PATH_PREFIX}_{sat_version_pattern}_{YYYY}_{DDD}_{start_HH}-{end_HH}.nc'
        elif naming_convention == OLD_GLM_PRE_REGRID_TEMP_NOTATION:
            # GLM_array_DDD_temp_HH.nc
            filename_pattern = f'GLM_array_{DDD}_temp_{start_HH}.nc'
        elif naming_convention == OLD_GLM_NOTATION:
            # GLM_array(_xxdeg)_DDD_HH1-HH2.nc
            if regrid:
                regrid_pattern = f'{regrid_res_str}_'
            else:
                regrid_pattern = ''
            filename_pattern = f'GLM_array_{regrid_pattern}{DDD}_{start_HH}-{end_HH}.nc'
        else:
            raise ValueError(f'Usupported naming convention for {sat_name} satellite. Supported values: "{OLD_GLM_PRE_REGRID_TEMP_NOTATION}", "{OLD_GLM_NOTATION}" or None')
    # ABI
    elif sat_name == cts.GOES_SATELLITE_ABI:
        sat_version_pattern = cts.ABI_GOESXX_PATTERN if sat_version_pattern is None else sat_version_pattern
        if not hourly:  # GEO_L1B-GOES1x_YYYY-MM-DDTHH-mm-ss_X_IR10x_V1-0x.hdf
            filename_pattern = f'GEO_L1B-{sat_version_pattern}_{YYYY}-{MM}-{DD}T{start_HH}-{mm}-{mm}_[NSG]_IR10[37]_V1-0[4-6].hdf'
        else:  # ABI_GEO_L1B-GOES1x_YYYY_MM_DD_HH1-HH2.nc or xxdeg_ABI_GEO_L1B-GOES1x_YYYY_MM_DD_HH1-HH2.nc
            filename_pattern = f"ABI_GEO_L1B-{sat_version_pattern}_{YYYY}_{MM}_{DD}_{start_HH}-{end_HH}.nc"
    else:
        raise ValueError(
            f'{sat_name} NOT supported yet. Supported satellite so far: "{cts.GOES_SATELLITE_GLM}", "{cts.GOES_SATELLITE_ABI}"')

    if regrid and naming_convention is None and hourly:
        return f'{regrid_res_str}_{filename_pattern}'
    else:
        return filename_pattern


def generate_sat_dirname_pattern(sat_name, regrid, regrid_res_str=cts.GRID_RESOLUTION_STR, naming_convention=None, YYYY=cts.YYYY_pattern, DDD=cts.DDD_pattern, MM=cts.MM_pattern, DD=cts.DD_pattern):
    """
    Generate directory name pattern for a specific satellite and regrid resolution (to be used with pathlib glob function)
    :param sat_name: <str>
    :param regrid: <bool>
    :param regrid_res_str: <str>
    :param naming_convention: <str> file naming convention (mostly for backward compatibility). Supported values: 'OLD', 'OLD_TEMP', None (default)
    :return: <str> directory name pattern for the satellite
    """
    # GLM
    if sat_name == cts.GOES_SATELLITE_GLM:
        if naming_convention is None:
            # OR_GLM-L2-LCFA_YYYY_MM_DD
            dirname_pattern = f'{cts.GLM_PATH_PREFIX}_{YYYY}_{MM}_{DD}'
        elif naming_convention == OLD_GLM_PRE_REGRID_TEMP_NOTATION:
            #OR_GLM-L2-LCFA_Gxx_sYYYYDDD
            dirname_pattern = f'{cts.GLM_PATH_PREFIX}_{cts.GLM_Gxx_PATTERN}_s{YYYY}{DDD}'
        elif naming_convention == OLD_GLM_NOTATION:
            # GLM_array(_05deg)_DDD
            if regrid:
                regrid_pattern = f'{regrid_res_str}_'
            else:
                regrid_pattern = ''
            dirname_pattern = f'GLM_array_{regrid_pattern}{DDD}'
        else:
            raise ValueError(f'Usupported naming convention for {sat_name} satellite. Supported values: "{OLD_GLM_PRE_REGRID_TEMP_NOTATION}", "{OLD_GLM_NOTATION}" or None')
    # ABI
    elif sat_name == cts.GOES_SATELLITE_ABI: #ABI_GEO_L1B_YYYY_MM_DD
        dirname_pattern = f"ABI_GEO_L1B_{YYYY}_{MM}_{DD}"
    else:
        raise ValueError(f'{sat_name} {cts.SAT_VALUE_ERROR}')

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
                f'{root_dir_path}/{cts.REGRID_GLM_DIRNAME}/{date.year:04d}/{regrid_res_str}_{cts.GLM_PATH_PREFIX}_{date.year:04d}_{date.month:02d}_{date.day:02d}')
        else:
            return pathlib.Path(
                f'{root_dir_path}/{cts.PRE_REGRID_GLM_DIRNAME}/{date.year:04d}/{cts.GLM_PATH_PREFIX}_{date.year:04d}_{date.month:02d}_{date.day:02d}')
    # ABI
    elif sat_name == cts.GOES_SATELLITE_ABI:
        root_dir_path = target_dir if target_dir is not None else cts.ABI_ROOT_DIR
        if regrid:
            return pathlib.Path(
                f'{root_dir_path}/{cts.REGRID_ABI_DIRNAME}/{date.year:04d}/{regrid_res_str}_{cts.ABI_PATH_PREFIX}_{date.year:04d}_{date.month:02d}_{date.day:02d}')
        else:
            return pathlib.Path(
                f'{root_dir_path}/{cts.PRE_REGRID_ABI_DIRNAME}/{date.year:04d}/{cts.ABI_PATH_PREFIX}_{date.year:04d}_{date.month:02d}_{date.day:02d}')
    else:
        raise ValueError(f'{sat_name} {cts.SAT_VALUE_ERROR}')


def generate_sat_hourly_file_path(date, sat_name, satellite, regrid, regrid_res_str=cts.GRID_RESOLUTION_STR,
                                  dir_path=None):
    """
    Generate absolute path to a satellite hourly file (regridded or not)
    <!> The path does not necessarily point to an existing file, it might point to a file that has yet to be created
    :param date: <pandas.Timestamp> or <numpy.datetime64> or <datetime.datetime> or <GLMPathParser>
    :param sat_name: <str> satellite name
    :param satellite: <str> satellite version e.g.: 'G16' for GLM or 'GOES16' for ABI
    :param regrid: <bool> indicates if the file is regridded
    :param regrid_res_str: <str> regrid resolution (if regrid == True)
    :param dir_path: <str> or <pathlib.Path> mostly used for testing purposes, if == None the default directory path is used
    :return: <pathlib.Path> object pointing to satellite hourly data file
    """
    date = date_to_pd_timestamp(date)
    dir_path = generate_sat_dir_path(date=date, sat_name=sat_name, regrid=regrid, regrid_res_str=regrid_res_str,
                                     target_dir=dir_path)
    if not dir_path.exists():
        dir_path.mkdir(parents=True)
    if sat_name == cts.GOES_SATELLITE_GLM:
        filename = f'{cts.GLM_PATH_PREFIX}_{satellite}_{date.year:04d}_{date.dayofyear:03d}_{date.hour:02d}-{(date + Timedelta(hours=1)).hour:02d}.nc'
    elif sat_name == cts.GOES_SATELLITE_ABI:
        filename = f'{cts.ABI_PATH_PREFIX}-{satellite}_{date.year:04d}_{date.month:02d}_{date.day:02d}_{date.hour:02d}-{(date + Timedelta(hours=1)).hour:02d}.nc'
    else:
        raise ValueError(f'{sat_name} {cts.SAT_VALUE_ERROR}')
    if regrid:
        filename = f'{regrid_res_str}_{filename}'
    return dir_path / pathlib.Path(filename)


def get_list_of_dates_from_list_of_sat_path(path_list, directory, sat_name, regrid, date_str, hourly=True, date_str_format='%Y-%m-%d'):
    """
    Takes a list of satellite data paths (directories or files) and returns the corresponding dates
    extracted from the file/directory names
    @param hourly:
    @param path_list: <list> [ <pathlib.Path> or <str>, ... ]
    @param directory: <bool> indicates if the paths point to directories
    @param sat_name: <str> satellite name
    @param regrid: <bool> indicates if the paths point to regridded files or directories
    @param date_str: <bool> indicates if we want the dates as str
    @return: <list> [ <pd.Timestamp> or <str>, ... ] list of all the dates as pd.Timestamps or str
    """
    date_list = []
    if sat_name == cts.GOES_SATELLITE_GLM:
        SatPathParser = GLMPathParser
    elif sat_name == cts.GOES_SATELLITE_ABI:
        SatPathParser = ABIPathParser
    else:
        raise ValueError(f'{sat_name} {cts.SAT_VALUE_ERROR}')
    for p in path_list:
        date = SatPathParser(p, regrid=regrid, directory=directory, hourly=hourly) \
            .get_start_date_pdTimestamp(ignore_missing_start_hour=True)
        if date_str:
            date = date.strftime(date_str_format)
        date_list.append(date)

    return date_list


# TODO: jsp si ça me sert vraiment dans le code au final
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
        sat_dir_path = [pathlib.Path(sat_dir_path)]
    # if not single path AND not list --> TypeError
    elif not isinstance(sat_dir_path, list):
        raise TypeError('Expecting list of pathlib.Path (or str) objects or single pathlib.Path (or str) object')

    # if parent directory --> get path of all subdirectories containing sat files
    if parent_dir:
        dirname_pattern = generate_sat_dirname_pattern(sat_name=sat_name, regrid=regrid, regrid_res_str=regrid_res_str)
        dir_list = []
        for parent_dir_path in sat_dir_path:
            # check that parent_dir_path is a pathlib.Path object (needed for glob function)
            parent_dir_path = pathlib.Path(parent_dir_path)
            dir_list.extend(parent_dir_path.glob(dirname_pattern))
        sat_dir_path = dir_list
    # get list of files
    filename_pattern = generate_sat_filename_pattern(sat_name=sat_name, regrid=regrid, regrid_res_str=regrid_res_str)
    file_list = []
    for dir_path in sat_dir_path:
        if not parent_dir: # if not parent dir, make sure dir_path is a pathlib object
            dir_path = pathlib.Path(dir_path)
        file_list.extend(dir_path.glob(filename_pattern))

    return sorted(file_list)


def generate_sat_dir_list_between_start_end_date(start_date, end_date, satellite, regrid,
                                                 regrid_res_str=cts.GRID_RESOLUTION_STR):
    """
    Generate list (iter) of daily directory path containing satellite data between start and end date
    <!> does not necessarily generate directory paths <!>
    :param start_date: <pandas.Timestamp> or <numpy.datetime64> or <datetime.datetime>
    :param end_date: <pandas.Timestamp> or <numpy.datetime64> or <datetime.datetime>
    :param satellite: <str> satellite name
    :param regrid: <bool> indicates if the directory contains regridded files
    :param regrid_res_str: <str> regrid resolution
    :return: <list>
    """
    # make sure the dates are pd.Timestamps
    start_date = date_to_pd_timestamp(start_date)
    end_date = date_to_pd_timestamp(end_date)
    dir_list = [
        generate_sat_dir_path(
            date=start_date + pd.Timedelta(i, 'D'), sat_name=satellite,
            regrid=regrid, regrid_res_str=regrid_res_str
        )
        for i in range((end_date - start_date).days + 1)
    ]
    return dir_list


# TODO: add check dir_list contient que des pathlib.PurePath objects (?)
def get_sat_files_list_between_start_end_date(dir_list, start_date, end_date, sat_name, regrid, hourly=True):
    if sat_name == cts.GOES_SATELLITE_GLM:
        SatPathParser = GLMPathParser
    elif sat_name == cts.GOES_SATELLITE_ABI:
        SatPathParser = ABIPathParser
    else:
        raise ValueError(f'{sat_name} {cts.SAT_VALUE_ERROR}')
    start_date, end_date = date_to_pd_timestamp(start_date), date_to_pd_timestamp(end_date)
    file_list = []
    dir_list = sorted(dir_list)
    fname_pattern = generate_sat_filename_pattern(sat_name=sat_name, regrid=regrid, hourly=hourly)
    for file in dir_list[0].glob(fname_pattern):
        fparser = SatPathParser(file_url=file, regrid=regrid, hourly=hourly)
        if fparser.start_hour >= start_date.hour:
            file_list.append(file)
    for file in dir_list[-1].glob(fname_pattern):
        fparser = SatPathParser(file_url=file, regrid=regrid, hourly=hourly)
        if fparser.start_hour <= end_date.hour:
            file_list.append(file)
    # for the days: start_day < day < end_day --> get all files matching generic filename pattern
    for dir_path in dir_list[1:-1]:
        file_list.extend(dir_path.glob(fname_pattern))
    return sorted(file_list)


def get_abi_coords_file(sat_version, file_version):
    if sat_version in ['GOES12', 'GOES13']:
        if file_version is not None:
            if file_version <= "V1-05":
                coords_file = cts.GOES_0750_C0_COORDS_FILE
            elif file_version >= "V1-06":
                coords_file = cts.GOES_0750_C1_COORDS_FILE
    elif sat_version == 'GOES16':
        coords_file = cts.GOESNG_0750_COORDS_FILE
    elif sat_version in ['GOES17', 'GOES18']:
        coords_file = cts.GOESNG_1370_COORDS_FILE
    else:
        raise ValueError(
            f'{sat_version} unsupported. Supported ABI satellites so far: "GOES12", "GOES13", "GOES16", "GOES17", "GOES18"')

    return pathlib.Path(f'{cts.ABI_COORDS_DIRPATH}/{coords_file}')
