import pathlib

import constants as cts
from utils_functions import date_to_pd_timestamp

def generate_sat_hourly_filename_pattern(sat_name, regrid, regrid_res=cts.GRID_RESOLUTION_STR):
    """
    Generate filename pattern for a specific satellite and regrid resolution (to be used with pathlib glob function)
    :param sat_name: <str>
    :param regrid: <bool>
    :param regrid_res: <str>
    :return: <str> filename pattern for the satellite
    """
    if sat_name == cts.GOES_SATELLITE:
        # OR_GLM-L2-LCFA_Gxx_YYYY_DDD_HH-HH.nc
        filename_pattern = f'{cts.GLM_DIRNAME}_{cts.Gxx_PATTERN}_{cts.YYYY_pattern}_{cts.DDD_pattern}_{cts.HH_pattern}-{cts.HH_pattern}.nc'
    else:
        raise ValueError(f'{sat_name} NOT supported yet. Supported satellite so far: "GOES"')
    if regrid:
        return f'{regrid_res}_{filename_pattern}'
    else:
        return filename_pattern


def generate_sat_dir_path(date, satellite, regrid, regrid_res=cts.GRID_RESOLUTION_STR):
    """
    Generate the path to the directory containing the satellite data for a specific date (regridded or not)
    <!> The path does not necessarily point to an existing directory, if it does not exist it will need to be created and filled with the correct data files
    :param date: <pandas.Timestamp> or <numpy.datetime64> or <datetime.datetime> or <GLMPathParser>
    :param satellite: <str> satellite name
    :param regrid: <bool> indicates if the directory contains regridded files
    :param regrid_res: <str> regrid resolution (if regrid == True)
    :return: <pathlib.Path> object pointing to satellite data directory for a specific date
    """
    # check date
    date = date_to_pd_timestamp(date)
    # now that we have the pandas.Timestamp we can generate the path
    if satellite == cts.GOES_SATELLITE:
        if regrid:
            return pathlib.Path(
                f'{cts.REGRID_GLM_ROOT_DIR}/{date.year}/{regrid_res}_{cts.GLM_DIRNAME}_{date.year}_{date.dayofyear:03d}')
        else:
            return pathlib.Path(
                f'{cts.PRE_REGRID_GLM_ROOT_DIR}/{date.year}/{cts.GLM_DIRNAME}_{date.year}_{date.dayofyear:03d}')
    else:
        raise ValueError(f'Satellite {satellite} not supported yet. Only GOES satellite supported for now')


def generate_sat_hourly_file_path(date, satellite, sat_version, regrid, regrid_res=cts.GRID_RESOLUTION_STR):
    """
    Generate absolute path to a satellite hourly file (regridded or not)
    <!> The path does not necessarily point to an existing file, it might point to a file that has yet to be created
    :param date: <pandas.Timestamp> or <numpy.datetime64> or <datetime.datetime> or <GLMPathParser>
    :param satellite: <str> satellite name
    :param sat_version: <str> satellite version e.g.: 'G16' for GOES satellite
    :param regrid: <bool> indicates if the file is regridded
    :param regrid_res: <str> regrid resolution (if regrid == True)
    :return: <pathlib.Path> object pointing to satellite hourly data file
    """
    date = date_to_pd_timestamp(date)
    dir_path = generate_sat_dir_path(date=date, satellite=satellite, regrid=regrid, regrid_res=regrid_res)
    if satellite == cts.GOES_SATELLITE:
        filename = f'{cts.GLM_DIRNAME}_{sat_version}_{date.year}_{date.dayofyear:03d}_{date.hour:02d}-{(date.hour+1):02d}.nc'
        if regrid:
            return dir_path / pathlib.Path(f'{regrid_res}_{filename}')
        else:
            return dir_path / pathlib.Path(filename)
    else:
        raise ValueError(f'Satellite {satellite} not supported yet. Only GOES satellite supported for now')