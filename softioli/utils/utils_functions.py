from datetime import datetime
import numpy as np
import pandas as pd
import pathlib

from .GLMPathParser import GLMPathParser
from .constants import OUTPUT_ROOT_DIR

#TODO: jsp si vraiment utile, peut-être juste pour GLMPathParser mais bizarre ce truc quand même
def date_to_pd_timestamp(date_to_check):
    """
    Function to convert date or GLMPathParser to pandas.Timestamp object
    Expecting pd.Timestamp OR datetime.datetime object OR numpy.datetime64 object OR GLMPathParser
    :param date_to_check: <pd.Timestamp> or <datetime.datetime> or <nup.datetime64> or <GLMPathParser> object
    :return: <pd.Timestamp>
    """
    # expecting a pd.Timestamp OR a datetime object OR np.datetime64 object OR GLMPathParser
    if not isinstance(date_to_check, pd.Timestamp):
        if isinstance(date_to_check, datetime) or isinstance(date_to_check, np.datetime64):
            return pd.Timestamp(date_to_check)
        elif isinstance(date_to_check, pathlib.PurePath):
            return GLMPathParser(date_to_check, directory=True, regrid=True).get_start_date_pdTimestamp(
                ignore_missing_start_hour=True)
        elif isinstance(date_to_check, GLMPathParser):
            return date_to_check.get_start_date_pdTimestamp(ignore_missing_start_hour=True)
        else:
            raise TypeError('Expecting pandas.Timestamp, datetime.datime, xarray.DataArray or GLMPathParser object')
    else:
        return date_to_check


def get_lon_varname(ds):
    ds_keys = ds.keys()
    if 'longitude' in ds_keys:
        label = 'longitude'
    elif 'lon' in ds_keys:
        label = 'lon'
    else:
        raise ValueError('neither "longitude" nor "lon" dimension found in ds')
    return label

def get_lat_varname(ds):
    ds_keys = ds.keys()
    if 'latitude' in ds_keys:
        label = 'latitude'
    elif 'lat' in ds_keys:
        label = 'lat'
    else:
        raise ValueError('neither "latitude" nor "lat" dimension found in ds')
    return label


def get_lon_lat_varnames(ds):
    return get_lon_varname(ds=ds), get_lat_varname(ds=ds)


def create_root_output_dir(date, dirname_suffix, root_dirpath=OUTPUT_ROOT_DIR):
    """
    Returns path to directory in which results should be stored and creates it if directory doesn't exist
    By default will return: /o3p/patj/SOFT-IO-LI_output/<date>_<output_dirname>
    @param date: <pd.Timestamp> or <str> expected format: YYYY-MM-DD_HHMM
    @param dirname_suffix: <str>
    @param root_dirpath: <pathlib.Path> or <str>
    @return: <pathlib.Path>
    """
    output_dirpath = pathlib.Path(f'{root_dirpath}/{date}_{dirname_suffix}')
    if not output_dirpath.exists():
        output_dirpath.mkdir(parents=True)
    return output_dirpath

def create_flight_output_dir(output_dirpath, flight_name, dirname_suffix='', missing_ok=True):
    """
    Returns path to directory in which results for a particular flight should be stored
    + Creates directory if it doesn't exist
    By default will return: <root_output_dirname>/<flight>_<dirname>
    @param output_dirpath: <pathlib.Path> or <str>
    @param flight_name: <str> or <int>
    @param dirname_suffix: <str>
    @param missing_ok: <bool> if True, directory is created, if False error raised
    @return: <pathlib.Path>
    """
    flight_output_dirpath = pathlib.Path(f'{output_dirpath}/{flight_name}{dirname_suffix}')
    if not flight_output_dirpath.exists():
        if missing_ok:
            flight_output_dirpath.mkdir(parents=True)
            print(f'Creating directory {flight_output_dirpath}')
        else:
            raise FileNotFoundError(f'{flight_output_dirpath} does NOT exist!')
    return flight_output_dirpath


def get_list_of_paths_between_two_values(dirpath, start_name, end_name, glob_pattern='*', subdir_glob_pattern=None):
    """
    Returns list of path to files or directories between start and end name (including start and end names)
    @param dirpath:
    @param start_name:
    @param end_name:
    @param glob_pattern:
    @param subdir_glob_pattern:
    @return:
    """
    dirpath = pathlib.Path(dirpath)
    all_paths = sorted(dirpath.glob(glob_pattern))
    res_list = []
    start_ok = False if start_name is not None else True
    for path in all_paths:
        pathname = path.name
        # if start name is found, put start_ok to True and then append all files until end_name is reached
        if pathname == start_name:
            start_ok = True
        if start_ok:
            res_list.append(path)
        if pathname == end_name:
            break
    if subdir_glob_pattern is not None:
        # only returns list of directory containing files or directories matching subdir_glob_pattern
        return [ p for p in res_list if len(list(p.glob(f'**/{subdir_glob_pattern}'))) > 0]
    else:
        return sorted(res_list)
