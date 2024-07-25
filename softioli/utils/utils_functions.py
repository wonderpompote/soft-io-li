from datetime import datetime
import numpy as np
import pandas as pd
import pathlib

from .GLMPathParser import GLMPathParser
from .constants import OUTPUT_ROOT_DIR

def str_to_path(path_to_convert):
    if isinstance(path_to_convert, str):
        return pathlib.Path(path_to_convert)
    elif isinstance(path_to_convert, pathlib.PurePath):
        return path_to_convert
    else:
        raise TypeError('Expecting str or pathlib object')


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

def create_flight_output_dir(output_dirpath, flight_name, dirname_suffix=''):
    """
    Returns path to directory in which results for a particular flight should be stored
    + Creates directory if it doesn't exist
    By default will return: <root_output_dirname>/<flight>_<dirname>
    @param output_dirpath: <pathlib.Path> or <str>
    @param flight_name: <str> or <int>
    @param dirname_suffix: <str>
    @return: <pathlib.Path>
    """
    flight_output_dirpath = pathlib.Path(f'{output_dirpath}/{flight_name}{dirname_suffix}')
    if not flight_output_dirpath.exists():
        flight_output_dirpath.mkdir(parents=True)
    return flight_output_dirpath
