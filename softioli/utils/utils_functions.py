from copy import deepcopy
from datetime import datetime
import json
import numpy as np
import pandas as pd
import pathlib
from warnings import warn

from .GLMPathParser import GLMPathParser

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


def write_plume_info_to_json_file(flight_name, info_name, data, json_path, missing_ok=False):
    if info_name == None:
        info_dict = data
    else:
        info_dict = {info_name: str(data)}
    json_path = pathlib.Path(json_path)
    if json_path.exists():
        with open(json_path, 'r+') as json_file:
            flight_data_json = json.load(json_file)
            flight_data_backup = deepcopy(flight_data_json)
    else:
        if missing_ok:  # create json file if missing
            json_path.touch()
            print(f'Created file {json_path}')
            flight_data_json = {}
        else:
            raise FileNotFoundError(f'{json_path} does not exist')

    try:
        flight_dict = flight_data_json.setdefault(flight_name, {})
        flight_dict.update(info_dict)
        with open(json_path, 'w') as json_file:
            json.dump(flight_data_json, json_file, indent=4)
    except Exception as e:
        warn(
            f'Error while writing new information to json: Exception: {e}\n"{info_name}": "{data}" not added to {flight_name} dictionary')
        with open(json_path, 'w') as json_file:
            json.dump(flight_data_backup, json_file, indent=4)


#TODO update with new fp out notation --> supprimer je crois
def get_fp_out_nc_file_list(parent_dir_path, old_fp_dirname=True):
    """
    Function to get a list of all flexpart output netcdf files available in parent_dir_path
    @param parent_dir_path: <str> or <pathlib.Path>
    @param old_fp_dirname: <bool> indicates if using fp dirname notation used by macc
    @return: <list> [ <pathlib.Path>, ... ]
    """
    parent_dir_path = str_to_path(parent_dir_path)
    if old_fp_dirname:
        fp_out_flight_dir_pattern = "flight_2018_0[0-3][0-9]_1h_05deg/10j_100k_output/"
        fp_file_pattern = "grid_time_*.nc"
    else:
        #TODO insert real pattern when fp notation has been decided
        raise ValueError('Only old fp dirname accepted for now')
    return parent_dir_path.glob(f"{fp_out_flight_dir_pattern}/{fp_file_pattern}")
