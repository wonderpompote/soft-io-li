from datetime import datetime
import numpy as np
import pandas as pd
import pathlib
import warnings
import xarray as xr

import fpout

from .GLMPathParser import GLMPathParser

def str_to_path(path_to_convert):
    if isinstance(path_to_convert, str):
        return pathlib.Path(path_to_convert)
    elif isinstance(path_to_convert, pathlib.PurePath):
        return path_to_convert
    else:
        raise TypeError('Expecting str or pathlib object')

# TODO: supprimer ! elle est dans fpout_sat_comparison (get_fpout_da)
def get_fp_da(fp_path, sum_height=True, load=False, chunks='auto', max_chunk_size=1e8,
              assign_releases_position_coords=False):
    if not str_to_path(fp_path).exists():
        raise ValueError(f'fp_path {fp_path} does NOT exist')
    fp_ds = fpout.open_fp_dataset(fp_path, chunks=chunks, max_chunk_size=max_chunk_size,
                                  assign_releases_position_coords=assign_releases_position_coords)
    fp_da = fp_ds.spec001_mr
    fp_da = fp_da.squeeze()
    if 'pointspec' in fp_da.dims:
        fp_da = fp_da.assign_coords(pointspec=fp_da.pointspec)
    if sum_height:
        fp_da = fp_da.sum('height')
    if load:
        fp_da.load()
    return fp_da

# TODO: supprmier (elle est dans fpout_sat_comparison maintenant je crois
def get_glm_da_PROVISOIRE(glm_path):
    glm_ds = xr.open_dataset(glm_path)
    glm_da = glm_ds.flash_count
    glm_da = glm_da.sortby(glm_da.time, ascending=False)
    """ <!> attention quand on aura GOES-E et GOES-W faudra prévoir ce cas là <!> """
    # GOES East covers approximately longitudes between -130 and -10 and latitudes between -60 and 60
    _glm_da_fillna = glm_da.sel(latitude=slice(-60, 60), longitude=slice(-135, -10)).fillna(0.)
    glm_da = xr.merge([glm_da, _glm_da_fillna]).flash_count
    return glm_da

#TODO: mettre à jour le nom et la fonction elle même / la supprimer d'ici si je a met dans fpout_sat_comparison
def get_fp_glm_ds(fp_da, glm_da, sum_height=True, load_fp_da=False):
    if isinstance(fp_da, str) or isinstance(fp_da, pathlib.PurePath):
        fp_da = get_fp_da(fp_path=fp_da, sum_height=sum_height, load=load_fp_da)
    if isinstance(glm_da, str) or isinstance(glm_da, pathlib.PurePath):
        glm_da = get_glm_da_PROVISOIRE(glm_path=glm_da)
    fp_glm_ds = xr.merge([fp_da, glm_da])
    fp_glm_ds = fp_glm_ds.sortby(fp_glm_ds.time, ascending=False)
    seven_days = np.timedelta64(7, 'D')
    end_date = fp_glm_ds.time.max() - seven_days
    fp_glm_ds = fp_glm_ds.where(fp_glm_ds['time'] >= end_date, drop=True)
    return fp_glm_ds


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


#TODO update with new fp out notation
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
