from datetime import datetime
import numpy as np
import pathlib
import warnings
import xarray as xr

import fpout


def str_to_path(path_to_convert):
    if isinstance(path_to_convert, str):
        return pathlib.Path(path_to_convert)
    elif isinstance(path_to_convert, pathlib.PurePath):
        return path_to_convert
    else:
        raise TypeError('Expecting str or pathlib object')

def get_fp_da(fp_path, sum_height=True, load=False, chunks='auto', max_chunk_size=1e8,
              assign_releases_position_coords=False):
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


def get_glm_da_PROVISOIRE(glm_path):
    glm_ds = xr.open_dataset(glm_path)
    glm_da = glm_ds.flash_count
    glm_da = glm_da.sortby(glm_da.time, ascending=False)
    """ <!> attention quand on aura GOES-E et GOES-W faudra prévoir ce cas là <!> """
    # GOES East covers approximately longitudes between -130 and -10 and latitudes between -60 and 60
    _glm_da_fillna = glm_da.sel(latitude=slice(-60, 60), longitude=slice(-135, -10)).fillna(0.)
    glm_da = xr.merge([glm_da, _glm_da_fillna]).flash_count
    return glm_da


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


def check_file_exists_with_suffix(path, file_suffix='.nc'):
    """
    Function to check is a given path points to existing file with specific file_suffix
    :param path: <str> or <pathlib.Path>
    :param file_suffix: '.nc' by default
    :return: <bool>
    """
    if not isinstance(path, pathlib.PurePath):
        if type(path) == str:
            path = pathlib.Path(path)
        else:
            raise TypeError(f'Expecting <str> or <pathlib.PurePath> object, not {type(path)}')
    return path.exists() and path.suffix == file_suffix


############# used by the old version of GLM regrid ####################################
def get_np_datetime64_from_string(year, day_of_year, hour=0, mins=0, secs=0, month=None, day=None):
    """
    Function to generate a np datetime64 object with specific year, day of year (or day + month), hour, min and sec
    from a string using datetime.strptime (for now only way to get date from a day of year string)
    Expecting str or int values
    Used to get the date (taken from the glm nc filename) that will be added to the target 0.5deg glm dataset
    :param year: year
    :param day_of_year: day of the year
    :param hour: hour, default = 0
    :param mins: minutes, default = 0
    :param secs: seconds, default = 0
    :param month: month number (given if day_of_year is unknown and put to 0 or None)
    :param day: day of the month (given if day_of_year is unknown and put to 0 or None)
    :return: <numpy.datetime64>
    """
    # if day_of_year, month and day are all None raise exception
    if all(d is None for d in [day_of_year, month, day]) or all(d == 0 for d in [day_of_year, month, day]):
        raise ValueError(f'Day of year or month and day values required')
    if day_of_year is not None or day_of_year != 0:
        # if day_of_year AND month and day are all not None, raise warning and use day_of_year
        if all(d is not None for d in [month, day]):
            warnings.warn('Expecting day of the year OR month and day values, not both. Day of year value will be used',
                          Warning)
        date_iso_str = datetime.strptime(f'{year}-{day_of_year}-{hour}-{mins}-{secs}', '%Y-%j-%H-%M-%S').isoformat()
    elif day_of_year is None and all(d is not None for d in [month, day]):
        date_iso_str = datetime.strptime(f'{year}-{day_of_year}-{hour}-{mins}-{secs}', '%Y-%j-%H-%M-%S').isoformat()
    else:
        raise TypeError('Day_of_year value equal to None and month or day value missing')
    return np.datetime64(date_iso_str)
