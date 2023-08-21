from datetime import datetime
import numpy as np
import pandas as pd
from pathlib import Path
import xarray as xr
import warnings

def check_str_to_Path(path_to_check):
    """
    Function to check if path_to_check is Path instance. 
    If not, check if path_to_check is a str and convert it to pathlib.Path object
    :path_to_check: should be Path or str
    :return: path_to_check as a pathlib.Path object or raise exception if path_to_check not Path nor str
    """
    if not isinstance(path_to_check, Path):
        if isinstance(path_to_check, str):
            path_to_check = Path(path_to_check)
        else:
            raise TypeError(f'given path should be str or Path object, not {type(path_to_check)}')
    return path_to_check

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


def np_datetime64_to_datetime(np_date):
    """
    Function to convert a numpy.datetime64 object to a datetime.datetime object using pandas.to_datetime function
    :param np_date: date to convert
    :type np_date: <numpy.datetime64> or <xarray.DataArray> if value contained is <numpy.datetime64>
    :return: converted date
    :rtype: <pandas.Timestamp> (pandas equivalent of <datetime.datetime>)
    """
    if isinstance(np_date, xr.DataArray):
        np_date = np_date.values
    if isinstance(np_date, np.datetime64):
        return pd.to_datetime(np_date)
    else:
        raise TypeError("wrong date format, expecting np.datetime64 object")



