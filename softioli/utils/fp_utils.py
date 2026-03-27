import numpy as np
import pandas as pd
import pathlib
import xarray as xr

from fpsim import check_fp_status
from fpout import open_fp_dataset

from .constants import YYYY_pattern

def get_arrival_timestamp_from_plume_csv_filename(csv_file_path):
    """
    Returns timestamp corresponding to flight arrival time (as indicated in plume csv filename)
    <!> expecting filename with following format: <flight_id>_arrivaltime-YYYYMMDD-HHmm_<extra_information>.csv
    :param csv_file_path: <pathlib.Path> or <str>
    :return: <pandas.Timestamp> arrival time
    """
    if not isinstance(csv_file_path, pathlib.Path):
        csv_file_path = pathlib.Path(csv_file_path)
    return pd.Timestamp(csv_file_path.name.split('_')[1].replace('arrivaltime-',''))

def get_timestamp_next_hour(timestamp):
    """
    Returns timestamp with next hour
    ex: pd.Timestamp('2018-06-03 12:50:00') will return pd.Timestamp('2018-06-03 13:00:00')
        pd.Timestamp('2018-06-03 07:03:00') will return pd.Timestamp('2018-06-03 08:00:00')
    :param: <pandas.Timestamp> or <str>
    :return: <pandas.Timestamp> timestamp to next hour
    """
    return pd.Timestamp(timestamp).floor('h') + pd.Timedelta(1,'h')


def get_fpout_nc_file_path_from_fp_dir(fp_dirpath, fp_output_dirname='output',
                                       nc_file_glob_pattern=f'grid_time_{YYYY_pattern}*.nc'):
    """
    Takes flexpart directory as argument and returns flexpart output netcdf file (if flexpart simulation was a success)
    @param fp_dirpath: <pathlib.Path> or <str> path to the flexpart directory
    @param fp_output_dirname: <str> output flexpart directory name (default='output')
    @param nc_file_glob_pattern: <str> pattern that should be used in the glob operation to find the .nc file
    @return: <pathlib.Path>
    """
    fp_dirpath = pathlib.Path(fp_dirpath)
    # check si fp success
    if check_fp_status(fp_dirpath):
        fp_output_dirpath = pathlib.Path(f'{fp_dirpath}/{fp_output_dirname}')
        nc_files = sorted(fp_output_dirpath.glob(nc_file_glob_pattern))
        if nc_files:
            return nc_files[0]
        else:
            raise FileNotFoundError(f"No matching .nc files in {fp_output_dirpath}")
    else:
        raise RuntimeError(f"Flexpart output status check failed for: {fp_dirpath}")


def get_fp_out_ds_xdays(fpout_path, days=7, sum_height=True, chunks='auto', max_chunk_size=1e8,
                        assign_releases_position_coords=False):
    """
    Function to retrieve a flexpart output dataset only for a specific number of days previous to the release
    :param fpout_path:
    :param days: number of days we should keep
    :param sum_height:
    :param chunks:
    :param max_chunk_size:
    :param assign_releases_position_coords:
    :return:
    """
    fp_ds = open_fp_dataset(fpout_path, chunks=chunks, max_chunk_size=max_chunk_size,
                            assign_releases_position_coords=assign_releases_position_coords)\
                                    .squeeze('nageclass')
    # rename numpoint dimension to pointspec
    fp_ds = fp_ds.rename({'numpoint': 'pointspec'})
    # get dataset containing releases info (RELxxxx variables)
    rel_ds = fp_ds.drop_vars([var for var in fp_ds.variables if not 'REL' in var])
    # fp simulation "start" date (ietime here because backwards)
    ietime = pd.Timestamp(f"{fp_ds.attrs['iedate']}{fp_ds.attrs['ietime']}")
    # fp release "start" dates (RELEND because backwards) --> get nearest hour before start
    release_start_dates = (ietime + fp_ds.RELEND).dt.ceil('h')
    # get "end" date (release_start_date - x days)
    end_dates = release_start_dates - np.timedelta64(days, 'D')
    # get spec001_mr over x days
    date_mask = ((fp_ds.time >= end_dates) & (fp_ds.time <= release_start_dates)).compute()
    fp_da = fp_ds.where(date_mask, drop=True).spec001_mr
    # merge rel info and spec001_mr
    fp_ds = xr.merge([fp_da, rel_ds])
    # sum over height
    if sum_height:
        fp_ds = fp_ds.sum('height', skipna=True)
    return fp_ds



def get_fp_out_da(fpout_path, sum_height=True, chunks='auto', max_chunk_size=1e8,
                  assign_releases_position_coords=False):
    """

    @param fpout_path:
    @param sum_height:
    @param chunks:
    @param max_chunk_size:
    @param assign_releases_position_coords:
    @return:
    """
    if not pathlib.Path(fpout_path).exists():
        raise ValueError(f'fp_path {fpout_path} does NOT exist')
    fp_ds = open_fp_dataset(fpout_path, chunks=chunks, max_chunk_size=max_chunk_size,
                            assign_releases_position_coords=assign_releases_position_coords)
    fp_da = fp_ds.spec001_mr
    fp_da = fp_da.squeeze()
    if 'pointspec' in fp_da.dims:
        fp_da = fp_da.assign_coords(pointspec=fp_da.pointspec)
    if sum_height:
        fp_da = fp_da.sum('height')
    return fp_da