"""
STEP 1:
- verif fp out file (amélioration: faire avec methodes fpsim de pawel)
STEP 2:
- check if we have all GLM files *
STEP 3:
- retrieve all REGRID glm files into one dataset
STEP 4:
- recup flash count pondéré
STEP 5:
- mettre trucs dans la bdd iagos ou jsp trop quoi

* CHECK IF WE HAVE ALL FILES:
- recup liste de dossiers journaliers entre start et end date
    --> si manque des dossiers on stock la liste des dossiers manquants dans une liste (faudra récup la date à partir de ça)
        --> regarde si dossiers manquants (noms SANS 05deg devant) sont dans "raw"
            si NON:
            --> va chercher sur icare les fichiers manquants
            --> concat en fichiers horaires
        --> regrid fichiers horaires
- recup liste des fichiers dont on a besoin:
    - if start_date:
        --> tous les fichiers >= start_time
    - if end_date:
        --> tous les fichiers <= end_time
    - else:
        --> tous les fichiers du dossier
- merge tous les fichiers avec xr.open_mfdataset


<!> améliorations:
- pour l'instant indique cts.GOES_SATELLITE_GLM en dur quand doit passer satellite argument dans les fonction
    ---> faudra que ce soit un param pour que plus tard on puisse utiliser avec d'autres satellites
"""
import argparse
from datetime import datetime
import numpy as np
import pandas as pd
import pathlib
import xarray as xr

import fpout

from . import utils
from .utils import constants as cts
from .utils import GLMPathParser
from . import sat_regrid


def generate_sat_dir_list_between_start_end_date(start_date, end_date, satellite, regrid,
                                                 regrid_res_str=cts.GRID_RESOLUTION_STR):
    """
    Generate list (iter) of daily directory path containing satellite data between start and end date
    :param start_date: <pandas.Timestamp> or <numpy.datetime64> or <datetime.datetime>
    :param end_date: <pandas.Timestamp> or <numpy.datetime64> or <datetime.datetime>
    :param satellite: <str> satellite name
    :param regrid: <bool> indicates if the directory contains regridded files
    :param regrid_res_str: <str> regrid resolution
    :return: <list>
    """
    # make sure the dates are pd.Timestamps
    start_date = utils.date_to_pd_timestamp(start_date)
    end_date = utils.date_to_pd_timestamp(end_date)
    dir_list = [
        utils.generate_sat_dir_path(
            date=start_date + pd.Timedelta(i, 'D'), satellite=satellite,
            regrid=regrid, regrid_res_str=regrid_res_str
        )
        for i in range((end_date - start_date).days + 1)
    ]
    return dir_list


# TODO: dask !!
def get_fp_out_da(fpout_path, sum_height=True, load=False, chunks='auto', max_chunk_size=1e8,
                  assign_releases_position_coords=False):
    """

    @param fpout_path:
    @param sum_height:
    @param load:
    @param chunks:
    @param max_chunk_size:
    @param assign_releases_position_coords:
    @return:
    """
    if not utils.str_to_path(fpout_path).exists():
        raise ValueError(f'fp_path {fpout_path} does NOT exist')
    fp_ds = fpout.open_fp_dataset(fpout_path, chunks=chunks, max_chunk_size=max_chunk_size,
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


# TODO: suppr dry_run une fois que les tests sont finis
def get_satellite_ds(start_date, end_date, sat_name, grid_resolution=cts.GRID_RESOLUTION,
                     grid_res_str=cts.GRID_RESOLUTION_STR, overwrite=False, dry_run=False):
    """

    @param start_date:
    @param end_date:
    @param sat_name:
    @param grid_resolution:
    @param grid_res_str:
    @param overwrite:
    @param dry_run:
    @return:
    """
    start_date, end_date = utils.date_to_pd_timestamp(start_date), utils.date_to_pd_timestamp(end_date)
    # list of daily directories containing the hourly satellite data files between start and end date
    regrid_daily_dir_list = generate_sat_dir_list_between_start_end_date(start_date=start_date, end_date=end_date,
                                                                         satellite=sat_name, regrid=True)
    if sat_name == cts.GOES_SATELLITE_GLM:
        SatPathParser = GLMPathParser
    else:
        raise ValueError(f'{sat_name} {cts.SAT_VALUE_ERROR}')

    # get list of missing regrid sat dir
    missing_raw_daily_dir_list = {
        utils.generate_sat_dir_path(
            date=SatPathParser(regrid_dir_path, directory=True, regrid=True) \
                        .get_start_date_pdTimestamp(ignore_missing_start_hour=True),
            satellite=sat_name,
            regrid=False
        )
        for regrid_dir_path in regrid_daily_dir_list if not regrid_dir_path.exists()
    }
    # check if missing_raw_daily_dir_list is empty, if not --> check if pre-regrid directories exist
    if missing_raw_daily_dir_list:
        ########################################"
        print()
        print(f"regrid_daily_dir_list : {regrid_daily_dir_list}")
        print()
        print(f'missing_raw_daily_dir_list : {missing_raw_daily_dir_list}')
        print()
        ##########################################
        # directories to regrid (pre-regrid directory exist but NOT regrid directory)
        dir_to_regrid_list = {d_path for d_path in missing_raw_daily_dir_list if d_path.exists()}
        # regrid the files in the missing directories
        if dir_to_regrid_list:
            # TODO: tests on fait juste print for now
            ##########################################
            print(f'Directories to regrid: {sorted(dir_to_regrid_list)}')
            print()
            ##########################################
            sat_regrid.regrid_sat_files(path_list=dir_to_regrid_list, sat_name=sat_name,
                                        grid_res=grid_resolution, dir_list=True,
                                        grid_res_str=grid_res_str, overwrite=overwrite, old_glm_filename=False)
            ##########################################
            return
            ##########################################
        # if we still have missing pre-regrid directories --> FileNotFoundError
        if missing_raw_daily_dir_list - dir_to_regrid_list:
            # get the missing dates from the remaining missing directory paths to display them in the error message
            missing_dates = utils.get_list_of_dates_from_list_of_sat_path(
                path_list=(missing_raw_daily_dir_list - dir_to_regrid_list),
                directory=True, satellite=sat_name, regrid=False, date_str=True
            )
            raise FileNotFoundError(f'The GLM files for the following dates are missing: {sorted(missing_dates)}\nPlease download '
                                    f'them from the ICARE server and try again')
    # get list of satellite data files between start and end date
    regrid_daily_file_list = []
    fname_pattern = utils.generate_sat_hourly_filename_pattern(sat_name=sat_name, regrid=True)
    regrid_daily_file_list.extend(regrid_dir_path.glob(fname_pattern) for regrid_dir_path in regrid_daily_dir_list)
    # create a dataset merging all the regrid hourly files
    if not dry_run:
        return xr.open_mfdataset(regrid_daily_file_list)  # <?> utiliser dask ???


def get_weighted_flash_count(spec001_mr_da, flash_count_da):
    """

    :param spec001_mr_da:
    :param flash_count_da:
    :return:
    """
    return (spec001_mr_da * flash_count_da).sum(['latitude', 'longitude']) / 3600

def get_weighted_fp_sat_ds(fp_path, sat_ds, sum_height=True, load=False, chunks='auto',
                           max_chunk_size=1e8, assign_releases_position_coords=False):
    """

    @param fp_path:
    @param sat_ds:
    @param sum_height:
    @param load:
    @param chunks:
    @param max_chunk_size:
    @param assign_releases_position_coords:
    @return:
    """
    # check fp_path and get fp_da
    fp_path = utils.check_file_exists_with_suffix(fp_path, file_suffix='.nc')
    fp_da = get_fp_out_da(fpout_path=fp_path, sum_height=sum_height, load=load,
                          chunks=chunks, max_chunk_size=max_chunk_size,
                          assign_releases_position_coords=assign_releases_position_coords)
    # in case sat_ds is a path to satellite .nc file
    if isinstance(sat_ds, str) or isinstance(sat_ds, pathlib.PurePath):
        sat_ds = xr.open_dataset(sat_ds)
    elif not isinstance(sat_ds, xr.Dataset):
        raise TypeError(
            f'Invalid sat_ds ({sat_ds}). Expecting <xarray.Dataset> or path (<str> or <pathlib.Path>) to satellite data file')
    # merge fp da and sat ds
    fp_sat_ds = xr.merge([fp_da, sat_ds])
    # only keep the first 7 days after release
    fp_sat_ds = fp_sat_ds.sortby(fp_sat_ds.time, ascending=False)
    seven_days = np.timedelta64(7, 'D')
    end_date = fp_sat_ds.time.max() - seven_days
    fp_sat_ds = fp_sat_ds.where(fp_sat_ds['time'] >= end_date, drop=True)
    # get weighted flash count
    fp_sat_ds['weighted_flash_count'] = get_weighted_flash_count(spec001_mr_da=fp_sat_ds['spec001_mr'],
                                                                 flash_count_da=fp_sat_ds['flash_count'])
    return fp_sat_ds
