"""
UPGRADES: gérer pour aller chercher les données satellites de PLUSIEURS satellites et les merge dans UN seul sat_ds!
Faut que je connaisse la zone couverte par FP out et que je lance get_sat_ds sur plusieurs sat
"""
import pathlib

import numpy as np
import pandas as pd
import xarray
import xarray as xr

from common.utils import short_list_repr
import fpout

from . import utils
from .utils import constants as cts
from .utils import GLMPathParser
from . import sat_regrid
from .utils.sat_utils import generate_sat_dir_list_between_start_end_date, get_sat_files_list_between_start_end_date


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

def get_fp_out_ds_7days(fpout_path, sum_height=True, load=False, chunks='auto', max_chunk_size=1e8,
                        assign_releases_position_coords=False):
    """

    :param fpout_path:
    :param sum_height:
    :param load:
    :param chunks:
    :param max_chunk_size:
    :param assign_releases_position_coords:
    :return:
    """
    # TODO: recup un dataset sur les infos de la release ET un dataArray spec001_mr ?
    if not utils.str_to_path(fpout_path).exists():
        raise ValueError(f'fp_path {fpout_path} does NOT exist')
    fp_ds = fpout.open_fp_dataset(fpout_path, chunks=chunks, max_chunk_size=max_chunk_size,
                                  assign_releases_position_coords=assign_releases_position_coords)\
                .squeeze('nageclass')
    # rename numpoint dimension to pointspec
    fp_ds = fp_ds.rename({'numpoint': 'pointspec'})
    # get dataset containing releases info (RELxxxx variables)
    rel_ds = fp_ds.drop_vars([var for var in fp_ds.variables if not 'REL' in var])
    # fp simulation start date (ietime here because backwards)
    ietime = pd.Timestamp(f"{fp_ds.attrs['iedate']}{fp_ds.attrs['ietime']}")
    # fp release start dates (RELEND because backwards) --> get nearest hour before start
    release_start_dates = (ietime + fp_ds.RELEND).dt.ceil('h')
    # get end date (release_start_date - 7 days)
    end_dates = release_start_dates - np.timedelta64(7, 'D')
    # get spec001_mr over 7 days
    date_mask = (fp_ds.time >= end_dates) & (fp_ds.time <= release_start_dates)
    fp_da = fp_ds.where(date_mask, drop=True).spec001_mr
    # merge rel info and spec001_mr
    fp_ds = xr.merge([fp_da, rel_ds])
    # load et cie
    if sum_height:
        fp_ds = fp_ds.sum('height')
    if load:
        fp_ds.load()
    return fp_ds


#TODO ça va servir à rien je pense
def get_release_info_ds(fp_ds, chunks='auto', max_chunk_size=1e8, assign_releases_position_coords=False):
    """
    Returns dataset containing only release info (RELxxxx variables from FLEXPART output)
    :param fp_ds: <xarray.Dataset> (or <pathlib.Path> or <str> pointing to existing FLEXPART output file)
    :param chunks:
    :param max_chunk_size:
    :param assign_releases_position_coords:
    :return: <xarray.Dataset>
    """
    if not isinstance(fp_ds, xr.Dataset):
        if utils.str_to_path(fp_ds).exists():
            fp_ds = fpout.open_dataset(url=fp_ds, chunks=chunks, max_chunk_size=max_chunk_size,
                                  assign_releases_position_coords=assign_releases_position_coords)
        else:
            raise ValueError(f'fp_path {fp_ds} does NOT exist')
    return fp_ds.drop_vars([var for var in fp_ds.variables if not 'REL' in var])



# TODO: suppr dry_run une fois que les tests sont finis
# TODO: pour avoir un sat_ds avec PLUSIEURS sources sat --> sat_name = list, for loop et ensuite je merge tout ?
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
            sat_name=sat_name,
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
            sat_regrid.regrid_sat_files(path_list=list(dir_to_regrid_list), sat_name=sat_name,
                                        grid_res=grid_resolution, dir_list=True,
                                        grid_res_str=grid_res_str, overwrite=overwrite, naming_convention=None)
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
    # TODO: PROBLEM !!!! CA PREND TOUTES LES HEURES, MAIS FAUT PAS CA POUR START ET END DAY
    regrid_daily_file_list = get_sat_files_list_between_start_end_date(dir_list=sorted(regrid_daily_dir_list), start_date=start_date, end_date=end_date, sat_name=sat_name, regrid=True)
    ##########################################
    print(f'Regrid daily file list: {short_list_repr(regrid_daily_file_list)}')
    print()
    ##########################################
    # create a dataset merging all the regrid hourly files
    return xr.open_mfdataset(regrid_daily_file_list, combine_attrs='drop_conflicts')  #TODO: <?> utiliser dask: ajouter parallel=True


def get_weighted_flash_count(spec001_mr_da, flash_count_da):
    """

    :param spec001_mr_da:
    :param flash_count_da:
    :return:
    """
    return (spec001_mr_da * flash_count_da).sum(['latitude', 'longitude']) / 3600

#TODO: <!> prendre 7 days from RELSTART !! NOT min date en fait --> du coup recup closest heure AVANT relstart je suppose ?
def get_weighted_fp_sat_ds(fp_da, sat_ds, sum_height=True, load=False, chunks='auto',
                           max_chunk_size=1e8, assign_releases_position_coords=False):
    """

    @param fp_da: <xarray.DataArray> or <pathlib.Path> (or <str>) path to existing fp out netcdf file
    @param sat_ds: <xarray.Dataset>
    @param sum_height:
    @param load:
    @param chunks:
    @param max_chunk_size:
    @param assign_releases_position_coords:
    @return:
    """
    # if passed fp_out path instead of dataArray/dataset
    if not isinstance(fp_da, xr.DataArray):
        # check fp_path and get fp_da
        if utils.str_to_path(fp_da).exists():
            fp_da = get_fp_out_ds_7days(fpout_path=fp_da, sum_height=sum_height, load=load,
                                        chunks=chunks, max_chunk_size=max_chunk_size,
                                        assign_releases_position_coords=assign_releases_position_coords)
        else:
            raise TypeError(
            f'Invalid fp_da ({fp_da}). Expecting <xarray.Dataset> or path (<str> or <pathlib.Path>) to existing FLEXPART output file')
    if not isinstance(sat_ds, xr.Dataset):
        raise TypeError(
            f'Invalid sat_ds ({sat_ds}). Expecting <xarray.Dataset> object')
    # merge fp da and sat ds
    fp_sat_ds = xr.merge([fp_da, sat_ds])
    # get weighted flash count
    fp_sat_ds['weighted_flash_count'] = get_weighted_flash_count(spec001_mr_da=fp_sat_ds['spec001_mr'],
                                                                 flash_count_da=fp_sat_ds['flash_count'])
    return fp_sat_ds

# TODO: fp_sat_comp function qui prend juste fp_path (list or not) as input and computes the weighted flash_count (and fichier intermédiaure etc., à voir)
# TODO: fp_sat_comp doit savoir TOUT SEUL quelles données sat on va chercher en fonction de ce qui est dispo et tout
def fpout_sat_comparison(fp_path, sat_name, file_list=False, sum_height=True, load=False, chunks='auto',
                         max_chunk_size=1e8, assign_releases_position_coords=False, grid_resolution=cts.GRID_RESOLUTION,
                         grid_res_str=cts.GRID_RESOLUTION_STR):
    if not file_list:
        fp_path = [fp_path]
    for fp_file in fp_path:
        # step1: verif si fp_path file exists
        if not utils.str_to_path(fp_file).exists():
            raise FileNotFoundError(f'Expecting existing fp out file! {fp_file} does NOT exist')
        # step2: recup fp_ds sur 7 JOURS avec les 7j pour chaque releases, PAS depuis début fichier
        fp_ds = get_fp_out_ds_7days(fpout_path=fp_file, sum_height=sum_height, load=load, chunks=chunks, max_chunk_size=max_chunk_size,
                                    assign_releases_position_coords=assign_releases_position_coords)
        # TODO: step3: recup liste des sat_name des zones couvertes
        # step4: recup start + end date
        start_date, end_date = pd.Timestamp(fp_ds.time.min().values), pd.Timestamp(fp_ds.time.max())
        #   step4: get sat_ds
        sat_ds = get_satellite_ds(start_date=start_date, end_date=end_date, sat_name=sat_name, grid_resolution=grid_resolution,
                                  grid_res_str=grid_res_str)
        # setp5: get weighted fp_sat_ds
        weighted_fp_sat_ds = get_weighted_fp_sat_ds(fp_da=fp_ds.spec001_mr, sat_ds=sat_ds)
        # step6: générer le fichier intermédiaire
        # --> je crois si je fais juste la somme de flash_count_weighted sur toutes les dims sauf pointspec c'est ok non ?
        # TODO: tester fpout_sat_comparison pour voir si get weighted OK
