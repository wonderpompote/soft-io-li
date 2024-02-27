"""
UPGRADES: gérer pour aller chercher les données satellites de PLUSIEURS satellites et les merge dans UN seul sat_ds!
Faut que je connaisse la zone couverte par FP out et que je lance get_sat_ds sur plusieurs sat
"""
import numpy as np
import xarray as xr

from common.utils import short_list_repr
import fpout

from . import utils
from .utils import constants as cts
from .utils import GLMPathParser
from . import sat_regrid
from .utils.sat_utils import generate_sat_dir_list_between_start_end_date


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
    regrid_daily_file_list = []
    regrid_daily_dir_list = sorted(regrid_daily_dir_list)
    fname_pattern = utils.generate_sat_hourly_filename_pattern(sat_name=sat_name, regrid=True)
    start_filename_pattern = utils.generate_sat_hourly_filename_pattern(sat_name=sat_name, regrid=True, year=start_date.year)
    for regrid_dir_path in regrid_daily_dir_list:
        if regrid_dir_path == regrid_daily_dir_list[0]:
            regrid_daily_file_list.extend(regrid_dir_path.glob(fname_pattern))
        regrid_daily_file_list.extend(regrid_dir_path.glob(fname_pattern))
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
        if utils.check_file_exists_with_suffix(fp_da, file_suffix='.nc'):
            fp_da = get_fp_out_da(fpout_path=fp_da, sum_height=sum_height, load=load,
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
    # only keep the first 7 days after release
    fp_sat_ds = fp_sat_ds.sortby(fp_sat_ds.time, ascending=False)
    seven_days = np.timedelta64(7, 'D')
    end_date = fp_sat_ds.time.max() - seven_days
    fp_sat_ds = fp_sat_ds.where(fp_sat_ds['time'] >= end_date, drop=True)
    # get weighted flash count
    fp_sat_ds['weighted_flash_count'] = get_weighted_flash_count(spec001_mr_da=fp_sat_ds['spec001_mr'],
                                                                 flash_count_da=fp_sat_ds['flash_count'])
    return fp_sat_ds

# TODO: fp_sat_comp function qui prend juste fp_path (list or not) as input and computes the weighted flash_count (and fichier intermédiaure etc., à voir)
# TODO: fp_sat_comp doit savoir TOUT SEUL quelles données sat on va chercher en fonction de ce qui est dispo et tout
def fpout_sat_comparison(fp_path, file_list=False, sum_height=True, load=False, chunks='auto', max_chunk_size=1e8, assign_releases_position_coords=False):
    # step1: verif si fp_path file exists
    # step2: recup start + end date
    # step2bis: recup liste des sat_name des zones couvertes
    # step3: get sat_ds
    # setp4: get weighted fp_sat_ds
    # step5: générer le fichier intermédiaire
    pass
