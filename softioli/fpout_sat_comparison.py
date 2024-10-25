"""
UPGRADES: gérer pour aller chercher les données satellites de PLUSIEURS satellites et les merge dans UN seul sat_ds!
Faut que je connaisse la zone couverte par FP out et que je lance get_sat_ds sur plusieurs sat
"""
import argparse
import numpy as np
import pandas as pd
import pathlib
import xarray as xr

from common.utils import short_list_repr
from fpout import open_fp_dataset
from fpsim import check_fp_status

import utils
from utils import constants as cts
from utils import GLMPathParser
import sat_regrid
from utils.sat_utils import generate_sat_dir_list_between_start_end_date, get_sat_files_list_between_start_end_date
from utils.fp_utils import get_fpout_nc_file_path_from_fp_dir


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
    if not pathlib.Path(fpout_path).exists():
        raise ValueError(f'fp_path {fpout_path} does NOT exist')
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
    # get "end" date (release_start_date - 7 days)
    end_dates = release_start_dates - np.timedelta64(7, 'D')
    # get spec001_mr over 7 days
    date_mask = ((fp_ds.time >= end_dates) & (fp_ds.time <= release_start_dates)).compute()
    fp_da = fp_ds.where(date_mask, drop=True).spec001_mr
    # merge rel info and spec001_mr
    fp_ds = xr.merge([fp_da, rel_ds])
    # load et cie
    if sum_height:
        fp_ds = fp_ds.sum('height')
    if load:
        fp_ds.load()
    return fp_ds

# TODO: suppr dry_run une fois que les tests sont finis
# TODO: pour avoir un sat_ds avec PLUSIEURS sources sat --> sat_name = list, for loop et ensuite je merge tout ?
def get_satellite_ds(start_date, end_date, sat_name, grid_resolution=cts.GRID_RESOLUTION,
                     grid_res_str=cts.GRID_RESOLUTION_STR, overwrite=False, dry_run=False, print_debug=False):
    """
    Returns dataset with regridded satellite data between start and end date
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
        if print_debug:
            print()
            print(f"regrid_daily_dir_list : {regrid_daily_dir_list}")
            print()
            print(f'missing_raw_daily_dir_list : {missing_raw_daily_dir_list}')
            print()
        # directories to regrid (pre-regrid directory exist but NOT regrid directory)
        dir_to_regrid_list = {d_path for d_path in missing_raw_daily_dir_list if d_path.exists()}
        # regrid the files in the missing directories
        if dir_to_regrid_list:
            if print_debug:
                print(f'Directories to regrid: {sorted(dir_to_regrid_list)}')
                print()
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
    regrid_daily_file_list = get_sat_files_list_between_start_end_date(dir_list=sorted(regrid_daily_dir_list),
                                                                       start_date=start_date, end_date=end_date,
                                                                       sat_name=sat_name, regrid=True)
    if print_debug:
        print(f'Regrid daily file list: {short_list_repr(regrid_daily_file_list)}')
        print()
    # create a dataset merging all the regrid hourly files
    sat_ds = xr.open_mfdataset(regrid_daily_file_list, combine_attrs='drop_conflicts') #TODO: <?> utiliser dask: ajouter parallel=True
    return sat_ds


def get_weighted_flash_count(spec001_mr_da, flash_count_da):
    """

    :param spec001_mr_da:
    :param flash_count_da:
    :return:
    """
    return (spec001_mr_da * flash_count_da).sum(['latitude', 'longitude']) / 3600


def get_weighted_fp_sat_ds(fp_ds, sat_ds, sum_height=True, load=False, chunks='auto',
                           max_chunk_size=1e8, assign_releases_position_coords=False):
    """

    @param fp_ds: <xarray.Dataset> or <pathlib.Path> (or <str>) path to existing fp out netcdf file
    @param sat_ds: <xarray.Dataset>
    @param sum_height:
    @param load:
    @param chunks:
    @param max_chunk_size:
    @param assign_releases_position_coords:
    @return:
    """
    # if passed fp_out path instead of dataArray/dataset
    if not (isinstance(fp_ds, xr.DataArray) or isinstance(fp_ds, xr.Dataset)):
        # check fp_path and get fp_da
        if pathlib.Path(fp_ds).exists():
            fp_ds = get_fp_out_ds_7days(fpout_path=fp_ds, sum_height=sum_height, load=load,
                                        chunks=chunks, max_chunk_size=max_chunk_size,
                                        assign_releases_position_coords=assign_releases_position_coords)
        else:
            raise TypeError(
                f'Invalid fp_da ({fp_ds}). Expecting <xarray.Dataset> or path (<str> or <pathlib.Path>) to existing FLEXPART output file')
    if not isinstance(sat_ds, xr.Dataset):
        raise TypeError(
            f'Invalid sat_ds ({sat_ds}). Expecting <xarray.Dataset> object')
    # merge fp da and sat ds
    fp_sat_ds = xr.merge([fp_ds, sat_ds], combine_attrs='drop_conflicts')
    # get weighted flash count
    fp_sat_ds['weighted_flash_count'] = get_weighted_flash_count(spec001_mr_da=fp_sat_ds['spec001_mr'],
                                                                 flash_count_da=fp_sat_ds['flash_count'])
    return fp_sat_ds


# TODO: fp_sat_comp doit savoir TOUT SEUL quelles données sat on va chercher en fonction de ce qui est dispo et tout (? pourquoi j'ai dit ça?)
def fpout_sat_comparison(fp_path, sat_name, flights_id_list, file_list=False, sum_height=True, load=False, chunks='auto',
                         max_chunk_size=1e8, assign_releases_position_coords=False, grid_resolution=cts.GRID_RESOLUTION,
                         grid_res_str=cts.GRID_RESOLUTION_STR, save_weighted_ds=False, flights_output_dirpath=None, weighted_ds_filename_suffix=''):
    if not file_list and isinstance(fp_path, str) or isinstance(fp_path, pathlib.Path):
        fp_path = [fp_path]
    for index, fp_file in enumerate(fp_path):
        # fp_file expected to be in <flight_output_dir>/flexpart/output/... hence the <fp_path>.parent.parent to get to the flexpart directory
        if check_fp_status(pathlib.Path(fp_file).parent.parent):
            # step2: recup fp_ds sur 7 JOURS avec les 7j pour chaque release, PAS depuis début fichier
            with get_fp_out_ds_7days(fpout_path=fp_file, sum_height=sum_height, load=load, chunks=chunks,
                                     max_chunk_size=max_chunk_size, assign_releases_position_coords=assign_releases_position_coords)\
                    as fp_ds:
                if args.print_debug:
                    print('##################################################')
                    print(f'Flight {flights_id_list[index]}')
                    print(f'Flexpart output: {fp_file}')
                    print('##################################################')
                # TODO: step3: recup liste des sat_name des zones couvertes
                start_date, end_date = pd.Timestamp(fp_ds.time.min().values), pd.Timestamp(fp_ds.time.max().values)
                #   step4: get sat_ds
                try:
                    sat_ds = get_satellite_ds(start_date=start_date, end_date=end_date, sat_name=sat_name, grid_resolution=grid_resolution,
                                              grid_res_str=grid_res_str)
                except FileNotFoundError as e:
                    print(e)
                    continue
                # setp5: get weighted fp_sat_ds
                weighted_fp_sat_ds = get_weighted_fp_sat_ds(fp_ds=fp_ds, sat_ds=sat_ds)
                # TODO: step6: ajouter données ABI à weighted_fp_sat_ds
                # TODO: blablablabla
                if save_weighted_ds:
                    if flights_output_dirpath is None:
                        Warning(f'Saving weighted ds to current directory ({pathlib.Path.cwd()})')
                        weighted_fp_sat_ds.to_netcdf(f'weighted_fp_sat_ds{weighted_ds_filename_suffix}.nc')
                    else:
                        weighted_ds_dirpath = pathlib.Path(f'{flights_output_dirpath}/{flights_id_list[index]}/flexpart_lightning_comparison')
                        weighted_ds_dirpath.mkdir(exist_ok=True) # create lightning comparison dirpath if it doesn't exist yet
                        weighted_fp_sat_ds.to_netcdf(f'{weighted_ds_dirpath}/weighted_fp_sat_ds{weighted_ds_filename_suffix}.nc')
                        print(f'Saved {weighted_ds_dirpath}/weighted_fp_sat_ds{weighted_ds_filename_suffix}.nc file')

                # TODO: step7: générer le fichier intermédiaire <?>
                # TODO: pour chaque RELSTART donner weighted_fp_sat_ds['weighted_flash_count'].sum('time') <?>
        else:
            raise FileNotFoundError(f'Expecting existing completed fp out file! {fp_file} does NOT exist and/or flexpart simulation has NOT been successful')




if __name__ == '__main__':
    parser = argparse.ArgumentParser()

    # output directories
    dir_group = parser.add_argument_group('Directories')
    dir_group.add_argument('-fo', '--flights-output-dir', required=True, type=pathlib.Path, help='Path to output directory (directory containing all flight output directories)')
    dir_group.add_argument('--flight-dirname-suffix', default='',
                              help='suffix to add to flight output directory name')
    dir_group.add_argument('-o', '--fp-output-dirname', default='flexpart', help='Name of the directory where the flexpart output will be stored (default="flexpart")')

    # flight list
    flight_group = parser.add_argument_group('Flights')
    flight_group.add_argument('--flight-list', action='store_true',
                              help='Indicates if a list of flight ids/names will be passed')
    flight_group.add_argument('--flight-range', action='store_true',
                              help='Indicates if start and end flight ids/names will be passed')
    # range
    flight_group.add_argument('-s', '--start-id',
                              help='Start flight name/id (in case we only want to retrieve NOx flights between two flight ids)')
    flight_group.add_argument('-e', '--end-id',
                              help='End flight name/id (in case we only want to retrieve NOx flights between two flight ids)')
    # list
    flight_group.add_argument('--flight-id-list', nargs='+', default=[],
                              help='List of flight ids/names (default = None)')

    # satellite
    sat_group = parser.add_argument_group('Satellite parameters')
    sat_group.add_argument('--sat-name', default=cts.GOES_SATELLITE_GLM, help=f'Satellite name (default={cts.GOES_SATELLITE_GLM})')
    sat_group.add_argument('--grid-res', default=cts.GRID_RESOLUTION, help=f'Satellite grid resolution (default={cts.GRID_RESOLUTION})')
    sat_group.add_argument('--grid-res-str', default=cts.GRID_RESOLUTION_STR,
                           help=f'Satellite grid resolution string, format="<res>deg" (default={cts.GRID_RESOLUTION_STR})')

    # flexpart output parameters
    fp_group = parser.add_argument_group('Flexpart output parameters')
    fp_group.add_argument('--dont-sum-height', action='store_true', help='Indicates if flexpart output should NOT be summed over altitude (by default it is because satellite data does not have altitude information)')
    fp_group.add_argument('--load-fpout', action='store_true', help='load fp_out dataArray into memory (default=False)')

    # weighted ds
    weighted_ds_group = parser.add_argument_group('Weighted ds parameters')
    weighted_ds_group.add_argument('--save-weighted-ds', action='store_true', help='Indicates if weighted ds should be saved')
    weighted_ds_group.add_argument('--ds-fname-suffix', default='', help='Suffix to add to the weighted ds filename. The dataset will be stored in the flexpart_lightning_comparison directory in the flight output directory (default suffix="")')


    # other
    parser.add_argument('--dry-run', action='store_true', help='dry run (fp_out and glm_out NOT loaded into memory and weighted flash count NOT calculated)')
    parser.add_argument('-d', '--print-debug', action='store_true', help='print debug (default=False)')

    args = parser.parse_args()
    print(args)

    if args.flight_range: # get list of flights containing potential plumes (flights with plume info csv file)
        flight_range_list = utils.get_list_of_paths_between_two_values(args.flights_output_dir,
                                                                       start_name=args.start_id, end_name=args.end_id,
                                                                       glob_pattern=f'{cts.YYYY_pattern}{cts.MM_pattern}{cts.DD_pattern}*',
                                                                       subdir_glob_pattern='*.csv')
        # only keep flight names from list of flight paths
        flight_range_list = [flight_path.name for flight_path in flight_range_list]
        args.flight_id_list = list(set(args.flight_id_list + flight_range_list))

    # get list
    fp_path_list = [
        get_fpout_nc_file_path_from_fp_dir(fp_dirpath=f'{args.flights_output_dir}/{flight_id}/{args.fp_output_dirname}')
        for flight_id in args.flight_id_list
    ]

    print(sorted(fp_path_list))
    print()
    print(sorted(args.flight_id_list))

    fpout_sat_comparison(fp_path=sorted(fp_path_list), flights_id_list=sorted(args.flight_id_list), sat_name=args.sat_name, file_list=True,
                         sum_height=(not args.dont_sum_height), load=args.load_fpout,
                         chunks='auto', max_chunk_size=1e8, assign_releases_position_coords=False,
                         grid_resolution=args.grid_res, grid_res_str=args.grid_res_str,
                         save_weighted_ds=args.save_weighted_ds, flights_output_dirpath=args.flights_output_dir,
                         weighted_ds_filename_suffix=args.ds_fname_suffix)

