"""
UPGRADES: gérer pour aller chercher les données satellites de PLUSIEURS satellites et les merge dans UN seul sat_ds!
Faut que je connaisse la zone couverte par FP out et que je lance get_sat_ds sur plusieurs sat
"""
import argparse
import pandas as pd
import pathlib
import warnings
import xarray as xr

from common.utils import short_list_repr, list_from_file
from fpsim import check_fp_status

import utils
from utils import constants as cts
import sat_regrid
from utils.sat_utils import generate_sat_dir_path, get_list_of_dates_from_list_of_sat_path, \
    generate_sat_dir_list_between_start_end_date, get_data_files_list_between_start_end_date, get_PathParser, merge_GOES_sat_data_with_overlap
from utils.fp_utils import get_fpout_nc_file_path_from_fp_dir, get_fp_out_ds_xdays





# TODO: suppr dry_run une fois que les tests sont finis
# TODO: pour avoir un sat_ds avec PLUSIEURS sources sat --> sat_name = list, for loop et ensuite je merge tout ?
def get_satellite_ds(start_date, end_date, sat_name, grid_resolution=cts.GRID_RESOLUTION, rm_pre_regrid_file=False,
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
    merge_sats_for_same_hour = False
    PathParser = get_PathParser(sat_name)
    if sat_name == cts.GOES_SATELLITE_GLM:
        # GOES EAST AND WEST GLM DATA AVAILABLE
        if start_date >= cts.MIN_GOES_EAST_WEST_DATE_GLM:
            merge_sats_for_same_hour = True
    elif sat_name == cts.GOES_SATELLITE_ABI:
        # GOES EAST AND WEST ABI DATA AVAILABLE
        if start_date >= cts.MIN_GOES_EAST_WEST_DATE_ABI:
            merge_sats_for_same_hour = True
    elif sat_name == cts.MTG_LI:
        pass
    else:
        raise ValueError(f'{sat_name} {cts.SAT_VALUE_ERROR}')

    # get list of pre-regrid daily directories corresponding to each missing regrid sat dir
    missing_raw_daily_dir_list = {
        generate_sat_dir_path(
            date=PathParser(regrid_dir_path, directory=True, regrid=True) \
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
            print(f"regrid_daily_dir_list : {sorted(regrid_daily_dir_list)}")
            print()
            print(f'missing_raw_daily_dir_list : {sorted(missing_raw_daily_dir_list)}')
            print()
        # directories to regrid (pre-regrid directory exist but NOT regrid directory)
        dir_to_regrid_list = {d_path for d_path in missing_raw_daily_dir_list if d_path.exists()}
        # regrid the files in the missing directories
        if dir_to_regrid_list:
            if print_debug:
                print(f'Directories to regrid: {sorted(dir_to_regrid_list)}')
                print()
            if not dry_run:
                sat_regrid.regrid_sat_files(path_list=list(dir_to_regrid_list), sat_name=sat_name,
                                            grid_res=grid_resolution, dir_list=True, print_debug=print_debug,
                                            grid_res_str=grid_res_str, overwrite=overwrite, 
                                            rm_pre_regrid_file=rm_pre_regrid_file)
        # if we still have missing pre-regrid directories --> FileNotFoundError
        if missing_raw_daily_dir_list - dir_to_regrid_list:
            # get the missing dates from the remaining missing directory paths to display them in the error message
            missing_dates = get_list_of_dates_from_list_of_sat_path(
                path_list=(missing_raw_daily_dir_list - dir_to_regrid_list),
                directory=True, sat_name=sat_name, regrid=False, date_str=True
            )
            raise FileNotFoundError(
                f'The {sat_name} files for the following dates are missing, please download them from the ICARE server and try again: \n{sorted(missing_dates)}')
    # get list of satellite data files between start and end date
    regrid_daily_file_list = get_data_files_list_between_start_end_date(dir_list=sorted(regrid_daily_dir_list),
                                                                        start_date=start_date, end_date=end_date,
                                                                        sat_name=sat_name, regrid=True)
    if print_debug:
        print(f'Regrid daily file list: {short_list_repr(sorted(regrid_daily_file_list))}')
        print()

    if not dry_run:
        # if several sat data files for the same hour --> preprocess them before merging
        if merge_sats_for_same_hour:
            sat_ds = merge_GOES_sat_data_with_overlap(regrid_daily_file_list=regrid_daily_file_list,
                                                      sat_name=sat_name, PathParser=PathParser, print_debug=print_debug)
        else:
            # create a dataset merging all the regrid hourly files
            # engine h5netcdf because default engine does not work with parallel=True
            sat_ds = xr.open_mfdataset(regrid_daily_file_list, parallel=True, engine='h5netcdf',
                                       combine='nested', concat_dim='time', combine_attrs='drop_conflicts')
            # if lightning ds only keep flash_count to prevent computations from being too long (hist take up too much place)
            if sat_name == cts.GOES_SATELLITE_GLM or sat_name == cts.MTG_LI:
                sat_ds = sat_ds[['flash_count']]

        return sat_ds

    else:
        return None


def get_weighted_flash_count(spec001_mr_da, flash_count_da):
    """

    :param spec001_mr_da:
    :param flash_count_da:
    :return:
    """
    return (spec001_mr_da * flash_count_da).sum(['latitude', 'longitude']) / 3600


def get_weighted_fp_sat_ds(fp_ds, lightning_sat_ds, chunks='auto',
                           max_chunk_size=1e8, assign_releases_position_coords=False, no_glm=False):
    """

    @param fp_ds: <xarray.Dataset> or <pathlib.Path> (or <str>) path to existing fp out netcdf file
    @param lightning_sat_ds: <xarray.Dataset>
    @param chunks:
    @param max_chunk_size:
    @param assign_releases_position_coords:
    @param no_glm: <bool> indicates if there is no glm data for this flight (flight took place before 03/2018)
    @return:
    """
    # if passed fp_out path instead of dataArray/dataset
    if not (isinstance(fp_ds, xr.DataArray) or isinstance(fp_ds, xr.Dataset)):
        # check fp_path and get fp_da
        if pathlib.Path(fp_ds).exists():
            fp_ds = get_fp_out_ds_xdays(fpout_path=fp_ds, sum_height=True,
                                        chunks=chunks, max_chunk_size=max_chunk_size,
                                        assign_releases_position_coords=assign_releases_position_coords)
        else:
            raise TypeError(
                f'Invalid fp_da ({fp_ds}). Expecting <xarray.Dataset> or path (<str> or <pathlib.Path>) to existing FLEXPART output file')
    if not isinstance(lightning_sat_ds, xr.Dataset):
        if no_glm: # if lightning_sat is NOT a dataset because there was no GLM data at this time --> return fp_ds
            return fp_ds
        raise TypeError(
            f'Invalid sat_ds ({lightning_sat_ds}). Expecting <xarray.Dataset> object')
    # merge fp da and sat ds
    fp_sat_ds = xr.merge([fp_ds, lightning_sat_ds], combine_attrs='drop_conflicts')
    # get weighted flash count
    fp_sat_ds['weighted_flash_count'] = get_weighted_flash_count(spec001_mr_da=fp_sat_ds['spec001_mr'],
                                                                 flash_count_da=fp_sat_ds['flash_count'])
    return fp_sat_ds



def fpout_sat_comparison(fp_path, lightning_sat_name, bTemp_sat_name, flights_id_list, file_list=False,
                         no_cloud_sat=False, chunks='auto', print_debug=False, dry_run=False, overwrite_weighted_ds=False,
                         max_chunk_size=1e8, assign_releases_position_coords=False, grid_resolution=cts.GRID_RESOLUTION,
                         grid_res_str=cts.GRID_RESOLUTION_STR, softioli_output_dirpath=None, result_dirname='flexpart_lightning_comparison',
                         result_ds_name='', overwrite_sat_files=False, rm_pre_regrid_abi_file=False,
                         rm_pre_regrid_li_file=False):
    if not file_list and isinstance(fp_path, str) or isinstance(fp_path, pathlib.Path):
        fp_path = [fp_path]
    missing_dates_list = {'lightning': [], 'cloud': []}
    for index, fp_file in enumerate(fp_path):
        # fp_file expected to be in <flight_output_dir>/flexpart/output/... hence the <fp_path>.parent.parent to get to the flexpart directory
        if check_fp_status(pathlib.Path(fp_file).parent.parent):
            # step2: recup fp_ds sur 7 JOURS avec les 7j pour chaque release, PAS depuis début fichier
            fp_ds = get_fp_out_ds_xdays(fpout_path=fp_file, sum_height=True, chunks=chunks,
                                        max_chunk_size=max_chunk_size,
                                        assign_releases_position_coords=assign_releases_position_coords)
            if print_debug:
                print('\n\n##################################################')
                print(f'Flight {flights_id_list[index]}')
                print(f'Flexpart output: {fp_file}')
                print('##################################################')
            start_date, end_date = pd.Timestamp(fp_ds.time.min().values), pd.Timestamp(fp_ds.time.max().values)
            #   step4: get sat_ds (no GLM data before 2018-03-14)
            if start_date < pd.Timestamp('2018-03-14') and lightning_sat_name == cts.GOES_SATELLITE_GLM:
                no_glm = True
                lightning_sat_ds_ok = False
                print(f'<!> No GLM data available before 2018-03-14 <!>')
            else:
                no_glm = False
                try:
                    lightning_sat_ds = get_satellite_ds(start_date=start_date, end_date=end_date,
                                                        sat_name=lightning_sat_name,
                                                        grid_resolution=grid_resolution, print_debug=print_debug,
                                                        grid_res_str=grid_res_str, dry_run=dry_run,
                                                        overwrite=overwrite_sat_files,
                                                        rm_pre_regrid_file=rm_pre_regrid_li_file)
                    if print_debug:
                        print('Lightning sat OK')
                        print(lightning_sat_ds)
                    lightning_sat_ds_ok = True
                except FileNotFoundError as e:
                    print(f'<!> {e}')
                    lightning_sat_ds_ok = False
                    for m_date in eval(str(e).split('\n')[1]):
                        if m_date not in missing_dates_list['lightning']:
                            missing_dates_list['lightning'].append(m_date)

            # step 5: get brightness temperature ds
            if not no_cloud_sat:
                try:
                    bTemp_sat_ds = get_satellite_ds(start_date=start_date, end_date=end_date, sat_name=bTemp_sat_name,
                                                    grid_resolution=grid_resolution, print_debug=print_debug,
                                                    grid_res_str=grid_res_str, dry_run=dry_run,
                                                    overwrite=overwrite_sat_files,
                                                    rm_pre_regrid_file=rm_pre_regrid_abi_file)
                    if print_debug:
                            print('Cloud sat OK')
                            print(bTemp_sat_ds)
                    bTemp_sat_ds_ok = True
                except FileNotFoundError as e:
                    print(f'<!> {e}')
                    bTemp_sat_ds_ok = False
                    for m_date in eval(str(e).split('\n')[1]):
                        if m_date not in missing_dates_list['cloud']:
                            missing_dates_list['cloud'].append(m_date)
                    continue
            else:
                bTemp_sat_ds_ok = False
            # setp6: get weighted fp_sat_ds
            if (not dry_run and lightning_sat_ds_ok and bTemp_sat_ds_ok) or (not dry_run and no_glm and bTemp_sat_ds_ok) or (not dry_run and lightning_sat_ds_ok and no_cloud_sat):
                if no_glm:
                    weighted_fp_sat_ds = get_weighted_fp_sat_ds(fp_ds=fp_ds, lightning_sat_ds=None, no_glm=True)
                else:
                    weighted_fp_sat_ds = get_weighted_fp_sat_ds(fp_ds=fp_ds, lightning_sat_ds=lightning_sat_ds)
                if not no_cloud_sat:
                    weighted_fp_sat_ds = weighted_fp_sat_ds.merge(bTemp_sat_ds)
                if print_debug:
                    print("Cloud temperature data added to weighted ds")
                    print()

                final_result_ds_name = f'{flights_id_list[index]}_{result_ds_name}'

                if softioli_output_dirpath is None:
                    warnings.warn(f'Saving weighted ds to current directory ({pathlib.Path.cwd()})')
                    weighted_fp_sat_ds.to_netcdf(f'{final_result_ds_name}.nc')
                else:
                    weighted_ds_dirpath = pathlib.Path(
                        f'{softioli_output_dirpath}/{flights_id_list[index]}/{final_result_ds_name}')
                    weighted_ds_filepath = pathlib.Path(
                        f'{weighted_ds_dirpath}/{final_result_ds_name}.nc')
                    if not weighted_ds_filepath.exists() or overwrite_weighted_ds:
                        # create lightning comparison dirpath if it doesn't exist yet
                        weighted_ds_dirpath.mkdir(exist_ok=True, parents=True)
                        weighted_fp_sat_ds.to_netcdf(path=weighted_ds_filepath, mode='w')
                        print(
                            f'Saved {weighted_ds_dirpath}/{final_result_ds_name}.nc file')
                    else:
                        print(
                            f'{weighted_ds_dirpath}/{final_result_ds_name}.nc already exists! Use --overwrite option if you want to overwrite the existing file')

            # TODO: step7: générer le fichier intermédiaire <?>
            # TODO: pour chaque RELSTART donner weighted_fp_sat_ds['weighted_flash_count'].sum('time') <?>
        else:
            raise FileNotFoundError(
                f'Expecting existing completed fp out file! {fp_file} does NOT exist and/or flexpart simulation has NOT been successful')
    missing_dates_list['lightning'] = sorted(missing_dates_list['lightning'])
    missing_dates_list['cloud'] = sorted(missing_dates_list['cloud'])
    return missing_dates_list


if __name__ == '__main__':
    parser = argparse.ArgumentParser()

    # REQUIRED: flight id list
    flight_group = parser.add_mutually_exclusive_group(required=True)
    # all flights in output dir
    flight_group.add_argument('-a', '--all-flights', action='store_true',
                              help='Indicates if all flights in output dir should be processed')
    # flight range
    flight_group.add_argument('--start-end-flight-id', nargs=2,
                              help='Start and end dates (in case we only want to retrieve NOx flights between two specific dates)')
    # list of flights in a txt file
    flight_group.add_argument('--flight-id-list',
                              help='Path to a txt file containing a list of flight ids (1 flight id/line)')
    # list
    flight_group.add_argument('--flight-id', nargs='+', default=[],
                              help='List of flight ids/names given directly, not via txt file')

    # output directories
    output_group = parser.add_argument_group('Output')
    output_group.add_argument('--softioli-output-dir', required=True, type=pathlib.Path,
                              help='Path to output directory (directory containing all flight output directories)')
    output_group.add_argument('--fp-output-dirname', default='flexpart',
                              help='Name of the directory where the flexpart output is stored (default="flexpart")')
    output_group.add_argument('--result-dirname', default='flexpart_lightning_comparison',
                              help='result directory name (default=flexpart_lightning_comparison)')
    output_group.add_argument('--result-ds-name', default='weighted_flash_ds.nc',
                                   help='Name of the resulting netcdf file. It will be stored in the flexpart_lightning_comparison directory in the flight output directory (default="<flight_name>_weighted_flash_ds.nc")')
    output_group.add_argument('--overwrite-weighted-ds', action='store_true',
                                   help='Indicates if weighted ds should be overwritten if it already exists')

    # satellite
    sat_group = parser.add_argument_group('Satellite parameters')
    sat_group.add_argument('--lightning-sat-name', default=cts.GOES_SATELLITE_GLM,
                           help=f'Lightning satellite name (default={cts.GOES_SATELLITE_GLM})')
    sat_group.add_argument('--no-cloud-sat', action='store_true', help=f'Indicates if cloud sat data should be ignored')
    sat_group.add_argument('--cloud-sat-name', default=cts.GOES_SATELLITE_ABI,
                           help=f'Cloud brightness temperature satellite name (default={cts.GOES_SATELLITE_ABI})')
    sat_group.add_argument('--grid-res', default=cts.GRID_RESOLUTION,
                           help=f'Satellite grid resolution (default={cts.GRID_RESOLUTION})')
    sat_group.add_argument('--grid-res-str', default=cts.GRID_RESOLUTION_STR,
                           help=f'Satellite grid resolution string, format="<res>deg" (default={cts.GRID_RESOLUTION_STR})')
    sat_group.add_argument('--overwrite-sat-files', action='store_true',
                        help='Indicates if existing pre_regrid and regrid satellite files should be overwritten')
    sat_group.add_argument('--rm-pre-regrid-abi-file', action='store_true',
                        help='Indicates if pre_regrid hourly ABI file should be removed once the corresponding regrid file has been generated (to free up space)')
    sat_group.add_argument('--rm-pre-regrid-glm-file', action='store_true',
                        help='Indicates if pre_regrid hourly GLM file should be removed once the corresponding regrid file has been generated (to free up space)')

    # other
    parser.add_argument('--dry-run', action='store_true',
                        help='dry run (fp_out and glm_out NOT loaded into memory and weighted flash count NOT calculated)')
    parser.add_argument('-d', '--print-debug', action='store_true', help='print debug (default=False)')


    args = parser.parse_args()
    print(args)

    # retrieve list of flight ids
    flight_id_list = []
    if args.all_flights:  # get list of flights containing potential plumes (flights with plume info csv file)
        all_flights_list = utils.get_list_of_paths_between_two_values(args.softioli_output_dir,
                                                                      start_name=None, end_name=None,
                                                                      glob_pattern=f'{cts.YYYY_pattern}{cts.MM_pattern}{cts.DD_pattern}*',
                                                                      subdir_glob_pattern='*.csv')

        # only keep flight names from list of flight paths (without duplicates)
        flight_id_list = sorted([flight_path.name for flight_path in all_flights_list])
    elif args.start_end_flight_id:  # get list of flights containing potential plumes (flights with plume info csv file)
        args.start_end_flight_id = sorted(args.start_end_flight_id)
        flight_range_list = utils.get_list_of_paths_between_two_values(args.softioli_output_dir,
                                                                       start_name=args.start_end_flight_id[0],
                                                                       end_name=args.start_end_flight_id[1],
                                                                       glob_pattern=f'{cts.YYYY_pattern}{cts.MM_pattern}{cts.DD_pattern}*',
                                                                       subdir_glob_pattern='*.csv')
        # only keep flight names from list of flight paths (without duplicates)
        flight_id_list = sorted([flight_path.name for flight_path in flight_range_list])
    elif args.flight_id_list: # txt file containing flight ids
        flight_id_list = list_from_file(args.flight_id_list, header=0, ignore_blank_lines=True)
    else: # flight ids passed directly in command line
        flight_id_list = args.flight_id

    # get flexpart output path list
    fp_path_list = []
    indices_flight_id_fp_not_ok_or_missing = []
    flight_id_list_fp_not_success = []
    flight_id_list_fp_output_missing = []
    for index, flight_id in enumerate(flight_id_list):
        if flight_id is not None and (args.softioli_output_dir / flight_id).exists():
            fp_dirpath = f'{args.softioli_output_dir}/{flight_id}/{args.fp_output_dirname}'
            try:
                fpout_nc_filepath = get_fpout_nc_file_path_from_fp_dir(fp_dirpath=fp_dirpath)
                fp_path_list.append(fpout_nc_filepath)
            except FileNotFoundError as e:
                print(f'<!> Skipping flight {flight_id}: {e}')
                indices_flight_id_fp_not_ok_or_missing.append(index)
                flight_id_list_fp_output_missing.append(flight_id)
            except RuntimeError as e:
                print(f'<!> Skipping flight {flight_id}: {e}')
                indices_flight_id_fp_not_ok_or_missing.append(index)
                flight_id_list_fp_not_success.append(flight_id)

    # remove not ok flight ids from main flight id list
    for id in sorted(indices_flight_id_fp_not_ok_or_missing, reverse=True):
        del flight_id_list[id]

    print(short_list_repr(sorted(fp_path_list)))
    print()
    print(sorted(flight_id_list))
    print()

    # remove ".nc" if given in result_ds_name
    if args.result_ds_name[-3:] == '.nc':
        args.result_ds_name = args.result_ds_name[:-3]

    missing_dates = fpout_sat_comparison(fp_path=sorted(fp_path_list), flights_id_list=sorted(flight_id_list),
                                         lightning_sat_name=args.lightning_sat_name, dry_run=args.dry_run,
                                         bTemp_sat_name=args.cloud_sat_name, no_cloud_sat=args.no_cloud_sat, file_list=True,
                                         chunks='auto', max_chunk_size=1e8, assign_releases_position_coords=False,
                                         grid_resolution=args.grid_res, grid_res_str=args.grid_res_str,
                                         print_debug=args.print_debug,
                                         softioli_output_dirpath=args.softioli_output_dir, result_dirname=args.result_dirname,
                                         result_ds_name=args.result_ds_name,
                                         overwrite_weighted_ds=args.overwrite_weighted_ds,
                                         overwrite_sat_files=args.overwrite_sat_files,
                                         rm_pre_regrid_abi_file=args.rm_pre_regrid_abi_file,
                                         rm_pre_regrid_li_file=args.rm_pre_regrid_glm_file)

    if len(indices_flight_id_fp_not_ok_or_missing) > 0:
        print('\nxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx')
        print(
            f'{len(indices_flight_id_fp_not_ok_or_missing)} invalid or missing flexpart outputs, please check them before running the program again')
        if len(flight_id_list_fp_not_success) > 0:
            print(f'Flexpart simulation failed:\n{flight_id_list_fp_not_success}')
        if len(flight_id_list_fp_output_missing) > 0:
            print(f'Missing flexpart output file:\n{flight_id_list_fp_output_missing}')
        print('\nxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx')

    if len(missing_dates["lightning"]) > 0:
        print('\nxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx')
        print(
            f'{len(missing_dates["lightning"])} missing {args.lightning_sat_name} daily files, please download them before running the program again: \n{missing_dates["lightning"]}')
        print('See logs above for more details on which flights have not been computed')
        print('\nxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx')

    if len(missing_dates["cloud"]) > 0:
        print('\nxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx')
        print(
            f'{len(missing_dates["cloud"])} missing ABI daily files, please download them before running the program again: \n{missing_dates["cloud"]}')
        print('See logs above for more details on which flights have not been computed')
        print('\nxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx')
