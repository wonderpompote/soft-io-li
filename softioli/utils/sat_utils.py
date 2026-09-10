from collections import defaultdict
import pathlib
from shutil import rmtree

import pandas as pd
from pandas import Timedelta
import xarray as xr

from .utils_functions import date_to_pd_timestamp
from . import constants as cts, ABIPathParser, open_hdf4
from . import GLMPathParser
from .ABIPathParser import ABIPathParser
from .MTGLIPathParser import MTGLIPathParser


def get_PathParser(sat_name):
    if sat_name == cts.GOES_SATELLITE_GLM:
        return GLMPathParser
    elif sat_name == cts.GOES_SATELLITE_ABI:
        return ABIPathParser
    elif sat_name == cts.MTG_LI:
        return MTGLIPathParser
    else:
        raise ValueError(f'Could not find corresponding PathParser, "{sat_name}" {cts.SAT_VALUE_ERROR}')

def generate_sat_filename_pattern(sat_name, regrid, regrid_res_str=cts.GRID_RESOLUTION_STR, hourly=True, sat_version_pattern=None,
                                  YYYY=cts.YYYY_pattern, DDD=cts.DDD_pattern, MM=cts.MM_pattern, DD=cts.DD_pattern, start_HH=cts.HH_pattern, end_HH=cts.HH_pattern, mm=cts.mm_pattern, sss=cts.sss_pattern):
    """
    Generate filename pattern for a specific satellite, naming convention and regrid resolution (to be used with pathlib glob function)
    :param sat_name: <str> name of the satellite (only 'GOES_GLM' supported for now)
    :param regrid: <bool>
    :param regrid_res_str: <str> grid resolution str (to be added to the resulting filename)
    :param YYYY: <str> or <int> year
    :param DDD: <str> or <int> day of the year
    :param start_HH: <str> or <int> start hour
    :param end_HH:  <str> or <int> end hour
    :return: <str> filename pattern for the satellite
    """
    # GLM
    if sat_name == cts.GOES_SATELLITE_GLM:
        sat_version_pattern = cts.GLM_Gxx_PATTERN if sat_version_pattern is None else sat_version_pattern
        if not hourly: # OR_GLM-L2-LCFA_Gxx_sYYYYDDDHHmmsss_e2YYYYDDDHHmmsss_cYYYYDDDHHmmsss.nc
            filename_pattern = f'{cts.GLM_PATH_PREFIX}_{sat_version_pattern}_s{YYYY}{DDD}{start_HH}{mm}{sss}_e{YYYY}{DDD}{end_HH}{cts.mm_pattern}{cts.sss_pattern}_c{YYYY}{DDD}{cts.HH_pattern}{cts.mm_pattern}{cts.sss_pattern}.nc'
        else: # OR_GLM-L2-LCFA_Gxx_YYYY_MM_DD_HH-HH.nc
            filename_pattern = f'{cts.GLM_PATH_PREFIX}_{sat_version_pattern}_{YYYY}_{MM}_{DD}_{start_HH}-{end_HH}.nc'
    # ABI
    elif sat_name == cts.GOES_SATELLITE_ABI:
        sat_version_pattern = cts.ABI_GOESXX_PATTERN if sat_version_pattern is None else sat_version_pattern
        if not hourly:  # GEO_L1B-GOES1x_YYYY-MM-DDTHH-mm-ss_X_IR10x_V1-0x.hdf
            filename_pattern = f'GEO_L1B-{sat_version_pattern}_{YYYY}-{MM}-{DD}T{start_HH}-{mm}-{mm}_[NSG]_IR10[37]_V1-0[4-6].hdf'
        else:  # ABI_GEO_L1B-GOES1x_YYYY_MM_DD_HH1-HH2.nc or xxdeg_ABI_GEO_L1B-GOES1x_YYYY_MM_DD_HH1-HH2.nc
            filename_pattern = f"ABI_GEO_L1B-{sat_version_pattern}_{YYYY}_{MM}_{DD}_{start_HH}-{end_HH}.nc"
    # MTG-LI
    elif sat_name == cts.MTG_LI:
        if not hourly: # W_XX-EUMETSAT-Darmstadt,IMG+SAT,MTI1+LI-2-LFL--FD--CHK-BODY---NC4E_C_EUMT_YYYYMMDDHHmmss_L2PF_OPE_YYYYMMDDHHmmss1_YYYYMMDDHHmmss2_N__T/C/O_xxxx_xxxx.nc
            filename_pattern = f'W_XX-EUMETSAT-Darmstadt,IMG+SAT,MTI1+LI-2-LFL--FD--CHK-BODY---NC4E_C_EUMT_{YYYY}{MM}{DD}{start_HH}{mm}{mm}_L2PF_OPE_{YYYY}{MM}{DD}{start_HH}{mm}{mm}_{YYYY}{MM}{DD}{start_HH}{mm}{mm}_N__[T,C,O]_[0-9][0-9][0-9][0-9]_[0-9][0-9][0-9][0-9].nc'
        else:
            filename_pattern = f'MTG_I1_LI_{YYYY}_{MM}_{DD}_{start_HH}-{end_HH}.nc'
    # NLDN
    elif sat_name == cts.NLDN:
        if hourly:
            filename_pattern = f"{cts.NLDN_PATH_PREFIX}_{YYYY}_{MM}_{DD}_{start_HH}-{end_HH}.nc"
    else:
        raise ValueError(
            f'{sat_name} NOT supported yet. Supported satellite so far: "{cts.GOES_SATELLITE_GLM}", "{cts.GOES_SATELLITE_ABI}"')

    if regrid and hourly:
        return f'{regrid_res_str}_{filename_pattern}'
    else:
        return filename_pattern


def generate_sat_dirname_pattern(sat_name, regrid, regrid_res_str=cts.GRID_RESOLUTION_STR, YYYY=cts.YYYY_pattern, MM=cts.MM_pattern, DD=cts.DD_pattern):
    """
    Generate directory name pattern for a specific satellite and regrid resolution (to be used with pathlib glob function)
    :param sat_name: <str>
    :param regrid: <bool>
    :param regrid_res_str: <str>
    :return: <str> directory name pattern for the satellite
    """
    # GLM
    if sat_name == cts.GOES_SATELLITE_GLM: # OR_GLM-L2-LCFA_YYYY_MM_DD
        dirname_pattern = f'{cts.GLM_PATH_PREFIX}_{YYYY}_{MM}_{DD}'
    # ABI
    elif sat_name == cts.GOES_SATELLITE_ABI: #ABI_GEO_L1B_YYYY_MM_DD
        dirname_pattern = f"ABI_GEO_L1B_{YYYY}_{MM}_{DD}"
    # MTG-LI
    elif sat_name == cts.MTG_LI: # MTG_I1_LI_YYYYMMDD
        dirname_pattern = f'MTG_I1_LI_{YYYY}{MM}{DD}'
    # NLDN
    elif sat_name == cts.NLDN:
        dirname_pattern = f'{cts.NLDN_PATH_PREFIX}_{YYYY}_{MM}_{DD}'
    else:
        raise ValueError(f'{sat_name} {cts.SAT_VALUE_ERROR}')

    if regrid:
        return f'{regrid_res_str}_{dirname_pattern}'
    else:
        return dirname_pattern


def generate_sat_dir_path(date, sat_name, regrid, regrid_res_str=cts.GRID_RESOLUTION_STR, target_dir=None):
    """
    Generate the absolute path to the directory containing the satellite data for a specific date (regridded or not)
    <!> The path does not necessarily point to an existing directory, if it does not exist it will need to be created and filled with the correct data files
    :param date: <pandas.Timestamp> or <numpy.datetime64> or <datetime.datetime> or <GLMPathParser>
    :param sat_name: <str> satellite name
    :param regrid: <bool> indicates if the directory contains regridded files
    :param regrid_res_str: <str> regrid resolution (if regrid == True)
    :param target_dir: <str> or <pathlib.Path> root directory path (if different from default (/o3p/patj/glm), mostly used for testing)
    :return: <pathlib.Path> object pointing to satellite data directory for a specific date
    """
    # check date
    date = date_to_pd_timestamp(date)
    # now that we have the pandas.Timestamp we can generate the path
    # GLM
    if sat_name == cts.GOES_SATELLITE_GLM:
        root_dir_path = target_dir if target_dir is not None else cts.GLM_ROOT_DIR
        if regrid:
            return pathlib.Path(
                f'{root_dir_path}/{cts.REGRID_GLM_DIRNAME}/{date.year:04d}/{regrid_res_str}_{cts.GLM_PATH_PREFIX}_{date.year:04d}_{date.month:02d}_{date.day:02d}')
        else:
            return pathlib.Path(
                f'{root_dir_path}/{cts.PRE_REGRID_GLM_DIRNAME}/{date.year:04d}/{cts.GLM_PATH_PREFIX}_{date.year:04d}_{date.month:02d}_{date.day:02d}')
    # ABI
    elif sat_name == cts.GOES_SATELLITE_ABI:
        root_dir_path = target_dir if target_dir is not None else cts.ABI_ROOT_DIR
        if regrid:
            return pathlib.Path(
                f'{root_dir_path}/{cts.REGRID_ABI_DIRNAME}/{date.year:04d}/{regrid_res_str}_{cts.ABI_PATH_PREFIX}_{date.year:04d}_{date.month:02d}_{date.day:02d}')
        else:
            return pathlib.Path(
                f'{root_dir_path}/{cts.PRE_REGRID_ABI_DIRNAME}/{date.year:04d}/{cts.ABI_PATH_PREFIX}_{date.year:04d}_{date.month:02d}_{date.day:02d}')
    # MTG-LI
    elif sat_name == cts.MTG_LI:
        root_dir_path = target_dir if target_dir is not None else cts.MTG_LI_ROOT_DIR
        if regrid:
            return pathlib.Path(f'{root_dir_path}/{cts.REGRID_MTG_LI_DIRNAME}/{date.year:04d}/{regrid_res_str}_{cts.MTG_LI_PATH_PREFIX}_{date.year:04d}{date.month:02d}{date.day:02d}')
        else:
            return pathlib.Path(
                f'{root_dir_path}/{cts.PRE_REGRID_MTG_LI_DIRNAME}/{date.year:04d}/{cts.MTG_LI_PATH_PREFIX}_{date.year:04d}{date.month:02d}{date.day:02d}')
    else:
        raise ValueError(f'{sat_name} {cts.SAT_VALUE_ERROR}')


def generate_sat_hourly_file_path(date, sat_name, satellite, regrid, regrid_res_str=cts.GRID_RESOLUTION_STR,
                                  dir_path=None):
    """
    Generate absolute path to a satellite hourly file (regridded or not)
    <!> The path does not necessarily point to an existing file, it might point to a file that has yet to be created
    :param date: <pandas.Timestamp> or <numpy.datetime64> or <datetime.datetime> or <GLMPathParser>
    :param sat_name: <str> satellite name
    :param satellite: <str> satellite version e.g.: 'G16' for GLM or 'GOES16' for ABI
    :param regrid: <bool> indicates if the file is regridded
    :param regrid_res_str: <str> regrid resolution (if regrid == True)
    :param dir_path: <str> or <pathlib.Path> mostly used for testing purposes, if == None the default directory path is used
    :return: <pathlib.Path> path to satellite hourly data file
    """
    date = date_to_pd_timestamp(date)
    dir_path = generate_sat_dir_path(date=date, sat_name=sat_name, regrid=regrid, regrid_res_str=regrid_res_str,
                                     target_dir=dir_path)
    # GLM
    if sat_name == cts.GOES_SATELLITE_GLM:
        filename = f'{cts.GLM_PATH_PREFIX}_{satellite}_{date.year:04d}_{date.month:02d}_{date.day:02d}_{date.hour:02d}-{(date + Timedelta(hours=1)).hour:02d}.nc'
    # ABI
    elif sat_name == cts.GOES_SATELLITE_ABI:
        filename = f'{cts.ABI_PATH_PREFIX}-{satellite}_{date.year:04d}_{date.month:02d}_{date.day:02d}_{date.hour:02d}-{(date + Timedelta(hours=1)).hour:02d}.nc'
    # MTG-Li
    elif sat_name == cts.MTG_LI:
        filename = f'{cts.MTG_LI_PATH_PREFIX}_{date.year:04d}_{date.month:02d}_{date.day:02d}_{date.hour:02d}-{(date + Timedelta(hours=1)).hour:02d}.nc'
    else:
        raise ValueError(f'{sat_name} {cts.SAT_VALUE_ERROR}')
    if regrid:
        filename = f'{regrid_res_str}_{filename}'
    return dir_path / pathlib.Path(filename)


def get_list_of_dates_from_list_of_sat_path(path_list, directory, sat_name, regrid, date_str, hourly=True, date_str_format='%Y-%m-%d'):
    """
    Takes a list of satellite data paths (directories or files) and returns the corresponding dates
    extracted from the file/directory names
    @param hourly:
    @param path_list: <list> [ <pathlib.Path> or <str>, ... ]
    @param directory: <bool> indicates if the paths point to directories
    @param sat_name: <str> satellite name
    @param regrid: <bool> indicates if the paths point to regridded files or directories
    @param date_str: <bool> indicates if we want the dates as str
    @return: <list> [ <pd.Timestamp> or <str>, ... ] list of all the dates as pd.Timestamps or str
    """
    date_list = []
    PathParser = get_PathParser(sat_name)
    for p in path_list:
        date = PathParser(p, regrid=regrid, directory=directory, hourly=hourly) \
            .get_start_date_pdTimestamp(ignore_missing_start_hour=True)
        if date_str:
            date = date.strftime(date_str_format)
        date_list.append(date)

    return date_list


def generate_sat_dir_list_between_start_end_date(start_date, end_date, satellite, regrid,
                                                 regrid_res_str=cts.GRID_RESOLUTION_STR):
    """
    Generate list (iter) of daily directory path containing satellite data between start and end date
    <!> does not necessarily generate directory paths <!>
    :param start_date: <pandas.Timestamp> or <numpy.datetime64> or <datetime.datetime>
    :param end_date: <pandas.Timestamp> or <numpy.datetime64> or <datetime.datetime>
    :param satellite: <str> satellite name
    :param regrid: <bool> indicates if the directory contains regridded files
    :param regrid_res_str: <str> regrid resolution
    :return: <list>
    """
    # make sure the dates are pd.Timestamps
    start_date = date_to_pd_timestamp(start_date)
    end_date = date_to_pd_timestamp(end_date)
    dir_list = [
        generate_sat_dir_path(
            date=start_date + pd.Timedelta(i, 'D'), sat_name=satellite,
            regrid=regrid, regrid_res_str=regrid_res_str
        )
        for i in range((end_date - start_date).days + 1)
    ]
    return dir_list


# TODO: add check dir_list contient que des pathlib.PurePath objects (?)
def get_data_files_list_between_start_end_date(dir_list, start_date, end_date, sat_name, regrid, hourly=True):
    PathParser = get_PathParser(sat_name)
    start_date, end_date = date_to_pd_timestamp(start_date), date_to_pd_timestamp(end_date)
    file_list = []
    dir_list = sorted([pathlib.Path(dir_p) for dir_p in dir_list])
    fname_pattern = generate_sat_filename_pattern(sat_name=sat_name, regrid=regrid, hourly=hourly)
    for file in dir_list[0].glob(fname_pattern):
        fparser = PathParser(file_url=file, regrid=regrid, hourly=hourly)
        if fparser.start_hour >= start_date.hour:
            file_list.append(file)
    for file in dir_list[-1].glob(fname_pattern):
        fparser = PathParser(file_url=file, regrid=regrid, hourly=hourly)
        if fparser.start_hour <= end_date.hour:
            file_list.append(file)
    # for the days: start_day < day < end_day --> get all files matching generic filename pattern
    for dir_path in dir_list[1:-1]:
        file_list.extend(dir_path.glob(fname_pattern))
    return sorted(file_list)


def get_list_of_sat_files_grouped_by_date(sat_files_list, sat_name, regrid, print_debug=False):
    PathParser = get_PathParser(sat_name)
    files_by_date_dict = defaultdict(list)
    for sat_file in sat_files_list:
        sat_file_date = PathParser(sat_file, regrid).get_start_date_pdTimestamp()
        files_by_date_dict[sat_file_date].append(sat_file)
    return files_by_date_dict


def get_abi_coords_file(sat_version, file_version, print_debug=False):
    if sat_version in ['GOES12', 'GOES13']:
        if file_version is not None:
            if file_version <= "V1-05":
                coords_file = cts.GOES_0750_C0_COORDS_FILE
            elif file_version >= "V1-06":
                coords_file = cts.GOES_0750_C1_COORDS_FILE
    elif sat_version == 'GOES16':
        coords_file = cts.GOESNG_0750_COORDS_FILE
    elif sat_version in ['GOES17', 'GOES18']:
        coords_file = cts.GOESNG_1370_COORDS_FILE
    else:
        raise ValueError(
            f'{sat_version} unsupported. Supported ABI satellites so far: "GOES12", "GOES13", "GOES16", "GOES17", "GOES18"')

    if print_debug:
        print(f'Using coords_file: {coords_file}')
    return pathlib.Path(f'{cts.ABI_COORDS_DIRPATH}/{coords_file}')



def merge_GOES_sat_data_with_overlap(regrid_daily_file_list, sat_name, PathParser, print_debug=False):
    sat_versions = set()
    # divide files in 3 categories: files without overlap, GOES-E files w/ overlap, GOES-W files w/ overlap
    files_without_overlap = []
    GOES_EAST_files_with_overlap = []
    GOES_WEST_files_with_overlap = []
    # get list of files grouped by date
    files_by_date_dict = get_list_of_sat_files_grouped_by_date(sat_files_list=regrid_daily_file_list, sat_name=sat_name,
                                                               regrid=True)
    # for each date, check if overlap and put file_list in corresponding list
    for date, file_list in sorted(files_by_date_dict.items()):
        if len(file_list) == 1:  # if just one file --> no overlap
            f_parsed_sat_version = PathParser(file_list[0], regrid=True).satellite_version
            sat_versions.add(f_parsed_sat_version)
            files_without_overlap.extend(file_list)
        else:  # if more than one file, add them to corresponding list of files to be pre-processed
            if print_debug:
                print(f'Overlapping files for {date}: {file_list}')
            for f in file_list:
                f_parsed_sat_version = PathParser(f, regrid=True).satellite_version
                sat_versions.add(f_parsed_sat_version)
                if f_parsed_sat_version in cts.GOES_EAST_SAT_VERSION:
                    GOES_EAST_files_with_overlap.append(f)
                elif f_parsed_sat_version in cts.GOES_WEST_SAT_VERSION:
                    GOES_WEST_files_with_overlap.append(f)
                else:
                    raise ValueError(f'Unsupported satellite version: {f_parsed_sat_version}')

    # functions to pre-process GOES-E and GOES-W data (cut at 100°W)
    def pre_process_GOES_EAST_data(ds):
        # if lightning sat, only keep flash_count variable to lighten computation time
        if sat_name == cts.GOES_SATELLITE_GLM:
            ds = ds[['flash_count']]
        return ds.where(ds.longitude >= -100, drop=True)

    def pre_process_GOES_WEST_data(ds):
        # if lightning sat, only keep flash_count variable to lighten computation time
        if sat_name == cts.GOES_SATELLITE_GLM:
            ds = ds[['flash_count']]
        return ds.where(ds.longitude < -100, drop=True)

    datasets_to_merge = []
    # for each list of files, open_mfdataset and pre-process accordingly before merging into a single dataset
    # open_mfdataset allows lazy loading of datasets + pre-processing integrated
    if files_without_overlap:
        merged_dataset = xr.open_mfdataset(files_without_overlap, parallel=True, engine='h5netcdf',
                              combine='nested', concat_dim='time', combine_attrs='drop_conflicts')
        if sat_name == cts.GOES_SATELLITE_GLM:
            merged_dataset = merged_dataset[['flash_count']]
        datasets_to_merge.append(merged_dataset)
    if GOES_EAST_files_with_overlap:
        datasets_to_merge.append(
            xr.open_mfdataset(GOES_EAST_files_with_overlap, parallel=True, engine='h5netcdf',
                              preprocess=pre_process_GOES_EAST_data, concat_dim='time',
                              combine='nested', combine_attrs='drop_conflicts')
        )
    if GOES_WEST_files_with_overlap:
        datasets_to_merge.append(
            xr.open_mfdataset(GOES_WEST_files_with_overlap, parallel=True, engine='h5netcdf',
                              preprocess=pre_process_GOES_WEST_data, concat_dim='time',
                              combine='nested', combine_attrs='drop_conflicts')
        )

    if print_debug:
        print(f'Merging {len(datasets_to_merge)} datasets')
        print(f'details: files_without_overlap={len(files_without_overlap)}, GOES_EAST_files_with_overlap={len(GOES_EAST_files_with_overlap)}, GOES_WEST_files_with_overlap={len(GOES_WEST_files_with_overlap)}')
    # merge without overlap, GOES-E and GOES-W datasets into a single dataset
    sat_ds = xr.merge(datasets_to_merge, combine_attrs='drop_conflicts')
    sat_ds.attrs[cts.SAT_VERSION_ATTRS_NAME] = list(sat_versions)
    if print_debug:
        print(f'GOES-EAST and GOES-WEST datasets merged successfully!')

    return sat_ds


def generate_abi_hourly_nc_file_from_15min_hdf_files(dir_path_list, remove_temp_files=False, overwrite=False, print_debug=False):
    # pour chaque daily dir
    if print_debug:
        print(f'generate_abi_hourly_nc_file_from_15min_hdf_files:\npath_list={dir_path_list}\noverwrite={overwrite}')
    for dir_p in dir_path_list:
        dir_date = ABIPathParser(file_url=dir_p, regrid=False, directory=True).get_start_date_pdTimestamp(ignore_missing_start_hour=False)
        if print_debug:
            print(f'Directory date generate hourly pre regrid file: {dir_date}')
        for h in range(24):
            # get filename pattern
            filename_pattern = generate_sat_filename_pattern(
                sat_name=cts.GOES_SATELLITE_ABI,
                regrid=False, hourly=False,
                YYYY=dir_date.year, MM=f'{dir_date.month:02d}', DD=f'{dir_date.day:02d}',
                start_HH=f'{h:02d}'
            )
            # get list of all 15-min files for the corresponding hour
            h_file_list = sorted(pathlib.Path(f'{dir_p}/temp').glob(filename_pattern))
            if not h_file_list:
                continue
            h_abi_ds_list = defaultdict(list)
            hdf_file_list = defaultdict(list)
            for h_file in h_file_list:
                # open file + rename col names to correspond to coords_ds col names
                with open_hdf4(str(h_file)).rename(dict(NbLines='Nlin', NbColumns='Ncol'))['Brightness_Temperature'] as abi_bTemp_da_wout_coords:
                    # get corresponding coords file_path
                    h_file_parser = ABIPathParser(file_url=h_file, regrid=False, hourly=False)
                    coords_file_path = get_abi_coords_file(sat_version=h_file_parser.satellite_version, file_version=h_file_parser.file_version, print_debug=print_debug)
                    # combine coords dataset with abi dataset
                    with xr.open_dataset(coords_file_path)[['Latitude', 'Longitude']] as coords_ds:
                        b_temp_w_coords_ds = coords_ds.assign(brightness_temperature=abi_bTemp_da_wout_coords)
                        # add file timestamp
                        b_temp_w_coords_ds = b_temp_w_coords_ds.expand_dims({
                            'time': [h_file_parser.get_start_date_pdTimestamp(ignore_missing_start_hour=False).to_datetime64()]
                        })
                        b_temp_w_coords_ds.attrs = abi_bTemp_da_wout_coords.attrs
                        b_temp_w_coords_ds = b_temp_w_coords_ds.rename_vars({'Latitude': 'latitude', 'Longitude': 'longitude'})
                        b_temp_w_coords_ds.attrs[cts.SAT_VERSION_ATTRS_NAME] = h_file_parser.satellite_version
                        # add ds to list corresponding to sat version
                        h_abi_ds_list[h_file_parser.satellite_version].append(b_temp_w_coords_ds)
                        # add hdf file name to corresponding sat version
                        hdf_file_list[h_file_parser.satellite_version].append(h_file.name)
            for sat, ds_list in sorted(h_abi_ds_list.items()):
                result_hourly_filename = generate_sat_hourly_file_path(
                                                        date=h_file_parser.start_date,
                                                        sat_name=cts.GOES_SATELLITE_ABI, satellite=sat,
                                                        regrid=False, dir_path=None)
                h_abi_ds_sat = xr.merge(ds_list, combine_attrs="drop_conflicts")
                h_abi_ds_sat.attrs[cts.SAT_VERSION_ATTRS_NAME] = sat
                h_abi_ds_sat.attrs['raw_hdf_files'] = hdf_file_list[sat]
                if not pathlib.Path(result_hourly_filename).exists() or (pathlib.Path(result_hourly_filename).exists() and overwrite):
                    h_abi_ds_sat.to_netcdf(
                        path=result_hourly_filename, mode='w',
                        encoding={"time": {"dtype": 'float64', 'units': 'nanoseconds since 1970-01-01'}}
                    )
                    print(f"Saved {result_hourly_filename}")
                else:
                    print(f'{result_hourly_filename} already exists!')

        if remove_temp_files:
            rmtree(pathlib.Path(f'{dir_p}/temp'))
            print(f"Deleting {dir_p}/temp directory")
