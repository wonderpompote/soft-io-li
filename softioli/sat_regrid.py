from collections import defaultdict
from datetime import datetime
import numpy as np
import pathlib
import pandas as pd
from shutil import rmtree
import xarray as xr

from utils import generate_sat_hourly_file_path, generate_sat_filename_pattern, \
    generate_sat_dirname_pattern, ABIPathParser, get_abi_coords_file, open_hdf4, get_SatPathParser
from utils import constants as cts
from utils.constants import SAT_SETTINGS, raw_lat_cname, raw_lon_cname, flash_area_varname, flash_energy_varname, \
    attrs_to_keep
from utils import xarray_pandas_utils as xr_pd_utils


# TODO: gérer quand goes w et goes e
def generate_lightning_sat_hourly_regrid_file(pre_regrid_file_url, sat_name, grid_res, overwrite,
                                              result_file_path, lat_min=cts.FPOUT_LAT_MIN, lat_max=cts.FPOUT_LAT_MAX,
                                              lon_min=cts.FPOUT_LON_MIN, lon_max=cts.FPOUT_LON_MAX,
                                              naming_convention=None, rm_pre_regrid_file=False):
    """
    Pre-process lightning satellite hourly data file to regrid it to specific resolution and obtain
    the following information for each grid cell:
        - 'flash_count': number of lightning flashes occurences
        - 'flash_energy_log_hist': histogram of the flash energy values (log10)
        - 'flash_area_log_hist': histogram of the flash area values (log10)
    :param pre_regrid_file_url: <pathlib.Path> or <str>
    :param sat_name: <str> satellite name (supported so far: 'GOES_GLM')
    :param grid_res: <float> grid resolution (default: 0.5°)
    :param grid_res_str: <str> grid resolution str (default: '05deg')
    :param overwrite: <bool> overwrite file if it already exists
    :param lat_min: <float>
    :param lat_max: <float>
    :param lon_min: <float>
    :param lon_max: <float>
    :param result_file_path: <str> or <pathlib.Path>
    :param naming_convention: <str> pre-regrid file naming convention (useful for backward compatibility). Supported values: 'OLD_TEMP', 'OLD', None (default)
    """
    if not sat_name in cts.SAT_SETTINGS:
        raise ValueError(f'{sat_name} {cts.SAT_VALUE_ERROR}')

    SatPathParser = get_SatPathParser(sat_name)

    # get pre-regrid file start date (year, day, hour) with <sat>PathParser
    pre_regrid_path_parsed = SatPathParser(file_url=pre_regrid_file_url, regrid=False,
                                           naming_convention=naming_convention)

    result_file_path = pathlib.Path(result_file_path)
    # check if regrid file exists and if it doesn't OR if overwrite == True --> "create it"
    if (not result_file_path.exists()) or (result_file_path.exists() and overwrite):
        # generate empty dataset with correctly gridded lat et lon
        target_ds = xr.Dataset(
            coords={
                'latitude': np.arange(lat_min, lat_max + grid_res, grid_res),
                'longitude': np.arange(lon_min, lon_max + grid_res, grid_res)
            },
            attrs={'grid_resolution': f'{grid_res}° x {grid_res}°',
                   'pre_regrid_satellite_file': pre_regrid_path_parsed.url.name}
        )
        #       STEP 4.2: open pre-regrid glm file
        with xr.open_dataset(pre_regrid_file_url) as lightning_sat_ds:
            # assign new longitude and latitude coords with chosen grid resolution using nearest method
            _ds_assigncoords_lonlat = lightning_sat_ds.assign_coords({
                'latitude': target_ds.latitude.sel(latitude=lightning_sat_ds[SAT_SETTINGS[sat_name][raw_lat_cname]],
                                                   method='nearest'),
                'longitude': target_ds.longitude.sel(longitude=lightning_sat_ds[SAT_SETTINGS[sat_name][raw_lon_cname]],
                                                     method='nearest')
            })
            # keep several attributes from the original sat file
            # TODO: update conditions (processing_level) if attribute names are different for other satellites
            new_attrs = {}
            for attr in SAT_SETTINGS[sat_name][attrs_to_keep]:
                if attr == "processing_level":
                    new_attrs[f'pre_regrid_data_{attr}'] = lightning_sat_ds.attrs.get(attr, '')
                else:
                    new_attrs[attr] = lightning_sat_ds.attrs.get(attr, '')
            new_attrs['pre_regrid_satellite_file'] = pre_regrid_path_parsed.url.name
            target_ds = target_ds.assign_attrs(new_attrs)
            # apply operations (count + hist) on flash energy and flash area variables
            flash_energy = SAT_SETTINGS[sat_name][flash_energy_varname]
            flash_area = SAT_SETTINGS[sat_name][flash_area_varname]
            # only keep relevant variables and coords
            _ds = _ds_assigncoords_lonlat[[flash_energy, flash_area]] \
                .reset_coords(names=['latitude', 'longitude'], drop=False) \
                .reset_coords(drop=True)
            # flash count <!> result = xarray.Dataset
            count_ds = xr_pd_utils.count_using_pandas(_ds[[flash_energy, 'latitude', 'longitude']],
                                                      data_var_name=flash_energy, res_var_name='flash_count')
            count_ds['flash_count'].attrs[
                'long_name'] = f'Number of flash occurrences in a {grid_res}° x {grid_res}° x 1h grid cell'
            # flash energy histogram <!> result = xarray.DataArray
            _ds['flash_energy_log'] = np.log10(_ds[flash_energy])
            flash_en_hist_ds = xr_pd_utils.histogram_using_pandas(
                _ds[['flash_energy_log', 'latitude', 'longitude']], data_var_name='flash_energy_log',
                min_bin_edge=cts.f_en_min_bin, max_bin_edge=cts.f_en_max_bin,
                step=cts.f_en_hist_step, res_var_name='flash_energy_log_hist')
            flash_en_hist_ds['flash_energy_log_hist'].attrs.update({
                'long_name': f'Number of flash occurrences in log10(flash_energy) bin in a {grid_res}° x {grid_res}° x 1h grid cell',
                'comment': 'log10(flash_energy) bins between -15 and -10, step between bins = 0.1'
            })
            # flash area histogram
            _ds['flash_area_log'] = np.log10(_ds[flash_area])
            flash_area_hist_ds = xr_pd_utils.histogram_using_pandas(
                _ds[['flash_area_log', 'latitude', 'longitude']], data_var_name='flash_area_log',
                min_bin_edge=cts.f_ar_min_bin, max_bin_edge=cts.f_ar_max_bin,
                step=cts.f_ar_hist_step, res_var_name='flash_area_log_hist')
            flash_area_hist_ds['flash_area_log_hist'].attrs.update({
                'long_name': f'Number of flash occurrences in log10(flash_area) bin in a {grid_res}° x {grid_res}° x 1h grid cell',
                'comment': 'log10(flash_area) bins between 1.5 and 4.5, step between bins = 0.1'
            })
            # merge count and hist ds with target ds
            target_ds = xr.merge([count_ds, flash_en_hist_ds, flash_area_hist_ds, target_ds],
                                 combine_attrs='no_conflicts')
        # add pre-regrid file date to regrid date + add regrid file creation date attr
        target_ds = target_ds.expand_dims(
            {'time': [pre_regrid_path_parsed.get_start_date_pdTimestamp(ignore_missing_start_hour=False)]})
        target_ds.attrs['regrid_file_creation_date'] = datetime.now().isoformat()
        target_ds.attrs[cts.SAT_VERSION_ATTRS_NAME] = pre_regrid_path_parsed.satellite_version
        # TODO: réduire units de l'heure pour prendre moins de place (pas besoin de nanoseconds en soit)
        target_ds.to_netcdf(
            path=result_file_path, mode='w',
            encoding={"time": {"dtype": 'float64', 'units': 'nanoseconds since 1970-01-01'}}
        )
        print(f"Created netcdf file {result_file_path}")

    else:  # file already exists so no need to create it again
        print(f"{result_file_path} already exists")

    if rm_pre_regrid_file and result_file_path.exists():  # remove associated pre-regrid file to free up space
        pathlib.Path(pre_regrid_file_url).unlink()


def generate_cloud_temp_sat_hourly_regrid_file(pre_regrid_file_url, sat_name, grid_res, result_file_path,
                                               overwrite, rm_pre_regrid_file=False,
                                               lat_min=cts.FPOUT_LAT_MIN, lat_max=cts.FPOUT_LAT_MAX,
                                               lon_min=cts.FPOUT_LON_MIN, lon_max=cts.FPOUT_LON_MAX):
    SatPathParser = get_SatPathParser(sat_name)
    if sat_name == cts.GOES_SATELLITE_ABI:
        btemp_varname = 'brightness_temperature'
    else:
        raise ValueError(f'{sat_name} {cts.SAT_VALUE_ERROR}')

    result_file_path = pathlib.Path(result_file_path)
    # check if regrid file exists and if it doesn't OR if overwrite == True --> "create it"
    if (not result_file_path.exists()) or (result_file_path.exists() and overwrite):
        latitude = np.arange(lat_min, lat_max + grid_res, grid_res)
        longitude = np.arange(lon_min, lon_max + grid_res, grid_res)
        lat_da = xr.DataArray(latitude, coords={"latitude": latitude}, dims=['latitude'])
        lon_da = xr.DataArray(longitude, coords={'longitude': longitude}, dims=['longitude'])

        with xr.open_dataset(pre_regrid_file_url) as pre_regrid_ds:
            # replace longitude and latitude values with correct resolution using nearest routine
            pre_regrid_ds['latitude'] = lat_da \
                .sel(latitude=pre_regrid_ds['latitude'], method='nearest') \
                .where(pre_regrid_ds['latitude'].notnull())
            pre_regrid_ds['longitude'] = lon_da \
                .sel(longitude=pre_regrid_ds['longitude'], method='nearest') \
                .where(pre_regrid_ds['longitude'].notnull())
            # convert to dataframe to be able to groupby
            df = pre_regrid_ds.to_dataframe().reset_index()[
                ['time', 'latitude', 'longitude', btemp_varname]]
            # only keep mean value for each latitude, longitude, satellite, 15 min (time) group
            df_gpby_mean = df.groupby(['time', 'longitude', 'latitude'], sort=True).mean()
            result_ds = xr.Dataset.from_dataframe(df_gpby_mean)
            # only keep min value for the hour
            result_ds = result_ds.min('time')
            # add time dimension
            result_ds = result_ds.expand_dims({'time': [pre_regrid_ds.time[0].values]})
            # ensure all latitude and longitude values are included to avoid non monotonic latitude error when opening multiple files with different versions
            result_ds = result_ds.reindex(latitude=latitude, longitude=longitude, fill_value=np.nan)
            # add atributes
            new_attrs = {
                'grid_resolution': f'{grid_res}° x {grid_res}°',
                'regrid_file_creation_date': pd.Timestamp.now().isoformat()
            }
            new_attrs.update(pre_regrid_ds.attrs)
            result_ds = result_ds.assign_attrs(new_attrs)
            result_ds[btemp_varname].attrs = pre_regrid_ds[btemp_varname].attrs
            # save regrid file
            result_ds.to_netcdf(
                path=result_file_path, mode='w',
                encoding={"time": {"dtype": 'float64', 'units': 'nanoseconds since 1970-01-01'}}
            )
            print(f"Created netcdf file {result_file_path}")

    else:  # file already exists so no need to create it again
        print(f"{result_file_path} already exists")

    if rm_pre_regrid_file and result_file_path.exists():  # remove associated pre-regrid file to free up space
        pathlib.Path(pre_regrid_file_url).unlink()

def generate_abi_hourly_nc_file_from_15min_hdf_files(path_list, remove_temp_files=False, overwrite=False, print_debug=False):
    # pour chaque daily dir
    if print_debug:
        print(f'generate_abi_hourly_nc_file_from_15min_hdf_files:\npath_list={path_list}\noverwrite={overwrite}')
    for dir_p in path_list:
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
                h_abi_ds_sat[cts.SAT_VERSION_ATTRS_NAME] = sat
                h_abi_ds_sat['raw_hdf_files'] = hdf_file_list[sat]
                if not pathlib.Path(result_hourly_filename).exists() or (pathlib.Path(result_hourly_filename).exists() and overwrite):
                    h_abi_ds_sat.to_netcdf(
                        path=result_hourly_filename, mode='w',
                        encoding={"time": {"dtype": 'float64', 'units': 'nanoseconds since 1970-01-01'}}
                    ) #TODO: est-ce que drop sat dimension du coup ? parce que pour la coupe faut que je sache qui est qui
                    print(f"Saved {result_hourly_filename}")
                else:
                    print(f'{result_hourly_filename} already exists!')

        if remove_temp_files:
            rmtree(pathlib.Path(f'{dir_p}/temp'))
            print(f"Deleting {dir_p}/temp directory")


def regrid_sat_files(path_list, sat_name, grid_res=cts.GRID_RESOLUTION,
                     grid_res_str=cts.GRID_RESOLUTION_STR, dir_list=False, overwrite=False,
                     naming_convention=None, remove_temp_abi_dir=False, result_dir_path=None,
                     print_debug=False, lat_min=cts.FPOUT_LAT_MIN, lat_max=cts.FPOUT_LAT_MAX,
                     lon_min=cts.FPOUT_LON_MIN, lon_max=cts.FPOUT_LON_MAX, rm_pre_regrid_file=False):
    """
    Function to regrid a list of hourly satellite data files to a specific grid resolution
    @param path_list: <list> [ <str> or <pathlib.Path>, ... ] list of files or daily directories to regrid
    @param sat_name: <str> name of the satellite (only 'GOES_GLM' and 'GOES_ABI' supported for now)
    @param grid_res: <float> grid resolution
    @param grid_res_str: <str> grid resolution str (to be added to the resulting filename)
    @param dir_list: <bool> if True, list received is a list of directories containing data files, NOT a list of files
    @param overwrite: <bool> overwrite file if it already exists
    @param naming_convention: <str> GLM file or directory naming convention (used for backward compatibility). Supported values: 'OLD_TEMP', 'OLD' or None (default)
    @param remove_temp_abi_dir: <bool> if True, the temp directory containing all 15min hdf files will be deleted after being processed
    @param result_dir_path: <str> or <pathlib.Path> mostly used for testing purposes, if == None the default directory path is used
    @param print_debug: <bool>
    @param lat_min:
    @param lat_max:
    @param lon_min:
    @param lon_max:
    @param rm_pre_regrid_file:
    @return:
    """
    if print_debug:
        print('--------------')
        print(f'regrid sat files: \nsat={sat_name} \ndir_list={dir_list} \npath_list={path_list}')
        print()
    SatPathParser = get_SatPathParser(sat_name)
    if sat_name == cts.GOES_SATELLITE_ABI:
        if dir_list:
            # check that we have all our hourly pre_regrid nc files
            path_to_concat_into_hourly_files = []
            hourly_pre_regrid_nc_file_pattern = generate_sat_filename_pattern(sat_name=cts.GOES_SATELLITE_ABI,
                                                                              regrid=False, hourly=True)
            for p in path_list:
                p = pathlib.Path(p)
                # if temp dir exists and not all hourly pre regrid nc files available
                if pathlib.Path(f'{p}/temp').exists() and len(sorted(p.glob(hourly_pre_regrid_nc_file_pattern))) == 0:
                    path_to_concat_into_hourly_files.append(p)
            if print_debug:
                print(f'{len(path_to_concat_into_hourly_files)} directories to concat into pre regrid hourly files')
                print(f'path_to_concat_into_hourly_files: {path_to_concat_into_hourly_files}')
                print()
            if len(path_to_concat_into_hourly_files) > 0:  # concat 15min hdf files into hourly nc files
                generate_abi_hourly_nc_file_from_15min_hdf_files(path_list=path_to_concat_into_hourly_files, print_debug=print_debug,
                                                                 remove_temp_files=remove_temp_abi_dir, overwrite=overwrite)
    elif sat_name == cts.GOES_SATELLITE_GLM:
        pass
    else:
        raise ValueError(
            f'{sat_name} {cts.SAT_VALUE_ERROR}')
    # if path_list contains paths to directories --> get list of files in each directory
    if dir_list:
        filename_pattern = generate_sat_filename_pattern(sat_name=sat_name, regrid=False, hourly=True,
                                                         naming_convention=naming_convention)
        # Get list of files in subdirectories
        path_list[:] = [
            file_path

            for dir_path in sorted(path_list)
            for file_path in dir_path.glob(filename_pattern)
        ]
    if print_debug:
        print(f'{len(path_list)} files to regrid')
        print(f'path_list={path_list}')
        print()
    for pre_regrid_file_url in path_list:
        # get pre-regrid file start date (year, day, hour) with <sat>PathParser
        if print_debug:
            print('---')
            print(f'pre_regrid_file_url: {pre_regrid_file_url}')
        pre_regrid_path_parsed = SatPathParser(file_url=pre_regrid_file_url, regrid=False, hourly=True,
                                               naming_convention=naming_convention)
        pre_regrid_file_date = pre_regrid_path_parsed.get_start_date_pdTimestamp(ignore_missing_start_hour=False)
        # create result nc file path
        result_file_path = generate_sat_hourly_file_path(date=pre_regrid_file_date, sat_name=sat_name, regrid=True,
                                                         satellite=pre_regrid_path_parsed.satellite_version,
                                                         regrid_res_str=grid_res_str, dir_path=result_dir_path)
        # if directory/ies containing result nc file path does NOT exist --> create it/them
        if not result_file_path.parent.exists():
            result_file_path.parent.mkdir(parents=True)
            print(f"Creating directory {result_file_path.parent}")

        # check if regrid file exists and if it doesn't OR if overwrite == True --> "create it"
        if not result_file_path.exists() or (result_file_path.exists() and overwrite):
            print(f"\nGenerating hourly regrid file for: {pre_regrid_file_url}")
            if sat_name == cts.GOES_SATELLITE_GLM:
                generate_lightning_sat_hourly_regrid_file(pre_regrid_file_url=pre_regrid_file_url,
                                                          sat_name=sat_name,
                                                          grid_res=grid_res,
                                                          overwrite=overwrite, result_file_path=result_file_path,
                                                          naming_convention=naming_convention,
                                                          lat_min=lat_min, lat_max=lat_max, lon_min=lon_min,
                                                          lon_max=lon_max, rm_pre_regrid_file=rm_pre_regrid_file)
            elif sat_name == cts.GOES_SATELLITE_ABI:
                generate_cloud_temp_sat_hourly_regrid_file(pre_regrid_file_url=pre_regrid_file_url,
                                                           sat_name=sat_name, grid_res=grid_res,
                                                           overwrite=overwrite, result_file_path=result_file_path,
                                                           lat_min=lat_min, lat_max=lat_max, lon_min=lon_min,
                                                           lon_max=lon_max, rm_pre_regrid_file=rm_pre_regrid_file)
        else:
            print(f'Regrid file {result_file_path} already exists')
