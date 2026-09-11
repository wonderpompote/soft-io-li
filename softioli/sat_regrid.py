from datetime import datetime
import numpy as np
import pathlib
import pandas as pd
import xarray as xr
import warnings
import flox.xarray

from utils.sat_utils import generate_abi_hourly_nc_file_from_15min_hdf_files
from utils import generate_sat_hourly_file_path, generate_sat_filename_pattern, \
    get_PathParser
from utils import constants as cts
from utils.constants import SAT_SETTINGS, raw_lat_cname, raw_lon_cname, flash_area_varname, flash_energy_varname, \
    attrs_to_keep
from utils import xarray_pandas_utils as xr_pd_utils


def generate_flash_count_ds(_df, data_var_name, res_var_name, grid_res):
    # flash count <!> result = xarray.Dataset
    count_ds = xr_pd_utils.count_using_pandas(_df=_df, data_var_name=data_var_name, res_var_name=res_var_name)
    count_ds['flash_count'].attrs['long_name'] = f'Number of flash occurrences in a {grid_res}° x {grid_res}° x 1h grid cell'
    return count_ds


# TODO: gérer quand goes w et goes e + refacto
def generate_lightning_sat_hourly_regrid_file(pre_regrid_file_url, sat_name,
                                              grid_res, generate_hists, generate_stats,
                                              overwrite, result_file_path,
                                              lat_min=cts.FPOUT_LAT_MIN, lat_max=cts.FPOUT_LAT_MAX,
                                              lon_min=cts.FPOUT_LON_MIN, lon_max=cts.FPOUT_LON_MAX,
                                              rm_pre_regrid_file=False):
    """
    Pre-process lightning satellite hourly data file to regrid it to specific resolution and obtain
    the following information for each grid cell:
        - 'flash_count': number of lightning flashes occurences
        - 'flash_energy_log_hist': histogram of the flash energy values (log10)
        - 'flash_area_log_hist': histogram of the flash area values (log10)
    :param pre_regrid_file_url: <pathlib.Path> or <str>
    :param sat_name: <str> satellite name (supported so far: 'GOES_GLM')
    :param grid_res: <float> grid resolution (default: 0.5°)
    :param generate_hists: <bool> indicates if flash energy and flash area histograms should be generated for each grid cell
    :param generate_stats: <bool> indicates if flash energy and flash area stast should be generated for each grid cell
    :param overwrite: <bool> overwrite file if it already exists
    :param lat_min: <float>
    :param lat_max: <float>
    :param lon_min: <float>
    :param lon_max: <float>
    :param result_file_path: <str> or <pathlib.Path>
    """
    if not sat_name in cts.SAT_SETTINGS:
        raise ValueError(f'{sat_name} {cts.SAT_VALUE_ERROR}')

    PathParser = get_PathParser(sat_name)

    # get pre-regrid file start date (year, day, hour) with <sat>PathParser
    pre_regrid_path_parsed = PathParser(file_url=pre_regrid_file_url, regrid=False)

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
        #       STEP 4.2: open pre-regrid sat file
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
            ds_to_merge_list = []
            count_ds = None

            if not flash_energy and not flash_area:
                raise ValueError(f'Expecting at least one variable equivalent to flash energy or flash area, got: flash_energy_varname={SAT_SETTINGS[sat_name][flash_energy_varname]} and flash_area_varname={SAT_SETTINGS[sat_name][flash_area_varname]}')

            if flash_energy:
                # only keep relevant variables and coords
                _ds = _ds_assigncoords_lonlat[flash_energy] \
                    .reset_coords(names=['latitude', 'longitude'], drop=False) \
                    .reset_coords(drop=True)
                # convert to pandas DataFrame for easier data processing
                _df = _ds[[flash_energy, 'latitude', 'longitude']].to_dataframe().reset_index(drop=True)
                if not count_ds:
                    count_ds = generate_flash_count_ds(_df=_df, data_var_name=flash_energy,
                                                       res_var_name='flash_count', grid_res=grid_res)
                    ds_to_merge_list.append(count_ds)

                if _ds[flash_energy].attrs['units'].upper() == 'J':
                    if generate_hists:
                        # flash energy histogram <!> result = xarray.DataArray
                        _df['flash_energy_log'] = np.log10(_df[flash_energy])
                        flash_en_hist_ds = xr_pd_utils.histogram_using_pandas(
                            _df, data_var_name='flash_energy_log',
                            min_bin_edge=cts.f_en_J_min_bin, max_bin_edge=cts.f_en_J_max_bin,
                            step=cts.f_en_J_hist_step, res_var_name='flash_energy_log_hist')
                        flash_en_hist_ds['flash_energy_log_hist'].attrs.update({
                            'long_name': f'Number of flash occurrences in log10(flash_energy) bin in a {grid_res}° x {grid_res}° x 1h grid cell',
                            'comment': 'log10(flash_energy) bins between -15 and -10, step between bins = 0.1'
                        })
                        ds_to_merge_list.append(flash_en_hist_ds)

                    if generate_stats:
                        # mean/std/percentiles on the flash_energy values (J), NOT on log values
                        flash_en_stats_ds = xr_pd_utils.stats_using_pandas(
                            _df, data_var_name=flash_energy, res_var_prefix='flash_energy')
                        for var in flash_en_stats_ds.data_vars:
                            flash_en_stats_ds[var].attrs['units'] = 'J'
                            flash_en_stats_ds[var].attrs['long_name'] = f'{var} of flash energy in a {grid_res}° x {grid_res}° x 1h grid cell'
                        ds_to_merge_list.append(flash_en_stats_ds)

                else:  # TODO: handle other flash energy variable units
                    warnings.warn(f'flash_energy unit ({_ds[flash_energy].attrs["units"]}), not supported yet')

            if flash_area: # flash_area and not flash_energy
                # only keep relevant variables and coords
                _ds = _ds_assigncoords_lonlat[flash_area] \
                    .reset_coords(names=['latitude', 'longitude'], drop=False) \
                    .reset_coords(drop=True)
                # convert to pandas DataFrame for easier data processing
                _df = _ds[[flash_area, 'latitude', 'longitude']].to_dataframe().reset_index(drop=True)
                if not count_ds:
                    # flash count <!> result = xarray.Dataset
                    count_ds = generate_flash_count_ds(_df=_df, data_var_name=flash_area,
                                                       res_var_name='flash_count', grid_res=grid_res)
                    ds_to_merge_list.append(count_ds)
                # flash area histogram
                flash_area_units = _ds[flash_area].attrs['units'].lower()
                # make sure flash_area values are in km2
                if flash_area_units == 'km2':
                    flash_area_km2_df = _ds[flash_area]
                elif flash_area_units == 'm2':
                    flash_area_km2_df = _ds[flash_area] / 1e6 # convert m2 to km2
                else:
                    flash_area_km2_df = None
                    warnings.warn(f'flash area variable unit ({_ds[flash_area].attrs["units"]}), not supported yet')
                if flash_area_km2_df is not None:
                    if generate_hists:
                        _df['flash_area_log'] = np.log10(flash_area_km2_df)
                        flash_area_hist_ds = xr_pd_utils.histogram_using_pandas(
                            _df, data_var_name='flash_area_log',
                            min_bin_edge=cts.f_ar_km2_min_bin, max_bin_edge=cts.f_ar_km2_max_bin,
                            step=cts.f_ar_km2_hist_step, res_var_name='flash_area_log_hist'
                        )
                        flash_area_hist_ds['flash_area_log_hist'].attrs.update({
                            'long_name': f'Number of flash occurrences in log10(flash_area) bin in a {grid_res}° x {grid_res}° x 1h grid cell',
                            'comment': 'log10(flash_area) bins between 1.5 and 4.5, step between bins = 0.1'
                        })
                        ds_to_merge_list.append(flash_area_hist_ds)

                    if generate_stats:
                        # mean/std/percentiles on the linear (non-log) flash_area values (km2)
                        _df['flash_area_km2'] = flash_area_km2_df.values
                        flash_area_stats_ds = xr_pd_utils.stats_using_pandas(
                            _df, data_var_name='flash_area_km2', res_var_prefix='flash_area')
                        for var in flash_area_stats_ds.data_vars:
                            flash_area_stats_ds[var].attrs['units'] = 'km2'
                            flash_area_stats_ds[var].attrs['long_name'] = f'{var} of flash area in a {grid_res}° x {grid_res}° x 1h grid cell'
                        ds_to_merge_list.append(flash_area_stats_ds)

            # merge count and hist ds with target ds
            ds_to_merge_list.append(target_ds)
            target_ds = xr.merge(ds_to_merge_list,
                                 combine_attrs='no_conflicts')
        # add pre-regrid file date to regrid date + add regrid file creation date attr
        target_ds = target_ds.expand_dims(
            {'time': [pre_regrid_path_parsed.get_start_date_pdTimestamp(ignore_missing_start_hour=False)]})
        target_ds.attrs['regrid_file_creation_date'] = datetime.now().isoformat()
        target_ds.attrs[cts.SAT_VERSION_ATTRS_NAME] = pre_regrid_path_parsed.satellite_version
        encoding = {"time": {"dtype": 'float64', 'units': 'nanoseconds since 1970-01-01'}}
        # zlib compression to reduce file size since it contains mostly zeros
        for var in target_ds.data_vars:
            encoding[var] = {"zlib": True, "complevel": 4}
        target_ds.to_netcdf(path=result_file_path, mode='w', encoding=encoding)
        print(f"Created netcdf file {result_file_path}")

    else:  # file already exists so no need to create it again
        print(f"{result_file_path} already exists")

    if rm_pre_regrid_file and result_file_path.exists():  # remove associated pre-regrid file to free up space
        pathlib.Path(pre_regrid_file_url).unlink()


def generate_cloud_temp_sat_hourly_regrid_file(pre_regrid_file_url, sat_name, grid_res, result_file_path,
                                               overwrite, rm_pre_regrid_file=False,
                                               lat_min=cts.FPOUT_LAT_MIN, lat_max=cts.FPOUT_LAT_MAX,
                                               lon_min=cts.FPOUT_LON_MIN, lon_max=cts.FPOUT_LON_MAX):
    """
    Function to generate hourly regrid file for cloud brightness temperature
    Only keep min brightness temperature for each grid cell
    """
    PathParser = get_PathParser(sat_name)
    if sat_name == cts.GOES_SATELLITE_ABI:
        btemp_varname = 'brightness_temperature'
    else:
        raise ValueError(f'{sat_name} {cts.SAT_VALUE_ERROR}')

    result_file_path = pathlib.Path(result_file_path)
    # check if regrid file exists and if it doesn't OR if overwrite == True --> "create it"
    if (not result_file_path.exists()) or (result_file_path.exists() and overwrite):
        latitude = np.arange(lat_min, lat_max + grid_res, grid_res)
        longitude = np.arange(lon_min, lon_max + grid_res, grid_res)
        n_lat, n_lon = len(latitude), len(longitude)

        with xr.open_dataset(pre_regrid_file_url) as pre_regrid_ds:
            lat_vals = pre_regrid_ds['latitude'].values
            lon_vals = pre_regrid_ds['longitude'].values

            # nearest target-grid value via direct arithmetic, clamped to the domain edges --
            # equivalent to the old .sel(..., method='nearest')
            lat_idx = np.clip(np.round((lat_vals - lat_min) / grid_res).astype(np.int64), 0, n_lat - 1)
            lon_idx = np.clip(np.round((lon_vals - lon_min) / grid_res).astype(np.int64), 0, n_lon - 1)
            lat_snapped = latitude[lat_idx]
            lon_snapped = longitude[lon_idx]

            # drop fill/off-disk pixels, same as the old .where(...notnull()) filter
            not_fill = ~np.isnan(lat_vals) & ~np.isnan(lon_vals)
            lat_snapped = np.where(not_fill, lat_snapped, np.nan)
            lon_snapped = np.where(not_fill, lon_snapped, np.nan)

            lat_snapped_da = xr.DataArray(lat_snapped, dims=pre_regrid_ds['latitude'].dims, name='latitude')
            lon_snapped_da = xr.DataArray(lon_snapped, dims=pre_regrid_ds['longitude'].dims, name='longitude')

            # mean brightness temperature per (time, latitude, longitude) grid cell.
            # expected_groups pins the full target grid as exact labels (isbin=False, the
            # default, since lat/lon are already snapped to exact grid values, not bins) --
            # this also fills any grid cell with no data as NaN, replacing the old .reindex()
            binned_mean = flox.xarray.xarray_reduce(
                pre_regrid_ds[btemp_varname],
                pre_regrid_ds['time'], lat_snapped_da, lon_snapped_da,
                func='nanmean',
                expected_groups=(None, latitude, longitude),
                fill_value=np.nan,
            )

            # only keep min value for the hour, ignoring cells with no data in a given 15-min slice
            with warnings.catch_warnings():
                warnings.simplefilter('ignore', category=RuntimeWarning)  # all-NaN cell across the hour
                result_da = binned_mean.min('time', skipna=True)

            result_ds = result_da.to_dataset(name=btemp_varname)
            # add time dimension
            result_ds = result_ds.expand_dims({'time': [pre_regrid_ds.time[0].values]})
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


def regrid_sat_files(path_list, sat_name, grid_res=cts.GRID_RESOLUTION,
                     grid_res_str=cts.GRID_RESOLUTION_STR,
                     generate_hists=True, generate_stats=True,
                     dir_list=False, overwrite=False,
                     remove_temp_abi_dir=False, result_dir_path=None,
                     print_debug=False, lat_min=cts.FPOUT_LAT_MIN, lat_max=cts.FPOUT_LAT_MAX,
                     lon_min=cts.FPOUT_LON_MIN, lon_max=cts.FPOUT_LON_MAX, rm_pre_regrid_file=False):
    """
    Function to regrid a list of hourly satellite data files to a specific grid resolution
    @param path_list: <list> [ <str> or <pathlib.Path>, ... ] list of files or daily directories to regrid
    @param sat_name: <str> name of the satellite (only 'GOES_GLM' and 'GOES_ABI' supported for now)
    @param grid_res: <float> grid resolution
    @param grid_res_str: <str> grid resolution str (to be added to the resulting filename)
    @param generate_hists: <bool> indicates if flash energy and flash area histograms should be generated for each grid cell
    @param generate_stats: <bool> indicates if flash energy and flash area stast should be generated for each grid cell
    @param dir_list: <bool> if True, list received is a list of directories containing data files, NOT a list of files
    @param overwrite: <bool> overwrite file if it already exists
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
    PathParser = get_PathParser(sat_name)
    if sat_name == cts.GOES_SATELLITE_ABI: # concat 15 min hdf files into hourly nc files
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
                generate_abi_hourly_nc_file_from_15min_hdf_files(dir_path_list=path_to_concat_into_hourly_files, print_debug=print_debug,
                                                                 remove_temp_files=remove_temp_abi_dir, overwrite=overwrite)
    elif sat_name in cts.SUPPORTED_LI_SATELLITES_LIST:
        pass # pass because concat into hourly files already done with bash script when retrieving GLM/MTG-LI data
    else:
        raise ValueError(
            f'{sat_name} {cts.SAT_VALUE_ERROR}')
    # if path_list contains paths to directories --> get list of files in each directory
    file_list = list(path_list)
    if dir_list:
        filename_pattern = generate_sat_filename_pattern(sat_name=sat_name, regrid=False, hourly=True) 
        # Get list of files in subdirectories
        file_list = [
            file_path

            for dir_path in sorted(path_list)
            for file_path in dir_path.glob(filename_pattern)
        ]
    if print_debug:
        print(f'{len(file_list)} files to regrid')
        print(f'{file_list=}')
        print()
    for pre_regrid_file_url in file_list:
        # get pre-regrid file start date (year, day, hour) with <sat>PathParser
        if print_debug:
            print('---')
            print(f'pre_regrid_file_url: {pre_regrid_file_url}')
        pre_regrid_path_parsed = PathParser(file_url=pre_regrid_file_url, regrid=False, hourly=True)
        pre_regrid_file_date = pre_regrid_path_parsed.get_start_date_pdTimestamp(ignore_missing_start_hour=False)
        # create result nc file path
        result_file_path = generate_sat_hourly_file_path(date=pre_regrid_file_date, sat_name=sat_name, regrid=True,
                                                         satellite=pre_regrid_path_parsed.satellite_version,
                                                         regrid_res_str=grid_res_str, dir_path=result_dir_path)
        # if directory/ies containing result nc file path does NOT exist --> create it/them
        if not result_file_path.parent.exists():
            result_file_path.parent.mkdir(parents=True, exist_ok=True)
            print(f"Creating directory {result_file_path.parent}")

        # check if regrid file exists and if it doesn't OR if overwrite == True --> "create it"
        if overwrite or not result_file_path.exists():
            print(f"\nGenerating hourly regrid file for: {pre_regrid_file_url}")
            if sat_name in cts.SUPPORTED_LI_SATELLITES_LIST:
                generate_lightning_sat_hourly_regrid_file(pre_regrid_file_url=pre_regrid_file_url,
                                                          sat_name=sat_name,
                                                          grid_res=grid_res,
                                                          generate_hists=generate_hists,
                                                          generate_stats=generate_stats,
                                                          overwrite=overwrite, result_file_path=result_file_path,
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
