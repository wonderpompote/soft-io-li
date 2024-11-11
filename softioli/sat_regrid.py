from datetime import datetime
import numpy as np
import pathlib
from shutil import rmtree
import xarray as xr

from utils import GLMPathParser, generate_sat_hourly_file_path, generate_sat_filename_pattern, generate_sat_dirname_pattern, open_hdf4, ABIPathParser, get_abi_coords_file
from utils import constants as cts
from utils.constants import SAT_SETTINGS, raw_lat_cname, raw_lon_cname, flash_area_varname, flash_energy_varname, \
    attrs_to_keep
from utils import xarray_pandas_utils as xr_pd_utils


def generate_lightning_sat_hourly_regrid_file(pre_regrid_file_url, sat_name, grid_res, grid_res_str, overwrite,
                                              lat_min=cts.FPOUT_LAT_MIN, lat_max=cts.FPOUT_LAT_MAX,
                                              lon_min=cts.FPOUT_LON_MIN, lon_max=cts.FPOUT_LON_MAX,
                                              result_dir_path=None, naming_convention=None):
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
    :param result_dir_path: <str> or <pathlib.Path>
    :param naming_convention: <str> pre-regrid file naming convention (useful for backward compatibility). Supported values: 'OLD_TEMP', 'OLD', None (default)
    """
    if not sat_name in cts.SAT_SETTINGS:
        raise ValueError(f'{sat_name} {cts.SAT_VALUE_ERROR}')
    if sat_name == cts.GOES_SATELLITE_GLM:
        SatPathParser = GLMPathParser

    # get pre-regrid file start date (year, day, hour) with <sat>PathParser
    pre_regrid_path_parsed = SatPathParser(file_url=pre_regrid_file_url, regrid=False,
                                           naming_convention=naming_convention)
    pre_regrid_file_date = pre_regrid_path_parsed.get_start_date_pdTimestamp(ignore_missing_start_hour=False)
    # create result nc file path
    result_dir_path = generate_sat_hourly_file_path(date=pre_regrid_file_date, sat_name=sat_name, regrid=True,
                                                    satellite=pre_regrid_path_parsed.satellite_version,
                                                    regrid_res_str=grid_res_str, dir_path=result_dir_path)
    # if directory/ies containing result nc file path does NOT exist --> create it/them
    if not result_dir_path.parent.exists():
        result_dir_path.parent.mkdir(parents=True)
        print(f"Creating directory {result_dir_path.parent}")

    # check if regrid file exists and if it doesn't OR if overwrite == True --> "create it"
    if not result_dir_path.exists() or (result_dir_path.exists() and overwrite):
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
            count_ds['flash_count'].attrs['long_name'] = f'Number of flash occurrences in a {grid_res}° x {grid_res}° x 1h grid cell'
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
            target_ds = xr.merge([count_ds, flash_en_hist_ds, flash_area_hist_ds, target_ds], combine_attrs='no_conflicts')
        # add pre-regrid file date to regrid date + add regrid file creation date attr
        target_ds = target_ds.expand_dims({'time': [pre_regrid_file_date]})
        target_ds.attrs['regrid_file_creation_date'] = datetime.now().isoformat()
        # TODO: réduire units de l'heure pour prendre moins de place (pas besoin de nanoseconds en soit)
        target_ds.to_netcdf(
            path=result_dir_path, mode='w',
            encoding={"time": {"dtype": 'float64', 'units': 'nanoseconds since 1970-01-01'}}
        )
        print(f"Created netcdf file {result_dir_path}")

    else:  # file already exists so no need to create it again
        print(f"{result_dir_path} already exists")



def generate_cloud_temp_sat_hourly_regrid_file(pre_regrid_file_url, sat_name, grid_res, grid_res_str, overwrite,
                                               lat_min=cts.FPOUT_LAT_MIN, lat_max=cts.FPOUT_LAT_MAX,
                                               lon_min=cts.FPOUT_LON_MIN, lon_max=cts.FPOUT_LON_MAX,
                                               result_dir_path=None):
    """
    Gère un seul fichier à la fois et il faut:
    1- recup SatPathParser
    2- pre_regrid_file_parsed = SatPathParser(pre_regrid_file_url, regrid=False, hourly=True)
    3- create result_dir_path = generate_sat_hourly_file_path(date=pre_regrid_file_date, sat_name=sat_name, regrid=True, satellite=pre_regrid_path_parsed.satellite_version, regrid_res_str=grid_res_str, dir_path=result_dir_path)
    4- if parent result dir does not exist --> create it:
    if not result_dir_path.parent.exists(): # TODO: pourquoi .parent ???
        result_dir_path.parent.mkdir(parents=True)
        print(f"Creating directory {result_dir_path.parent}")
    5- if result
    TODO: generic function pour regrid longitude et latitudes + création dossier parents et tout (je pense que ça peut le faire si on va jusqu'à truc reset_coords
    TODO: <!> les cas où j'ai
    ensuite, une fois que j'ai les bonnes latitudes et longitudes
    --> PBM = quand j'ai deux satellites, j'ai la dimension "satellite"
    """
    pass

def generate_abi_hourly_nc_file_from_15min_hdf_files(path_list, remove_temp_files=False):
    # pour chaque daily dir
    for dir_p in path_list:
        dir_date = ABIPathParser(file_url=dir_p, regrid=False, directory=True)
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
            h_abi_ds_list = []
            for h_file in h_file_list:
                # open file + rename col names to coorespond to coords_ds col names
                with open_hdf4(str(h_file)).rename(dict(NbLines='Nlin', NbColumns='Ncol')) as abi_ds_wout_coords:
                    print(abi_ds_wout_coords)
                    # get corresponding coords file_path
                    h_file_parser = ABIPathParser(file_url=h_file, regrid=False, hourly=False)
                    coords_file_path = get_abi_coords_file(sat_version=h_file_parser.satellite, file_version=h_file_parser.version)
                    # combine coords dataset with abi dataset
                    with open_hdf4(str(coords_file_path)) as coords_ds:
                        b_temp_w_coords_ds = coords_ds.assign(Brightness_Temperature=abi_ds_wout_coords.Brightness_Temperature)
                        # add file timestamp
                        b_temp_w_coords_ds = b_temp_w_coords_ds.expand_dims({
                            'time': [h_file_parser.get_start_date_pdTimestamp(ignore_missing_start_hour=False).to_datetime64()],
                            'satellite': [h_file_parser.satellite]
                        })
                        h_abi_ds_list.append(b_temp_w_coords_ds[['Latitude', 'Longitude', 'Brightness_Temperature']])
            h_abi_ds = xr.merge(h_abi_ds_list)
            result_hourly_filename = generate_sat_hourly_file_path(
                                                    date=h_file_parser.start_date,
                                                    sat_name=cts.GOES_SATELLITE_ABI, satellite=h_file_parser.satellite,
                                                    regrid=False, dir_path=None)
            h_abi_ds.to_netcdf(
                path=result_hourly_filename, mode='w',
                encoding={"time": {"dtype": 'float64', 'units': 'nanoseconds since 1970-01-01'}}
            )
            print(f"Saved {result_hourly_filename}")
        if remove_temp_files:
            rmtree(pathlib.Path(f'{dir_p}/temp'))
            print(f"Deleting {dir_p}/temp directory")




def regrid_sat_files(path_list, sat_name, grid_res=cts.GRID_RESOLUTION,
                     grid_res_str=cts.GRID_RESOLUTION_STR, dir_list=False, overwrite=False,
                     result_dir_path=None, naming_convention=None, remove_temp_abi_dir=False):
    """
    Function to regrid a list of hourly satellite data files to a specific grid resolution
    :param path_list: <list> [ <str> or <pathlib.Path>, ... ] list of files or daily directories to regrid
    :param sat_name: <str> name of the satellite (only 'GOES_GLM' and 'GOES_ABI' supported for now)
    :param grid_res: <float> grid resolution
    :param grid_res_str: <str> grid resolution str (to be added to the resulting filename)
    :param dir_list: <bool> if True, list received is a list of directories containing data files, NOT a list of files
    :param overwrite: <bool> overwrite file if it already exists
    :param result_dir_path: <pathlib.Path> or <str> mostly for testing, directory in which resulting file should be stored, if None --> use default path
    :param naming_convention: <str> file or directory naming convention (mostly for backward compatibility). Supported values: 'OLD_TEMP', 'OLD' or None (default)
    :return:
    """
    if sat_name == cts.GOES_SATELLITE_ABI and dir_list:
        path_to_concat_into_hourly_files = []
        hourly_pre_regrid_nc_file_pattern = generate_sat_filename_pattern(sat_name=cts.GOES_SATELLITE_ABI, regrid=False, hourly=True)
        for p in path_list:
            p = pathlib.Path(p)
            # if temp dir exists and not all hourly pre regrid nc files available
            if pathlib.Path(f'{p}/temp').exists() and len(sorted(p.glob(hourly_pre_regrid_nc_file_pattern))) != 24:
                path_to_concat_into_hourly_files.append(p)
        if len(path_to_concat_into_hourly_files) > 0: #concat 15min hdf files into hourly nc files
            generate_abi_hourly_nc_file_from_15min_hdf_files(path_list=hourly_pre_regrid_nc_file_pattern, remove_temp_files=remove_temp_abi_dir)
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
    for pre_regrid_file_url in path_list:
        if sat_name == cts.GOES_SATELLITE_GLM:
            print(f"\nGenerating hourly regrid file for: {pre_regrid_file_url}")
            generate_lightning_sat_hourly_regrid_file(pre_regrid_file_url=pre_regrid_file_url,
                                                      sat_name=sat_name,
                                                      grid_res=grid_res, grid_res_str=grid_res_str,
                                                      overwrite=overwrite, result_dir_path=result_dir_path,
                                                      naming_convention=naming_convention)
        elif sat_name == cts.GOES_SATELLITE_ABI:
            pass
        else:
            raise ValueError(
                f'{sat_name} {cts.SAT_VALUE_ERROR}')
