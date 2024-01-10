"""
glm_regrid.py
Main functions:
- regrid hourly GLM files into target grid hourly GLM files
- concat hourly regrid GLM files into a daily GLM file
- 
Last docstring update: 27/09/23

Future ameliorations:
- add attributes to regridded data variables
- .filna(0) for zone covered by GOES satellite (nan for the rest)
- deal with GOES-E and GOES-W data
- add more descriptive attributes to the regridded dataset itself
- complete file docstring properly

Last amelioration docstring update: 28/11/23
"""


from datetime import datetime
import numpy as np
import os
from pathlib import Path
import xarray as xr

from utils import constants as cts
from utils import glm_utils, utils_functions, xarray_utils


def find_regrid_glm_file_list_between_min_max_date(min_date, max_date, regrid_glm_files_path=cts.GLM_REGRID_DIR_PATH,
                                                   daily_glm_files_dirname=cts.CONCAT_GLM_REGRID_DIR_NAME,
                                                   daily_regrid_file_name=cts.GLM_REGRID_DIR_NAME + cts.DDD_pattern + ".nc"):
    """
    Function to get a list of all GLM files (previously regridded) between min and max date
    :param min_date: <xr.DataArray> (<numpy.datetime64>)
    :param max_date: <xr.DataArray> (<numpy.datetime64>)
    :param regrid_glm_files_path: <Path>
    :param daily_glm_files_dirname: <str>
    :param daily_regrid_file_name: <str>
    :return: list of GLM files (Path objects) between min and max date
    :rtype: <list> [ <Path>, ..., <Path> ]
    """
    daily_regrid_glm_files_path = Path(f'{regrid_glm_files_path}/{min_date.dt.year.values}/{daily_glm_files_dirname}')
    # get sorted list of all daily GLM files in directory
    all_daily_file_list = sorted(Path(daily_regrid_glm_files_path).glob(daily_regrid_file_name))
    hourly_regrid_glm_files_dirs = Path(f'{regrid_glm_files_path}/{min_date.dt.year.values}')
    nc_file_list = []
    for daily_file in sorted(all_daily_file_list):
        daily_file_date_int = int(glm_utils.get_day_of_year_from_glm_daily_regrid_filename(daily_file))
        # if daily_file date between min and max --> append daily file to nc_file_list
        if min_date.dt.dayofyear.values < daily_file_date_int < max_date.dt.dayofyear.values:
            nc_file_list.append(daily_file)

        # if file.day_of_year == min_date.day_of_year --> only append hourly files starting from min date hour
        elif daily_file_date_int == min_date.dt.dayofyear.values:
            """ <!> revoir ce path là et utiliser une de mes petites fonctions """
            hourly_files_path = Path(f'{hourly_regrid_glm_files_dirs}/GLM_array_05deg_{daily_file_date_int}')
            all_hourly_file_for_day = sorted(
                hourly_files_path.glob(
                    f'{glm_utils.generate_glm_hourly_nc_file_pattern(daily_file_date_int, regrid_str=cts.REGRID_STR)}')
            )
            # go through hourly regrid files for this specific day
            for h_file_index, hourly_file in enumerate(all_hourly_file_for_day):
                hourly_file_start_hour = int(glm_utils.get_start_hour_from_glm_hourly_regrid_filename(hourly_file))
                # if file.hour == min_date.start_hour
                if hourly_file_start_hour == min_date.dt.hour.values:
                    # add all files with hours >= min_date.hour
                    nc_file_list += all_hourly_file_for_day[h_file_index:]
                    break

        # if file.day_of_year == max_date.day_of_year --> only append hourly files before max date hour
        elif daily_file_date_int == max_date.dt.dayofyear.values:
            hourly_files_path = hourly_regrid_glm_files_dirs / Path(f"{cts.GLM_REGRID_DIR_NAME}{daily_file_date_int}")
            all_hourly_file_for_day = sorted(
                hourly_files_path.glob(
                    f'{glm_utils.generate_glm_hourly_nc_file_pattern(daily_file_date_int, regrid_str=cts.REGRID_STR)}')
            )
            # go through hourly regrid files for this specific day
            for h_file_index, hourly_file in enumerate(all_hourly_file_for_day):
                hourly_file_start_hour = int(glm_utils.get_start_hour_from_glm_hourly_regrid_filename(hourly_file))
                # if file.hour == max_date.start_hour
                if hourly_file_start_hour == max_date.dt.hour.values:
                    # add all files with hour <= max_date.hour
                    nc_file_list += all_hourly_file_for_day[:(h_file_index + 1)]
                    break

        # if bigger day than max_date, stop for loop
        elif daily_file_date_int > max_date.dt.dayofyear.values:
            break

    return nc_file_list


def concat_glm_files_for_flexpart_out(nc_file_list, result_concat_file_path, overwrite=False):
    """
    Concat all GLM files for duration of a FLEXPART simulation (usually 10 days)
    and saves it into a single netCDF file
    <!> the resulting dataset is NOT returned but saved as a .nc file <!>
    :param nc_file_list:
    :param result_concat_file_path:
    :param overwrite: if True, existing file will be overwritten
    """
    # <!!> check pas si on a bien donné un truc en .nc
    if not isinstance(result_concat_file_path, Path):
        result_concat_file_path = Path(result_concat_file_path)
    if (not result_concat_file_path.exists()) or (result_concat_file_path.exists() and overwrite):
        result_concat_ds = xr.open_mfdataset(nc_file_list)
        if not result_concat_file_path.parent.exists():
            os.makedirs(result_concat_file_path.parent)
            print(f"Creating directory {result_concat_file_path.parent}")
        result_concat_ds.to_netcdf(
            path=result_concat_file_path, mode="w",
            encoding={"time": {"dtype": 'float64', 'units': 'nanoseconds since 1970-01-01'}}
        )
        print(f"Creating file: {result_concat_file_path.parts[-1]}")
    else:
        print(f'{result_concat_file_path} already exists')


def concat_hourly_nc_files_into_daily_file(root_dir=cts.GLM_REGRID_DIR_PATH, year='2018',
                                           dir_pattern=cts.GLM_REGRID_DIR_NAME + cts.DDD_pattern,
                                           result_dir_name=cts.CONCAT_GLM_REGRID_DIR_NAME,
                                           result_file_name=cts.GLM_REGRID_DIR_NAME,
                                           overwrite=False):
    """
    Function to concatenate regridded hourly glm files stored in root_dir into daily files
    :param root_dir:
    :param year:
    :param dir_pattern:
    :param result_dir_name:
    :param result_file_name:
    :param overwrite: if True, existing file will be overwritten
    """
    # <!> PAS de file_pattern parce qu'on utilise generate glm pattern
    #    --> MAIS si on met concat ailleurs va falloir changer ça pour qu'on ait des patterns adapté aux données
    # idem pour dir_path_day
    root_dir_path = Path(f'{root_dir}/{year}/')
    dir_list = sorted(Path(root_dir_path).glob(dir_pattern))
    # loop through dir list
    for dir_path in dir_list:
        dir_path_day = str(dir_path).split('_')[-1]
        pattern = glm_utils.generate_glm_hourly_nc_file_pattern(day_of_year=dir_path_day, regrid_str="05deg_")
        file_list = sorted(dir_path.glob(pattern))
        concat_file_name = Path(f'{root_dir_path}/{result_dir_name}/{result_file_name}{str(dir_path_day)}.nc')
        # check if dir containing result concatenated files exists
        if not concat_file_name.parent.exists():
            os.makedirs(concat_file_name.parent)
            print(f"Creating directory {concat_file_name.parent}")
        # check if concatenated file already exists OR if we want to overwrite it
        # if not concatenate all hourly glm files to create a daily
        if (not concat_file_name.exists()) or (concat_file_name.exists() and overwrite):
            with xr.open_mfdataset(file_list) as combined_ds:
                # save to netcdf
                combined_ds.to_netcdf(concat_file_name,
                                      encoding={"time": {"dtype": 'float64', 'units': 'nanoseconds since 1970-01-01'}})
                print(f'Creating file: {concat_file_name}')
                print('-----')
        else:
            print(f'{concat_file_name} already exists')
            print('-----')


#### lon_min used to be -179.75 and lon_max 180 (to have lon between -179.75 and -179.75) BUT FP out lon between -179.25 and 180.25
def generate_hourly_regrid_glm_file(glm_ds_url, data_vars_dict,
                                    lon_min=-179.25, lon_max=180.75,
                                    lat_min=-89.75, lat_max=90, grid_resolution=0.5,
                                    regrid_result_root_path=cts.GLM_REGRID_DIR_PATH,
                                    regrid_daily_dir_name=cts.GLM_REGRID_DIR_NAME,
                                    overwrite=False):
    """
    Function to open hourly GLM file and grid the data into specific grid (by default 0.5° x 0.5°, same as FLEXPART output)
    The resulting dataset contains the given GLM data variables gridded according to the given grid resolution
        + a new date coordinate corresponding to the given GLM file date
    :param glm_ds_url: path to glm file that needs to be regridded
    :param data_vars_dict: <dict> for each variable: name the operation to be applied and dimensions on which the operation should be applied
    :param lon_min: new grid minimum longitude
    :param lon_max: new grid maximum longitude
    :param lat_min: new grid minimum latitude
    :param lat_max: new grid maximum latitude
    :param grid_resolution: new grid resolution
    :param regrid_result_root_path: root directory containing all the regridded glm files and directories
    :param regrid_daily_dir_name: name of the directory in which the resulting hourly file will be stored
    :param overwrite: if True, existing file will be overwritten
    """
    if not isinstance(glm_ds_url, Path):
        glm_ds_url = Path(glm_ds_url)
    # get glm file day of year, start hour, end hour and year (taken from glm directory name)
    glm_ds_date = glm_utils.get_glm_hourly_file_date_from_filename(
        glm_filename=glm_ds_url,
        year=glm_utils.get_glm_daily_dir_date(glm_ds_url.parent)["year"]
    )
    # create result nc file path
    result_nc_file_path = glm_utils.generate_glm_regrid_hourly_nc_file_path(
        Path(f'{regrid_result_root_path}/{glm_ds_date["year"]}/{regrid_daily_dir_name}'),
        glm_ds_date["day_of_year"], glm_ds_date["start_hour"], glm_ds_date["end_hour"]
    )

    # if directory that will contain nc file does not exist -> create it
    if not result_nc_file_path.parent.exists():
        os.makedirs(result_nc_file_path.parent)
        print(f"Creating directory {result_nc_file_path}")

    # if regridded glm file does not already exist (or if exists but overwrite == True) -> create it
    if (not result_nc_file_path.exists()) or (result_nc_file_path.exists() and overwrite):
        # generate dataset with correctly gridded longitudes and latitudes
        date = utils_functions.get_np_datetime64_from_string(
            year=glm_ds_date["year"],
            day_of_year=glm_ds_date['day_of_year'],
            hour=glm_ds_date['start_hour']
        )
        target_ds = xr.Dataset(
            coords={
                'latitude': np.arange(lat_min, lat_max, grid_resolution),
                'longitude': np.arange(lon_min, lon_max, grid_resolution)
            },
            attrs={
                'grid_resolution': f'{grid_resolution}° x {grid_resolution}°'
            }
        )

        with xr.open_dataset(glm_ds_url) as _glm_ds:
            # assign new longitude and latitude coords with chosen grid resolution using nearest method
            _ds_assigncoords_lonlat = _glm_ds.assign_coords({
                'latitude': target_ds.latitude.sel(latitude=_glm_ds.flash_lat, method='nearest'),
                'longitude': target_ds.longitude.sel(longitude=_glm_ds.flash_lon, method='nearest')
            })
            # keep several attributes from the original glm file
            target_ds.assign_attrs({
                'production_site': _glm_ds.attrs.get('production_site', ''),
                'orbital_slot': _glm_ds.attrs.get('orbital_slot', ''),
                'platform_ID': _glm_ds.attrs.get('platform_ID', ''),
                'instrument_type': _glm_ds.attrs.get('instrument_type', ''),
                'instrument_ID': _glm_ds.attrs.get('instrument_ID', ''),
                'spatial_resolution': _glm_ds.attrs.get('spatial_resolution', ''),
                'glm_data_procesing_level': _glm_ds.attrs.get('processing_level', '')
            })
            
            # for each operation on each data variable
            for data_var in data_vars_dict:
                 for op in data_vars_dict[data_var]['operation']:
                    # only keep interesting variable and coords
                    _ds = _ds_assigncoords_lonlat[data_var] \
                        .reset_coords(names=['latitude', 'longitude'], drop=False) \
                        .reset_coords(drop=True)
                    # histogram
                    if op.lower() == "histogram":
                        hist_params = data_vars_dict[data_var]['histogram']
                        _ds[f'log_{data_var}'] = np.log10(_ds[data_var])
                        # --> call histogram function
                        res_ds = xarray_utils.histogram_using_pandas(_ds, f'log_{data_var}', hist_params)
                    # count
                    elif op.lower() == 'count':
                        res_ds = xarray_utils.count_using_pandas(_ds, data_var, data_vars_dict[data_var]['count'])
                    else:
                        raise ValueError(f'Unexpected operation name ({op}, operations supported: "histogram", "count"')
                    # merge resulting ds with target ds --> puts nans for missing latitude and longitude values
                    target_ds = xr.merge([res_ds, target_ds], combine_attrs='no_conflicts')

        # add date to target_ds
        target_ds = target_ds.expand_dims({'time': [date]})
        # add creation date to dataset attributes
        target_ds.attrs['regrid_file_creation_date'] = datetime.now().isoformat()
        # convert target ds to netCDF file, manual encoding to keep finer date value
        # (otherwise converted by default to int64, units "days since 1970-01-01")
        target_ds.to_netcdf(
            path=result_nc_file_path,
            mode='w',
            encoding={"time": {"dtype": 'float64', 'units': 'nanoseconds since 1970-01-01'}}
        )
        print(f"Created netcdf file {result_nc_file_path.parts[-1]}")
        return target_ds.attrs

    else:  # file already exists so no need to create it again
        print(f"{result_nc_file_path} already exists")
        return xr.open_dataset(result_nc_file_path)


#### lon_min used to be -179.75 and lon_max 180 (to have lon between -179.75 and -179.75) BUT FP out lon between -179.25 and 180.25
def regrid_glm_files(glm_dir_url=cts.OG_GLM_FILES_PATH, glm_dir_pattern=cts.GLM_DIR_NAME+cts.YYYYDDD_PATTERN,
                     glm_hourly_file_pattern=glm_utils.generate_glm_hourly_nc_file_pattern(),
                     data_vars_to_regrid=cts.DEFAULT_GLM_DATA_VARS_TO_REGRID,
                     target_glm_05deg_dir=cts.GLM_REGRID_DIR_PATH, regrid_daily_dir_name=cts.GLM_REGRID_DIR_NAME,
                     lon_min=-179.25, lon_max=180.75, lat_min=-89.75, lat_max=90, grid_resolution=0.5, overwrite=False):
    """
    Function to regrid hourly glm file
    calls generate_hourly_regrid_glm_file() on a list of hourly glm netCDF files
    :param glm_dir_url:
    :param glm_dir_pattern:
    :param glm_hourly_file_pattern:
    :param data_vars_to_regrid:
    :param target_glm_05deg_dir:
    :param regrid_daily_dir_name:
    :param lon_min:
    :param lon_max:
    :param lat_min:
    :param lat_max:
    :param grid_resolution:
    :param overwrite: if True, existing file will be overwritten
    :return:
    """
    # looking for all glm directories with names following pattern: OR_GLM-L2-LCFA_G16_sYYYYDDD
    glm_dir_list = sorted(Path(glm_dir_url).glob(glm_dir_pattern))
    # for each directory, get all glm files and call generate_hourly_regrid_glm_file
    for dir_path in glm_dir_list:
        glm_file_list = sorted(dir_path.glob(glm_hourly_file_pattern))
        for glm_file in glm_file_list:
            print(glm_file)
            generate_hourly_regrid_glm_file(
                glm_ds_url=glm_file,
                data_vars_dict=data_vars_to_regrid,
                regrid_result_root_path=target_glm_05deg_dir,
                regrid_daily_dir_name=regrid_daily_dir_name,
                lon_min=lon_min, lon_max=lon_max, lat_min=lat_min, lat_max=lat_max,
                grid_resolution=grid_resolution,
                overwrite=overwrite
            )
            print('-----')


if __name__ == '__main__':

    data_vars_dict = {
        "flash_energy": {
            "operation": ['histogram', 'count'],
            "histogram": {
                "min_bin_edge": -15,
                "max_bin_edge": -10,
                "step": 0.1,
                "res_var_name": "flash_energy_log_hist"
            },
            "count": {
                "res_var_name": "flash_count"
            }
        },
        "flash_area": {
            "operation": ['histogram'],
            "histogram": {
                "min_bin_edge": 1.5,
                "max_bin_edge": 4.5,
                "step": 0.1,
                "res_var_name": "flash_area_log_hist"
            }
        }
    }
    
    # regrid all glm files in og glm file path
    #regrid_glm_files(data_vars_to_regrid=data_vars_dict, overwrite=True)
    
    # concat all houry glm files into daily files
    concat_hourly_nc_files_into_daily_file(overwrite=True)

    # regrid one single file
    """glm_path = '/o3p/macc/glm/OR_GLM-L2-LCFA_G16_s2018156/GLM_array_156_19-20.nc'
    test_ds = generate_hourly_regrid_glm_file(
        glm_ds_url=glm_path,
        data_vars_dict=data_vars_dict,
        regrid_result_root_path='/o3p/patj/test-glm/hist_regrid',
        regrid_daily_dir_name='GLM_regrid_hist_attrs_05',
        overwrite=True
    )"""

    # create GLM file for a particular FP out
    fp_out_path_dic = {
        "flight_001": '/o3p/patj/SOFT-IO-LI/flexpart10.4/flexpart_v10.4_3d7eebf/src/exercises/soft-io-li/flight_2018_001_1h_05deg/10j_100k_output/grid_time_20180603150000.nc',
        "flight_003": '/o3p/macc/flexpart10.4/flexpart_v10.4_3d7eebf/src/exercises/soft-io-li/flight_2018_003_1h_05deg/10j_100k_output/grid_time_20180605210000.nc',
        "flight_006": '/o3p/patj/SOFT-IO-LI/flexpart10.4/flexpart_v10.4_3d7eebf/src/exercises/soft-io-li/flight_2018_006_1h_05deg/10j_100k_output/grid_time_20180607150000.nc',
        
    }

    for flight in fp_out_path_dic:
        with xr.open_dataset(fp_out_path_dic[flight]).spec001_mr as fp_da:
            print(f"min_date {flight}: {fp_da.time.min().values} --> {fp_da.time.min().dt.dayofyear.values}")
            print(f"max_date {flight}: {fp_da.time.max().values} --> {fp_da.time.max().dt.dayofyear.values}")
            nc_file_list = find_regrid_glm_file_list_between_min_max_date(
                min_date=fp_da.time.min(),
                max_date=fp_da.time.max()
            )
            concat_glm_files_for_flexpart_out(
                nc_file_list=nc_file_list,
                result_concat_file_path=f'/o3p/patj/glm/flights_glm/GLM_{flight}.nc',
                overwrite=False
            )
    
