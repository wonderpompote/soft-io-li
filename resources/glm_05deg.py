import os

import numpy as np
from pathlib import Path
from tqdm import tqdm
import xarray as xr

""" beurk beurk temporaire """
import sys
sys.path.append('/home/patj/SOFT-IO-LI/src/')
""" beurk beurk temporaire """
from utils import constants as cts # ..utils avant mais fonctionne pas
from utils import glm_utils, utils_functions, xarray_utils


def find_regrid_glm_file_list_between_min_max_date(min_date, max_date, regrid_glm_files_path=cts.GLM_REGRID_DIR_PATH,
                                                   daily_glm_files_dirname=cts.CONCAT_GLM_REGRID_DIR_NAME,
                                                   daily_regrid_file_name=cts.GLM_REGRID_DIR_NAME+cts.DDD_pattern+".nc"):
    """

    :param min_date:
    :param max_date:
    :param regrid_glm_files_path:
    :param daily_glm_files_dirname:
    :param daily_regrid_file_name:
    :return:
    """
    daily_regrid_glm_files_path = Path(f'{regrid_glm_files_path}/{min_date.dt.year.values}/{daily_glm_files_dirname}')
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
                hourly_files_path.glob(f'{glm_utils.generate_glm_hourly_nc_file_pattern(daily_file_date_int)}')
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
            hourly_files_path = hourly_regrid_glm_files_dirs / Path(f"GLM_array_05deg_{daily_file_date_int}")
            all_hourly_file_for_day = sorted(
                hourly_files_path.glob(f'{glm_utils.generate_glm_hourly_nc_file_pattern(daily_file_date_int)}')
            )
            # go through hourly regrid files for this specific day
            for h_file_index, hourly_file in enumerate(all_hourly_file_for_day):
                hourly_file_start_hour = int(glm_utils.get_start_hour_from_glm_hourly_regrid_filename(hourly_file))
                # if file.hour == max_date.start_hour
                if hourly_file_start_hour == max_date.dt.hour.values:
                    # add all files with hour <= max_date.hour
                    nc_file_list += all_hourly_file_for_day[:(h_file_index+1)]
                    break

        # if bigger day than max_date, stop for loop
        elif daily_file_date_int > max_date.dt.dayofyear.values:
            break
    return nc_file_list


def concat_glm_files_for_flight(nc_file_list, result_concat_file_path, overwrite=False):
    """

    :param nc_file_list:
    :param result_concat_file_path:
    :param overwrite:
    :return:
    """
    # <!> check pas si on a bien donné un truc en .nc
    if not isinstance(result_concat_file_path, Path):
        result_concat_file_path = Path(result_concat_file_path)
    if (not result_concat_file_path.exists()) or (result_concat_file_path.exists() and overwrite):
        result_concat_ds = xr.open_mfdataset(nc_file_list)
        if not result_concat_file_path.parent.exists():
            os.makedirs(result_concat_file_path.parent)
            print(f"Creating directory {result_concat_file_path.parent}")
        result_concat_ds.to_netcdf(result_concat_file_path,
                                   encoding={"time": {"dtype": 'float64', 'units': 'nanoseconds since 1970-01-01'}})
        print(f"Creating file: {result_concat_file_path.parts[-1]}")
    else:
        print(f'{result_concat_file_path} already exists')


def concat_hourly_nc_files_into_daily_file(root_dir=cts.GLM_REGRID_DIR_PATH, year='2018',
                                           dir_pattern=cts.GLM_REGRID_DIR_NAME+cts.DDD_pattern,
                                           result_dir_name=cts.CONCAT_GLM_REGRID_DIR_NAME,
                                           result_file_name=cts.GLM_REGRID_DIR_NAME):
    """
    Function to concatenate regridded hourly glm files stored in root_dir into bigger daily ones
    :param root_dir:
    :param year:
    :param dir_pattern:
    :param result_dir_name:
    :param result_file_name:
    :return:
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
        # check if concatenated file already exists
        # if not concatenate all hourly glm files to create a daily
        if not concat_file_name.exists():
            # OU sinon nco.Nco().ncrcat(input=file_list, output=concat_file_name, options=['-h'])
            # (mais fonctionne pas)
            with xr.open_mfdataset(file_list) as combined_ds:
                # save to netcdf
                combined_ds.to_netcdf(concat_file_name,
                                      encoding={"time": {"dtype": 'float64', 'units': 'nanoseconds since 1970-01-01'}})
                print(f'Creating file: {concat_file_name}')
                print('-----')
        else:
            print(f'{concat_file_name} already exists')
            print('-----')


def generate_hourly_regrid_glm_file(glm_ds_url, data_vars_dict, lon_min=-179.75, lon_max=180, lat_min=-89.75,
                                    lat_max=90,
                                    grid_resolution=0.5, regrid_result_root_path=cts.GLM_REGRID_DIR_PATH,
                                    regrid_daily_dir_name=cts.GLM_REGRID_DIR_NAME):
    """
    Function to open an hourly GLM file and grid the data into specific grids (by default 0.5° x 0.5°, same as FLEXPART output).
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
    :return:
    """
    """ <!> améliorations:
        - là on groupby latitude sur flash_energy mais on pourrait vouloir le faire sur autres trucs (genre group qchose)
            --> rendre ça + générique pour qu'on puisse récup plusieurs variables
            Parce que là du coup même le sum et le count sont fait sur le flash_energy grouped by
        - est-ce que vraiment besoin return le dataset ??? 
            --> pour concat oui
                --> non en fait, je vais faire le concat avec une liste de path ça évitera de se trimballer des gros ds dans une liste
        OK- pourrait check si file existe déjà et si oui PAS besoin de le créer
    """
    # get glm file day of year, start hour, end hour and year (taken from glm directory name)
    glm_ds_date = glm_utils.get_glm_hourly_file_date_from_filename(glm_ds_url, glm_utils.get_glm_daily_dir_date(glm_ds_url.parent)["year"])
    result_nc_file_path = glm_utils.generate_glm_05deg_hourly_nc_file_path(
        Path(f'{regrid_result_root_path}/{glm_ds_date["year"]}/{regrid_daily_dir_name}'),
        glm_ds_date["day_of_year"], glm_ds_date["start_hour"], glm_ds_date["end_hour"]
    )
    # if directory that will contain nc file does not exist -> create it
    if not result_nc_file_path.parent.exists():
        os.makedirs(result_nc_file_path.parent)
        print(f"Creating directory {result_nc_file_path}")

    # if glm 05 deg file does not already exist -> create it
    if not result_nc_file_path.exists():
        # generate dataset in which we'll be putting the summed flash_energy values
        latitudes = np.arange(lat_min, lat_max, grid_resolution)
        longitudes = np.arange(lon_min, lon_max, grid_resolution)
        date = utils_functions.get_np_datetime64_from_string(year=glm_ds_date["year"], day_of_year=glm_ds_date['day_of_year'],
                                                   hour=glm_ds_date['start_hour'])
        data_vars = {}
        for var in data_vars_dict:
            data_vars[var] = (['time', 'latitude', 'longitude'], np.zeros(shape=(1, len(latitudes), len(longitudes))))
        target_ds = xr.Dataset(
            data_vars=data_vars,
            coords={
                'time': [date],  # np.atleast_1d(date),
                'latitude': latitudes,
                'longitude': longitudes,
            })
        with xr.open_dataset(glm_ds_url) as glm_ds:
            # assign new longitude and latitude coords with chosen grid resolution using nearest method
            glm_ds_regrid_lon_lat = glm_ds.assign_coords({
                'latitude': target_ds.latitude.sel(latitude=glm_ds.flash_lat, method='nearest'),
                'longitude': target_ds.longitude.sel(longitude=glm_ds.flash_lon, method='nearest')
            })
            var_grouped_by_lat_da = glm_ds_regrid_lon_lat['flash_energy'].groupby('latitude')
            # for each latitude, group by longitude and apply operation
            for grouped_lat, da_for_lat in tqdm(var_grouped_by_lat_da):
                # group by longitude
                da_for_lat_grouped_by_lon = da_for_lat.groupby('longitude')
                for data_var_name in data_vars_dict:
                    da_operation = xarray_utils.apply_operation_on_grouped_da(
                        grouped_by_da=da_for_lat_grouped_by_lon,
                        operation=data_vars_dict[data_var_name]['operation'],
                        operation_dims=data_vars_dict[data_var_name]['operation_dims']
                    )
                    # add new values to target_ds for specific latitude and longitude(s)
                    target_ds[data_var_name].loc[
                        dict(latitude=grouped_lat, longitude=da_operation.longitude)] = da_operation
        # convert target ds to netCDF file (manual encoding to keep finer date value (by default converted to int64, units "days since 1970-01-01")
        target_ds.to_netcdf(result_nc_file_path,
                            encoding={"time": {"dtype": 'float64', 'units': 'nanoseconds since 1970-01-01'}})
        print(f"Created netcdf file {result_nc_file_path.parts[-1]}")

    else:  # file already exists so no need to create it again
        print(f"{result_nc_file_path} already exists")


def regrid_glm_files(glm_dir_url=cts.OG_GLM_FILES_PATH, glm_dir_pattern=cts.GLM_DIR_NAME + cts.YYYYDDD_PATTERN,
                     glm_hourly_file_pattern=glm_utils.generate_glm_hourly_nc_file_pattern(),
                     data_vars_to_regrid=cts.DEFAULT_GLM_DATA_VARS_TO_REGRID,
                     target_glm_05deg_dir=cts.GLM_REGRID_DIR_PATH, regrid_daily_dir_name=cts.GLM_REGRID_DIR_NAME,
                     lon_min=-179.75, lon_max=180, lat_min=-89.75, lat_max=90, grid_resolution=0.5):
    """
    Function to regrid hourly glm files, calls generate_hourly_regrid_glm_file() on a list of hourly glm netCDF files
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
                grid_resolution=grid_resolution
            )


if __name__ == '__main__':
    #regrid_glm_files()
    #concat_hourly_nc_files_into_daily_file()
    # flight003: '/o3p/macc/flexpart10.4/flexpart_v10.4_3d7eebf/src/exercises/soft-io-li/flight_2018_003_1h_05deg/10j_100k_output/grid_time_20180605210000.nc'
    fp_out_path = '/o3p/patj/SOFT-IO-LI/flexpart10.4/flexpart_v10.4_3d7eebf/src/exercises/soft-io-li/flight_2018_001_1h_05deg/10j_100k_output/grid_time_20180603150000.nc'
    with xr.open_dataset(fp_out_path).spec001_mr as fp_da:
        nc_file_list = find_regrid_glm_file_list_between_min_max_date(
            min_date=fp_da.time.min(),
            max_date=fp_da.time.max()
        )
        concat_glm_files_for_flight(
            nc_file_list=nc_file_list,
            result_concat_file_path='/o3p/patj/test-glm/flights_concat/GLM_flight_001.nc'
        )
