import warnings

import numpy as np
import pandas as pd
import xarray as xr
from tqdm import tqdm
from datetime import datetime
from pathlib import Path

import fpout


############## will eventually be imported properly from utils etc. ##############
# déjà dans utils/xarray_utils.py MAIS pas encore fait les imports proprement
def check_all_dims_in_ds_or_da(dims_to_check, da_or_ds):
    """
    Function to check if given dim(s) are in dataArray dimensions
    :param dims_to_check: <str> or Iterable object of <str>, dmensions to check
    :param da_or_ds: <xarray.DataArray> or <xarray.Dataset>
    :return: <bool> True if all dimensions given are in the dataArray or dataset, False otherwise
    """
    # if iterable, check if filled with str
    if hasattr(dims_to_check, '__iter__') and not all(isinstance(dim, str) for dim in dims_to_check):
        raise TypeError(f'Expecting Iterable of strings')
    # if not iterable, check if str
    elif not isinstance(dims_to_check, str):
        raise TypeError(f'Expecting str or Iterable, not {type(dims_to_check)}')
    # check that we're given a dataArray or dataset
    if not isinstance(da_or_ds, xr.Dataset) and not isinstance(da_or_ds, xr.DataArray):
        raise TypeError(f'Expecting <xarray.Dataset> or <xarray.DataArray> object, not {type(da_or_ds)}')
    if isinstance(dims_to_check, str):
        return dims_to_check in da_or_ds.sizes
    else:
        return all(dim in da_or_ds.sizes for dim in dims_to_check)


""" AMELIORATIONS: ??
Check values to make sure they're in the right range --> maybe already done by datetime.strptime"""


# déjà dans utils/utilitaires.py MAIS pas encore fait les imports proprement
def get_np_datetime64_from_string(year, day_of_year, hour=0, mins=0, secs=0, month=None, day=None):
    """
    Function to generate a np datetime64 object with specific year, day of year (or day + month), hour, min and sec
    from a string using datetime.strptime (for now only way to get date from a day of year string)
    Expecting str or int values
    Used to get the date (taken from the glm nc filename) that will be added to the target 0.5deg glm dataset
    :param year: year
    :param day_of_year: day of the year
    :param hour: hour, default = 0
    :param mins: minutes, default = 0
    :param secs: seconds, default = 0
    :param month: month number (given if day_of_year is unknown and put to 0 or None)
    :param day: day of the month (given if day_of_year is unknown and put to 0 or None)
    :return: <numpy.datetime64>
    """
    # if day_of_year, month and day are all None raise exception
    if all(d is None for d in [day_of_year, month, day]) or all(d == 0 for d in [day_of_year, month, day]):
        raise ValueError(f'Day of year or month and day values required')
    if day_of_year is not None or day_of_year != 0:
        # if day_of_year AND month and day are all not None, raise warning and use day_of_year
        if all(d is not None for d in [month, day]):
            warnings.warn('Expecting day of the year OR month and day values, not both. Day of year value will be used',
                          Warning)
        date_iso_str = datetime.strptime(f'{year}-{day_of_year}-{hour}-{mins}-{secs}', '%Y-%j-%H-%M-%S').isoformat()
    elif day_of_year is None and all(d is not None for d in [month, day]):
        date_iso_str = datetime.strptime(f'{year}-{day_of_year}-{hour}-{mins}-{secs}', '%Y-%j-%H-%M-%S').isoformat()
    else:
        raise TypeError('Day_of_year value equal to None and month or day value missing')
    return np.datetime64(date_iso_str)


# déjà dans utils/xarray_utils.py MAIS pas encore fait les imports proprement
def apply_operation_on_grouped_da(grouped_by_da, operation, operation_dims=None):
    """
    Function to apply a specific operation on a given grouped by dataArray over given dimensions
    Currently accepted operations:
    - 'sum': will apply sum() function on the DataArrayGroupBy object over given dimensions
    - 'count': will apply count() function on the DataArrayGroupBy over given dimensions
    :param grouped_by_da: <xr.core.groupby.DataArrayGroupBy> <!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!>
    :param operation: <str> expecting 'sum' or 'count'
    :param operation_dims: <str> or Iterable of dimension names
    :return: result of the operation
    """
    # check input
    if not isinstance(grouped_by_da, xr.core.groupby.DataArrayGroupBy):
        raise TypeError(f'Expecting <xr.core.groupby.DataArrayGroupBy> not {type(grouped_by_da)}')
    if (operation_dims is not None) and (not check_all_dims_in_ds_or_da(grouped_by_da, operation_dims)):
        raise AttributeError(f'DataArray does not have {operation_dims} in its dimensions')
    # operation
    if operation.lower() == 'sum':
        return grouped_by_da.sum(operation_dims)
    elif operation.lower() == 'count':
        return grouped_by_da.count(operation_dims)
    else:
        raise ValueError(f'{operation} operation not supported, expecting \'sum\' or \'count\'')


# already in utils/fp_utils.py but not imported properly
def get_min_max_fp_output_date(fp_out_ds):
    """
    Function to get the min and max date from a FLEXPART output dataset
    :param fp_out_ds: <xarray.Dataset> FP output dataset
    :return: <tuple> (<numpy.datetime64>, <numpy.datetime64>)
    """
    return fp_out_ds.spec001_mr.time.min().values, fp_out_ds.spec001_mr.time.max().values


# already in utils/utilitaires.py but not imported properly
def np_datetime64_to_datetime(np_date):
    """
    Function to convert a numpy.datetime64 object to a datetime.datetime object using pandas.to_datetime function
    :param np_date: date to convert
    :type np_date: <numpy.datetime64> or <xarray.DataArray> if value contained is <numpy.datetime64>
    :return: converted date
    :rtype: <pandas.Timestamp> (pandas equivalent of <datetime.datetime>)
    """
    if isinstance(np_date, xr.DataArray):
        np_date = np_date.values
    if isinstance(np_date, np.datetime64):
        return pd.to_datetime(np_date)
    else:
        raise TypeError("wrong date format, expecting np.datetime64 object")


# already in utils/glm_utils.py but not imported properly
def get_glm_format_date_from_datetime(date_to_split):
    """
    Funtion returning a dictionary containing year, day_of_year and hour from a datetime object
    Used for glm file manipulations (with day of year)
    :param date_to_split: <datetime.datetime> date to split
    :return: <dict> { "year": <str>, "day_of_year": <str>, "hour": <str> }
    """
    # convert datetime object to str with format YYYY-DDD-HH with YYYY: year, DDD: day of year, HH: hour
    date_str = date_to_split.strftime('%Y-%j-%H')
    date_split = date_str.split('-')
    return {
        "year": date_split[0],
        "day_of_year": date_split[1],
        "hour": date_split[2]
    }


GLM_DIR_NAME = "OR_GLM-L2-LCFA_G16_s"  # OR_GLM-L2-LCFA_G16_sYYYYDDD
GLM_HOURLY_FILE_NAME = "GLM_array_"  # GLM_array_DDD_HH1-HH2.nc
GLM_05DEG_DIR_PATH = '/o3p/patj/glm/GLM_array_05deg'


# already in utils/glm_utils.py but not imported properly !!! <!> comments missing !!!!
def get_glm_hourly_file_date(glm_filename):
    # expecting str with format GLM_array_DDD_HH1-HH2.nc with DDD: day number, HH1: start hour, HH2: end hour
    # --> faire un pattern check (utiliser une fonction qui sera mise dans utils pour faire ça ?)
    #     ---> comme ça si on change le format on peut changer le check easy peasy (mais y aura quand même des pbms en fait)
    if isinstance(glm_filename, Path):
        glm_filename = str(glm_filename)
    glm_filename = glm_filename.split(".")[0]  # remove .nc part of the path
    glm_filename_split = glm_filename.split("_")
    glm_hour_split = glm_filename_split[-1].split('-')
    return {
        "day_of_year": glm_filename_split[-2],
        "start_hour": glm_hour_split[0],
        "end_hour": glm_hour_split[1]
    }


# already in utils/glm_utils.py but not imported properly !!! <!> comments missing !!!!
def get_glm_daily_dir_date(glm_dirname):
    # expecting str with format OR_GLM-L2-LCFA_G16_sYYYYDDD with YYYY: year, DDD: day number
    # --> faire un pattern check (utiliser une fonction qui sera mise dans utils pour faire ça ?)
    #     ---> comme ça si on change le format on peut changer le check easy peasy (mais y aura quand même des pbms en fait)
    if isinstance(glm_dirname, Path):
        glm_filename = str(glm_dirname)
    glm_dirname = glm_dirname.split("_")[-1]  # get last part of the dir name (sYYYYDDD)
    glm_dirname_split = glm_dirname[1:]  # remove the s
    return {
        "year": glm_dirname_split[:4],
        "day_of_year": glm_dirname_split[-3:]
    }


# already in utils/glm_utils.py but not imported properly !!! <!> comments missing !!!!
def generate_glm_hourly_nc_file_pattern(day_of_year, start_hour="[0-2][0-9]", end_hour="[0-2][0-9]"):
    return GLM_HOURLY_FILE_NAME + str(day_of_year) + "_" + str(start_hour) + "-" + str(end_hour) + ".nc"

def generate_glm_05deg_hourly_nc_file_path(glm_dir_path_root, day_of_year, start_hour, end_hour):
    return f'{glm_dir_path_root}{day_of_year}/GLM_array_05deg_{day_of_year}_{start_hour}-{end_hour}.nc'
####################################################################################


# already in utils/glm_utils.py but not imported properly !!! <!> comments missing !!!!
# <!> à laisser dans utils ???
def get_daily_glm_dir_path_list(glm_dir_path, start_date_dic, end_date_dic):
    """
    Function returning a list of all the daily glm directories between start and end date
    Each daily glm directory contains the hourly glm files for this day
    :param glm_dir_path: directory where the daily glm directories are stored
    :param start_date_dic: start date dictionary (year, day number, hour)
    :param end_date_dic: end date dictionary (year, day number, hour)
    :return: list of Path objects for all the directories between start and end date
    :rtype: <list> [ <pathlib.Path>, ..., <pathlib.Path> ]
    """
    all_dir_list = sorted(glm_dir_path.glob(GLM_DIR_NAME + start_date_dic["year"] + "[0-3][0-6][0-9]"))
    dir_list = []
    # <!> glm_dir_path doit être un Path
    min_date_dir_path = glm_dir_path / Path(GLM_DIR_NAME + start_date_dic["year"] + start_date_dic["day_of_year"])
    max_date_dir_path = glm_dir_path / Path(GLM_DIR_NAME + end_date_dic["year"] + end_date_dic["day_of_year"])
    for dir_name in all_dir_list:
        if min_date_dir_path <= dir_name <= max_date_dir_path:
            dir_list.append(dir_name)
    return sorted(dir_list)


def get_glm_hourly_nc_files_path(glm_dir_path, start_date_dic, end_date_dic):
    """
    Function returning a list of all the hourly GLM files between start and end date.
    The hourly files are in the directories listed in glm_daily_dir_path
    :param glm_dir_path: directory where the daily glm directories are stored
    :param start_date_dic: <dict> containing year, day number and hour of the start date
    :param end_date_dic: <dict> containing year, day number and hour of the end date
    :return: list of Path objects for all the hourly glm netCDF files between start and end date
    :rtype: <list> [ <pathlib.Path>, ..., <pathlib.Path> ]
    """
    nc_file_list = []
    glm_daily_dir_list = get_daily_glm_dir_path_list(glm_dir_path, start_date_dic, end_date_dic)
    for daily_dir_path in sorted(glm_daily_dir_list):  # for each day dir go through hourly file list
        # get directory day and year (according to dir name)
        daily_dir_path_dic = get_glm_daily_dir_date(str(daily_dir_path))
        # if dir day between min and max day, add all nc files corresponding to the pattern --> GLM_array_DDD_HH1-HH2.nc
        if start_date_dic["day_of_year"] < daily_dir_path_dic["day_of_year"] < end_date_dic["day_of_year"]:
            nc_file_list.extend(
                sorted(daily_dir_path.glob(generate_glm_hourly_nc_file_pattern(daily_dir_path_dic["day_of_year"]))))

        # if min day, we only keep the hourly files > min day start hour
        elif daily_dir_path_dic["day_of_year"] == start_date_dic["day_of_year"]:
            for hourly_file_path in sorted(daily_dir_path.iterdir()):
                hourly_file_date_dic = get_glm_hourly_file_date(hourly_file_path.parts[-1])
                if hourly_file_date_dic["start_hour"] >= start_date_dic["hour"]:
                    nc_file_list.append(hourly_file_path)

        # if max day, we only keep the hourly files < max day end hour
        elif daily_dir_path_dic["day_of_year"] == end_date_dic["day_of_year"]:
            for hourly_file_path in sorted(daily_dir_path.iterdir()):
                hourly_file_date_dic = get_glm_hourly_file_date(hourly_file_path.parts[-1])
                if hourly_file_date_dic["start_hour"] <= end_date_dic["hour"]:
                    nc_file_list.append(hourly_file_path)
                else:  # break the loop once file date is over the end hour
                    break
    return sorted(nc_file_list)


def get_fp_output_start_end_dates_dict(fp_out_ds):
    """
    Function returning FLEXPART simulation start and end date as dictionaries containing year, day of year and hour value
    :param fp_out_ds: <xarray.Dataset> FLEXPART simulation output
    :return: <tuple> ( <dict> { "year": <str>, "day_of_year": <str>, "hour": <str> },
                        <dict> { "year": <str>, "day_of_year": <str>, "hour": <str> } )
    """
    start_npdate, end_npdate = get_min_max_fp_output_date(fp_out_ds)  # <numpy.datetime64>
    start_date = np_datetime64_to_datetime(start_npdate)  # <datetime.datetime>
    end_date = np_datetime64_to_datetime(end_npdate)
    return get_glm_format_date_from_datetime(start_date), get_glm_format_date_from_datetime(end_date)


""" <!> va falloir bidouiller pour le nom du resulting nc file"""


def get_hourly_glm_05_deg_ds(glm_ds_url, data_vars_dict, lon_min=-179.75, lon_max=180, lat_min=-89.75, lat_max=90,
                             grid_resolution=0.5, nc_file_path='/o3p/patj/glm/GLM_array_'):
    """
    Function to open an hourly GLM file and grid the data into specific grids (by default 0.5° x 0.5°, same as FLEXPART output).
    The resulting dataset contains the given GLM data variables gridded according to the given grid resolution + a new date coordinate corresponding to the given GLM file date
    :param glm_ds_url: path to glm file that needs to be regridded
    :param data_vars_dict: dictionary containing, for each variable name the operation to be applied and the dimensions on which the operation should be applied
    :param lon_min: new grid minimum longitude
    :param lon_max: new grid maximum longitude
    :param lat_min: new grid minimum latitude
    :param lat_max: new grid maximum latitude
    :param grid_resolution: new grid resolution
    :param nc_file_path: name of result netCDF file
    :return: <xarray.Dataset> newly gridded dataset
    """
    """ 
    <!> améliorations:
        - là on groupby latitude sur flash_energy mais on pourrait vouloir le faire sur autres trucs (genre group qchose)
            --> rendre ça + générique pour qu'on puisse récup plusieurs variables
            Parce que là du coup même le sum et le count sont fait sur le flash_energy grouped by
        - est-ce que vraiment besoin return le dataset ??? 
    """
    # generate dataset in which we'll be putting the summed flash_energy values
    latitudes = np.arange(lat_min, lat_max, grid_resolution)
    longitudes = np.arange(lon_min, lon_max, grid_resolution)
    glm_ds_date = get_glm_hourly_file_date(glm_ds_url)
    """ <!!!!!> YEAR EN DUR POUR L'INSTANT AIE AIE AIE """
    date = get_np_datetime64_from_string(year=2018, day_of_year=glm_ds_date['day_of_year'],
                                         hour=glm_ds_date['start_hour'])
    data_vars = {}
    for var in data_vars_dict:
        data_vars[var] = (['time', 'latitude', 'longitude'], np.zeros(shape=(1, len(latitudes), len(longitudes))))
    target_ds = xr.Dataset(
        data_vars=data_vars,
        coords={
            'time': np.atleast_1d(date),
            'latitude': latitudes,
            'longitude': longitudes,
        }
    )
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
                da_operation = apply_operation_on_grouped_da(
                    grouped_by_da=da_for_lat_grouped_by_lon,
                    operation=data_vars_dict[data_var_name]['operation'],
                    operation_dims=data_vars_dict[data_var_name]['operation_dims']
                )
                # add new values to target_ds for specific latitude and longitude(s)
                target_ds[data_var_name].loc[
                    dict(latitude=grouped_lat, longitude=da_operation.longitude)] = da_operation
    # ON VEUT: /o3p/patj/glm/GLM_array_05deg_DDD/GLM_array_05deg_DDD_HH1-HH2.nc
    # on a:
    # - /o3p/macc/glm/OR_GLM-L2-LCFA_G16_s2018146/GLM_array_146_21-22.nc
    #       --> on garde    GLM_array_146_21-22.nc
    #       --> pour avoir  GLM_array_05deg_DDD_HH1-HH2.nc
    # -
    nc_file_path = generate_glm_05deg_hourly_nc_file_path(glm_ds_url, glm_ds_date["day_of_year"], glm_ds_date["start_hour"], glm_ds_date["end_hour"])
    target_ds.to_netcdf(nc_file_path)
    """ <!> est-ce que vraiment besoin de return le dataset ?? <!>"""
    return target_ds


if __name__ == '__main__':
    """
    glm_path = "GLM_array_156_19-20.nc"
    
    """
    fp_out_path = '/o3p/macc/flexpart10.4/flexpart_v10.4_3d7eebf/src/exercises/soft-io-li/flight_2018_003_1h_05deg/10j_100k_output/grid_time_20180605210000.nc'
    fp_ds = fpout.open_fp_dataset(fp_out_path, chunks='auto', max_chunk_size=1e8, assign_releases_position_coords=False)
    start_date_dic, end_date_dic = get_fp_output_start_end_dates_dict(fp_ds)
    print("------------------------------------------------------")
    print(f"start_date_dic : {start_date_dic}\nend_date_dic : {end_date_dic}")
    print("------------------------------------------------------")

    glm_dir_url = Path('/o3p/macc/glm')
    glm_file_list = get_glm_hourly_nc_files_path(glm_dir_url, start_date_dic, end_date_dic)
    """
    MERDOUILLE faut gérer les dossiers en plus !! PAS QUE LES FICHIERS
    Catherine les met là dedans: 
    dr.to_netcdf('/o3p/macc/test/GLM_array_154_05deg/GLM_array_154_'+s[0]+'_batch_bis.nc')  
    """
    for glm_file in glm_file_list:
        print(glm_file)
        #nc_file_path = GLM_05_DIR_PATH +
        glm_file_date = get_glm_hourly_file_date(glm_file)
        print(generate_glm_05deg_hourly_nc_file_path(GLM_05_DIR_PATH, glm_file_date["day_of_year"], glm_file_date["start_hour"], glm_file_date["end_hour"]))
        break
        data_vars = {
            "flash_energy": {
                "operation": "sum",
                "operation_dims": None
            },
            "flash_count": {
                "operation": "count",
                "operation_dims": None
            }
        }
        final_ds = get_hourly_glm_05_deg_ds(
            glm_ds_url=glm_file,
            data_vars_dict=data_vars,
            nc_file_name='glm_o5_ok_date_init_ds.nc'
        )





"""
OK- STEP 1: get list of all hourly netcdf files
    1- from FP output ds get start and end date 
        --> faire une fonction qui appelle:
            get_min_max_fp_output_date
            np_datetime64_to_datetime
            get_split_glm_format_date
    2- 
STEP 2: for each hourly file call, regrid it to 0.5 x 0.5 deg grid
    1- get nc dir list
    2- get nc file list
    3- loop through nc file list and call get_hourly_glm_05_deg_ds on each of them
    <!> gérer le nom des fichiers de sortie !!!
"""

{ "flash_energy": { "operation": "sum", "operation_dims": None }, "flash_count": { "operation": "count", "operation_dims": None } }