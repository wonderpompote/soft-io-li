"""
Regrid hourly satellite files to match FLEXPART output grid (usually 0.5° x 0.5°)

Satellite data supported:
- GLM (GOES-E and GOES-W)

Main goal: given a list of pre-regrid hourly nc files
--> regrid each of them and store them in regrid_hourly_<sat_name> directory
"""
from datetime import datetime
import numpy as np
import xarray as xr

from .utils import GLMPathParser, generate_sat_hourly_file_path, generate_sat_hourly_filename_pattern
from .utils import constants as cts
from .utils import xarray_pandas_utils as xr_pd_utils


# TODO: <!!> qaund autre satellites voir si on peut pas généraliser cette fonction au lieu d'en faire plusieurs ?
def generate_glm_hourly_regrid_file(pre_regrid_file_url, grid_resolution, grid_res_str, overwrite,
                                    data_vars_to_regrid_dict=cts.DEFAULT_GLM_DATA_VARS_TO_REGRID,
                                    lat_min=cts.FPOUT_LAT_MIN, lat_max=cts.FPOUT_LAT_MAX,
                                    lon_min=cts.FPOUT_LON_MIN, lon_max=cts.FPOUT_LON_MAX,
                                    result_dir_path=None, old_glm_filename=False):
    """
    Pre-process GLM hourly data file to correspond to a specific grid resolution
    @param pre_regrid_file_url: <pathlib.Path> or <str>
    @param grid_resolution: <float>
    @param grid_res_str: <str> grid resolution str (to be added to the resulting filename)
    @param overwrite: <bool> overwrite file if it already exists
    @param data_vars_to_regrid_dict: <dict> dictionary with the parameters for each operation on a data variable (default)
    @param lat_min: <float> (default)
    @param lat_max: <float> (default)
    @param lon_min: <float> (default)
    @param lon_max: <float> (default)
    @param result_dir_path: <pathlib.Path> or <str> mostly for testing, directory in which resulting file should be stored, if None --> use default path
    @param old_glm_filename: <bool> if file to regrid uses the old file notation (used by macc)
    @return:
    """
    # STEP 1: recup pre-regrid file start date (year, day, hour) --> avec GLMPathParser / <sat>PathParser
    pre_regrid_glmparser = GLMPathParser(file_url=pre_regrid_file_url, regrid=False, old_glm_filename=old_glm_filename)
    pre_regrid_file_date = pre_regrid_glmparser.get_start_date_pdTimestamp(ignore_missing_start_hour=False)

    # STEP 2: create result nc file path
    result_dir_path = generate_sat_hourly_file_path(date=pre_regrid_file_date, satellite=cts.GOES_SATELLITE,
                                                    sat_version=pre_regrid_glmparser.satellite_version, regrid=True,
                                                    regrid_res=grid_res_str, dir_path=result_dir_path)

    # STEP 3: if directory/ies containing result nc file path does NOT exist --> create it/them
    # if directory that will contain nc file does not exist -> create it
    if not result_dir_path.parent.exists():
        result_dir_path.parent.mkdir(parents=True)
        print(f"Creating directory {result_dir_path.parent}")

    # STEP 4: check if regrid file exists and if it doesn't OR if overwrite == True --> "create it"
    if (not result_dir_path.exists()) or (result_dir_path.exists() and overwrite):
        #       STEP 4.1: generate empty dataset with correctly gridded lat et lon
        target_ds = xr.Dataset(
            coords={
                'latitude': np.arange(lat_min, lat_max + grid_resolution, grid_resolution),
                'longitude': np.arange(lon_min, lon_max + grid_resolution, grid_resolution)
            },
            attrs={
                'grid_resolution': f'{grid_resolution}° x {grid_resolution}°'
            }
        )

        #       STEP 4.2: open pre-regrid glm file
        with xr.open_dataset(pre_regrid_file_url) as glm_ds:
            #       STEP 4.2.1: assign_coords de target ds au pre-regrid ds
            # assign new longitude and latitude coords with chosen grid resolution using nearest method
            _ds_assigncoords_lonlat = glm_ds.assign_coords({
                'latitude': target_ds.latitude.sel(latitude=glm_ds.flash_lat, method='nearest'),
                'longitude': target_ds.longitude.sel(longitude=glm_ds.flash_lon, method='nearest')
            })
            #           STEP 4.2.2: keep several attributes from the original glm file
            target_ds = target_ds.assign_attrs({
                'production_site': glm_ds.attrs.get('production_site', ''),
                'orbital_slot': glm_ds.attrs.get('orbital_slot', ''),
                'platform_ID': glm_ds.attrs.get('platform_ID', ''),
                'instrument_type': glm_ds.attrs.get('instrument_type', ''),
                'instrument_ID': glm_ds.attrs.get('instrument_ID', ''),
                'spatial_resolution': glm_ds.attrs.get('spatial_resolution', ''),
                'glm_data_procesing_level': glm_ds.attrs.get('processing_level', '')
            })

            # for each operation on each data variable
            for data_var in data_vars_to_regrid_dict:
                for op in data_vars_to_regrid_dict[data_var]['operation']:
                    # only keep interesting variable and coords
                    _ds = _ds_assigncoords_lonlat[data_var] \
                        .reset_coords(names=['latitude', 'longitude'], drop=False) \
                        .reset_coords(drop=True)
                    # histogram
                    if op.lower() == "histogram":
                        hist_params = data_vars_to_regrid_dict[data_var]['histogram']
                        _ds[f'log_{data_var}'] = np.log10(_ds[data_var])
                        # --> call histogram function
                        op_result_ds = xr_pd_utils.histogram_using_pandas(_ds, f'log_{data_var}', hist_params)
                    # count
                    elif op.lower() == 'count':
                        op_result_ds = xr_pd_utils.count_using_pandas(_ds, data_var,
                                                                      data_vars_to_regrid_dict[data_var]['count'])
                    else:
                        raise ValueError(f'Unexpected operation name ({op}, operations supported: "histogram", "count"')
                    # merge op resulting ds with result ds --> puts nans for missing latitude and longitude values
                    target_ds = xr.merge([op_result_ds, target_ds], combine_attrs='no_conflicts')

        # STEP 5: ajoute pre-regrid date + attribute regrid_file_creation_date au result ds
        target_ds = target_ds.expand_dims({'time': [pre_regrid_file_date]})
        target_ds.attrs['regrid_file_creation_date'] = datetime.now().isoformat()

        # STEP 6: result ds to_netcdf (<!> modifier encoding de la date, ça prend de la palce pour rien)
        # TODO: réduire units de l'heure pour prendre moins de place (pas besoin de nanoseconds en soit)
        target_ds.to_netcdf(
            path=result_dir_path, mode='w',
            encoding={"time": {"dtype": 'float64', 'units': 'nanoseconds since 1970-01-01'}}
        )
        print(f"Created netcdf file {result_dir_path.name}")

    else:  # file already exists so no need to create it again
        print(f"{result_dir_path} already exists")


def regrid_sat_files(path_list, sat_name, grid_resolution=cts.GRID_RESOLUTION,
                     grid_res_str=cts.GRID_RESOLUTION_STR, dir_list=False, overwrite=False,
                     result_dir_path=None, old_glm_filename=False):
    """
    Funciton to regrid a list of hourly satellite data files to a specific grid resolution
    @param path_list: <list> [ <str> or <pathlib.Path>, ... ] list of files or directories to regrid
    @param sat_name: <str> name of the satellite (only 'GOES' supported for now)
    @param grid_resolution: <float> grid resolution
    @param grid_res_str: <str> grid resolution str (to be added to the resulting filename)
    @param dir_list: <bool> if True, list received is a list of directories containing data files, NOT a list of files
    @param overwrite: <bool> overwrite file if it already exists
    @param result_dir_path: <pathlib.Path> or <str> mostly for testing, directory in which resulting file should be stored, if None --> use default path
    @param old_glm_filename: <bool> if file to regrid uses the old file notation (used by macc)
    @return:
    """
    # if path_list contains paths to directories --> get list of files in each directory
    if dir_list:
        _d_path_list = sorted(path_list)  # store path_list temporarily in another list
        path_list = []
        filename_pattern = generate_sat_hourly_filename_pattern(sat_name=sat_name, regrid=False)
        for dir_path in _d_path_list:
            path_list.extend(dir_path.glob(filename_pattern))
    for pre_regrid_file_url in path_list:
        if sat_name == cts.GOES_SATELLITE:
            generate_glm_hourly_regrid_file(pre_regrid_file_url=pre_regrid_file_url, grid_resolution=grid_resolution,
                                            grid_res_str=grid_res_str,
                                            data_vars_to_regrid_dict=cts.DEFAULT_GLM_DATA_VARS_TO_REGRID,
                                            overwrite=overwrite, result_dir_path=result_dir_path,
                                            old_glm_filename=old_glm_filename)
        else:
            raise ValueError(
                f'{sat_name} satellite data not yet supported. Supported satellite data so far: GOES (GLM) )')