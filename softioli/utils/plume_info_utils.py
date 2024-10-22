import numpy as np
import pandas as pd
import pathlib
import xarray as xr

from common.utils import timestamp_now_formatted

from .constants import OUTPUT_ROOT_DIR, AIRPRESS_VARNAME, NOx_PLUME_ID_VARNAME, ARRIVALTIME_FORMAT_CSV_FILENAME, PROGRAM_ATTR, NOx_MEDIAN, CO_O3_BACKGROUND_DS_PATH, RHL_VARNAME, PROGRAM_ATTR, IAGOS, CORE
from .iagos_utils import get_CO_varname, get_O3_varname, get_NOx_varname
from .utils_functions import get_lon_lat_varnames

#TODO: supprimer d'ici
def generate_root_plume_detection_output_dirpath(date=None, dirname_suffix='', root_output_dir_path=None):
    """
    Return directory path in which plume detection outputs should be stored
    @param date: <pd.Timestamp> or <str> (or anything convertible to pandas
    @param dirname_suffix:
    @param root_output_dir_path:
    @return: <root_output_dir_path>/YYYY-MM-DD_HHMM_plume_detection<_suffix>
    """
    date = date if date is not None else timestamp_now_formatted('%Y-%m-%d_%H%M', tz='CET')
    root_output_dir_path = root_output_dir_path if root_output_dir_path is not None else OUTPUT_ROOT_DIR
    date = pd.Timestamp(date).strftime('%Y-%m-%d_%H%M')

    return pathlib.Path(f'{root_output_dir_path}/{date}_plume_detection{dirname_suffix}')

#TODO: supprimer d'ici (à mettre dans utils_function)
def generate_plume_detection_flight_output_dirpath(flight_name, date=None, plume_detection_res_dir=None, dirname_suffix=''):
    if plume_detection_res_dir is None:
        plume_detection_res_dir = generate_root_plume_detection_output_dirpath(date=date, dirname_suffix=dirname_suffix)
    else:
        plume_detection_res_dir = pathlib.Path(plume_detection_res_dir)

    return pathlib.Path(f'{plume_detection_res_dir}/{flight_name}/')


def get_dict_value_by_region(value_by_region_ds):
    """
    Returns dictionary with excess value for each region in a dict: { <region1>: <value_region1>, ... , <region_n>: <value_region_n> }
    @param value_by_region_ds: <xarray.Dataset> Dataset containing a single value for each region
    @return: <dict> { <str>: <float>, ... , <str>: <float> }
    """
    excess_dict = {}
    for region in value_by_region_ds.geo_region.values:
        excess_dict[region] = value_by_region_ds.sel(geo_region=region).item() # can be rounded w/ np.round(val, nb_decimals)
    return excess_dict


def get_excess_std_by_region(var_da, excess_da):
    return (var_da - excess_da).groupby('geo_region').std()


def get_excess_mean_by_region(var_da, excess_da):
    return (var_da - excess_da).groupby('geo_region').mean()


def write_plume_info_to_csv_file(ds, output_dirpath, filename_suffix='', CO_O3_background_ds_path=CO_O3_BACKGROUND_DS_PATH):
    if not (np.isnan(ds[NOx_PLUME_ID_VARNAME].where(ds[NOx_PLUME_ID_VARNAME] > 0)).all()):
        lon_varname, lat_varname = get_lon_lat_varnames(ds)
        CO_varname = get_CO_varname(flight_program=ds.attrs[PROGRAM_ATTR], smoothed=True, tropo=True)
        O3_varname = get_O3_varname(flight_program=ds.attrs[PROGRAM_ATTR], tropo=True)
        NOx_varname = get_NOx_varname(flight_program=ds.attrs[PROGRAM_ATTR], smoothed=True, tropo=True, filtered=True)
        # get CO and O3 background values for each region and month
        CO_O3_bckgd_ds = xr.open_dataset(CO_O3_background_ds_path).mean('year') \
                            .sel(geo_region=ds['geo_region'], month=ds['UTC_time'].dt.month, q=0.5)

        plume_info_list = []
        plume_id_list = [id for id in np.unique(ds[NOx_PLUME_ID_VARNAME]) if (id != -1 and not np.isnan(id))]

        for plume_id in plume_id_list:
            plume_ds = ds.where(ds[NOx_PLUME_ID_VARNAME] == plume_id, drop=True)

            plume_info_dict = {
                'plume_id': plume_id,
                'start_UTC_time': plume_ds.UTC_time.values[0],
                'end_UTC_time': plume_ds.UTC_time.values[-1],
                'start_lon': plume_ds[lon_varname].values[0],
                'end_lon': plume_ds[lon_varname].values[-1],
                'start_lat': plume_ds[lat_varname].values[0],
                'end_lat': plume_ds[lat_varname].values[-1],
                'start_press': plume_ds[AIRPRESS_VARNAME].values[0],
                'end_press': plume_ds[AIRPRESS_VARNAME].values[-1],

                'O3_mean': np.nanmean(plume_ds[O3_varname].values),
                'O3_excess_std': get_dict_value_by_region(get_excess_std_by_region(plume_ds[O3_varname], CO_O3_bckgd_ds['O3_quantile'])),
                'O3_excess_mean': get_dict_value_by_region(get_excess_mean_by_region(plume_ds[O3_varname], CO_O3_bckgd_ds['O3_quantile'])),

                'CO_mean': np.nanmean(plume_ds[CO_varname].values),
                'CO_excess_std': get_dict_value_by_region(get_excess_mean_by_region(plume_ds[CO_varname], CO_O3_bckgd_ds['CO_quantile'])),
                'CO_excess_mean': get_dict_value_by_region(get_excess_std_by_region(plume_ds[CO_varname], CO_O3_bckgd_ds['CO_quantile'])),

                'NOx_mean': np.nanmean(plume_ds[NOx_varname].values),
                'NOx_excess_mean': np.nanmean(plume_ds[NOx_varname].values - NOx_MEDIAN),
                'NOx_excess_std': np.nanstd(plume_ds[NOx_varname].values - NOx_MEDIAN),
            }
            if ds.attrs[PROGRAM_ATTR] in [f"{IAGOS}-{CORE}", CORE]:
                plume_info_dict['RHL_mean'] = np.nanmean(plume_ds[RHL_VARNAME].values)
            plume_info_list.append(plume_info_dict)

        output_dirpath = pathlib.Path(output_dirpath)
        if not output_dirpath.exists():
            output_dirpath.mkdir(parents=True)

        arrival_time = pd.Timestamp(ds.attrs["arrival_UTC_time"]).strftime(ARRIVALTIME_FORMAT_CSV_FILENAME)
        pd.DataFrame(plume_info_list).to_csv(f'{output_dirpath}/{ds.attrs["flight_name"]}_arrivaltime-{arrival_time}{filename_suffix}_plume-info.csv')