import numpy as np
import pandas as pd
import pathlib

from common.utils import timestamp_now_formatted

from .constants import OUTPUT_ROOT_DIR, AIRPRESS_VARNAME, NOx_PLUME_ID_VARNAME, TIMESTAMP_FORMAT
from .iagos_utils import get_CO_varname, get_O3_varname
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


def write_plume_info_to_csv_file(ds, output_dirpath, filename_suffix=''):
    if not (np.isnan(ds[NOx_PLUME_ID_VARNAME].where(ds[NOx_PLUME_ID_VARNAME] > 0)).all()):
        lon_varname, lat_varname = get_lon_lat_varnames(ds)
        CO_varname = get_CO_varname(flight_program=ds.attrs['program'], tropo=True, filtered=False)
        O3_varname = get_O3_varname(flight_program=ds.attrs['program'], tropo=True)
        plume_info_list = []
        plume_id_list = [id for id in np.unique(ds[NOx_PLUME_ID_VARNAME]) if (id != -1 and not np.isnan(id))]
        for plume_id in plume_id_list:
            plume_ds = ds.where(ds[NOx_PLUME_ID_VARNAME] == plume_id, drop=True)
            """ <!> calculer O3, CO mean + median et tout ? et pour les NOx aussi ? <!> """
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
                'O3_median': np.nanmedian(plume_ds[O3_varname].values),
                'O3_std': np.nanstd(plume_ds[O3_varname].values),
                'CO_mean': np.nanmean(plume_ds[CO_varname].values),
                'CO_median': np.nanmedian(plume_ds[CO_varname].values),
                'CO_std': np.nanstd(plume_ds[CO_varname].values),
            }
            plume_info_list.append(plume_info_dict)

        output_dirpath = pathlib.Path(output_dirpath)
        if not output_dirpath.exists():
            output_dirpath.mkdir(parents=True)

        arrival_time = pd.Timestamp(ds.attrs["arrival_UTC_time"]).strftime(TIMESTAMP_FORMAT)
        pd.DataFrame(plume_info_list).to_csv(f'{output_dirpath}/{ds.attrs["flight_name"]}_{arrival_time}{filename_suffix}_plume-info.csv')