import pandas as pd
import pathlib
import warnings
import xarray as xr

import constants as cts
from .utils_functions import str_to_path, date_to_pd_timestamp


def get_NOx_varname(flight_program, smoothed, filtered):
    if flight_program == cts.CORE or flight_program == f'{cts.IAGOS}-{cts.CORE}':
        NOx_varname = cts.CORE_NOx_VARNAME
    elif flight_program == cts.CARIBIC or flight_program == f'{cts.IAGOS}-{cts.CARIBIC}':
        NOx_varname = cts.CARIBIC_NOx_VARNAME
    else:
        raise KeyError(f'{flight_program} {cts.FLIGHT_PROGRAM_KEYERROR_MSG}')
    # add suffix to NOx varname
    if smoothed:
        NOx_varname = f'{NOx_varname}_smoothed'
    if filtered:
        NOx_varname = f'{NOx_varname}_filtered'
    return NOx_varname

def get_CO_varname(flight_program, filtered):
    if flight_program == cts.CORE or flight_program == f'{cts.IAGOS}-{cts.CORE}':
        CO_varname = cts.CO_VARNAME
    elif flight_program == cts.CARIBIC or flight_program == f'{cts.IAGOS}-{cts.CARIBIC}':
        CO_varname = cts.CARIBIC_CO_VARNAME
    else:
        raise KeyError(f'{flight_program} {cts.FLIGHT_PROGRAM_KEYERROR_MSG}')
    # add suffix to CO varnameif filtered:
        CO_varname = f'{CO_varname}_filtered'
    return CO_varname


def get_var_list(flight_program):
    """
    Returns list of variables names corresponding to a given flight program (CO, O3, NO, NO2 and NOx)
    :param flight_program:
    :return: <list> [ <str>, ..., <str> ]
    """
    if flight_program == cts.CORE or flight_program == f'{cts.IAGOS}-{cts.CORE}':
        var_list = [cts.CO_VARNAME, cts.O3_VARNAME, cts.CORE_NO_VARNAME, cts.CORE_NO2_VARNAME, cts.CORE_NOx_VARNAME]
    elif flight_program == cts.CARIBIC or flight_program == f'{cts.IAGOS}-{cts.CARIBIC}':
        var_list = [cts.CARIBIC_CO_VARNAME, cts.CARIBIC_O3_VARNAME, cts.CARIBIC_NO_VARNAME, cts.CARIBIC_NO2_VARNAME]
    else:
        raise KeyError(f'{flight_program} {cts.FLIGHT_PROGRAM_KEYERROR_MSG}')
    return var_list

def get_valid_data(var_list, ds, valid_data_flag_value=0, print_debug=False):
    """
    Only keep values of the dataset where validity flag is equal to good
    :param var_list: <list> [ <str>, ... ] list of variable names
    :param ds: <xarray.Dataset>
    :param valid_data_flag_value: <int> value of the validity_flag = GOOD (default=0)
    :param print_debug: <bool> for testing purposes, print debug
    :return: <xarray.Dataset>
    """
    for varname in var_list:
        if print_debug:
            print(f'{varname}.notnull().sum() BEFORE val flag filter: {ds[varname].notnull().sum().values}')
        if varname in ds.keys():
            ds[varname] = ds[varname].where(ds[f'{varname}_validity_flag'] == valid_data_flag_value)
        if print_debug:
            print(f'{varname}.notnull().sum() AFTER val flag filter: {ds[varname].notnull().sum().values}')
            print('---')
    return ds



# TODO: filtrer par date aussi ??
def get_NOx_flights_from_catalogue(iagos_cat_path=cts.IAGOSv3_CAT_PATH, start_flight_id=None, end_flight_id=None,
                                   flight_type=None, flight_id_list=None):
    """
    Returns list of NOx flights urls (from iagos v3 catalogue) between
    :param iagos_cat_path:
    :param start_flight_id:
    :param end_flight_id:
    :param flight_type:
    :param flight_id_list:
    :return:
    """
    if not str_to_path(iagos_cat_path).exists():
        raise ValueError(f"Invalid IAGOS catalogue path, {iagos_cat_path} does NOT exist!")
    df_cat = pd.read_parquet(iagos_cat_path)
    # get all NO2 data variable names
    NO2_vars = [varname for varname in df_cat if varname.startswith("data_vars_") and ("NO2") in varname]
    # get all NOx flights (flights with NO2 variables > 0)
    NOx_flights = df_cat.loc[(df_cat[NO2_vars] > 0).any(axis='columns')]
    # if we only want flights between two flight ids or specific flights in a given list
    if flight_id_list is not None:
        NOx_flights = NOx_flights.loc[flight_id_list]
    elif (start_flight_id is not None) and (end_flight_id is not None):
        NOx_flights = NOx_flights.loc[start_flight_id:end_flight_id]
        # TODO: recup flights from several programs instead of just one (passe une liste)
    # if we only want flights from one specific program
    if flight_type is not None:
        if flight_type in ['CARIBIC', 'CORE', 'MOZAIC']:
            flight_type = f'IAGOS-{flight_type}'
        elif flight_type not in ['IAGOS-CARIBIC', 'IAGOS-CORE', 'IAGOS-MOZAIC']:
            raise ValueError(
                f"Invalid IAGOS program type ({flight_type}), expecting any of the following: ['IAGOS-CARIBIC', 'CARIBIC', 'IAGOS-CORE', 'CORE', 'IAGOS-MOZAIC', 'MOZAIC']")
        NOx_flights = NOx_flights.loc[NOx_flights.attrs_program == flight_type]

    return NOx_flights.general_url


# --------------- PV ---------------
def find_PV_file_in_PV_dir(pv_dir_path, flight_name):
    pv_dir_path = str_to_path(pv_dir_path)
    pv_file_path = sorted(pv_dir_path.glob(f'{cts.IAGOSv3_PV_FILE_PREFIX}_{flight_name}_L4*.nc*'))
    # if empty list --> no PV file associated with flight
    if len(pv_file_path) == 0:
        raise FileNotFoundError(f'Unable to retrieve PV values: no PV file associated with flight {flight_name}')
    # if more than one IAGOS_ECMWF_<flight_ID>_L4_x.x.x.nc4 file --> return most recent one
    elif len(pv_file_path) > 1:
        return pv_file_path[-1]
    else:  # len == 1
        return pv_file_path[0]

def get_PV_file_path(d_time, flight_name):
    d_time = date_to_pd_timestamp(d_time)
    pv_dir_path = pathlib.Path(f'{cts.IAGOSv3_PV_PATH}/{d_time.year}{d_time.month:02d}')
    if not pv_dir_path.exists():
        raise FileNotFoundError(f'Unable to retrieve PV values: directory {pv_dir_path} does NOT exist')
    else:
        return find_PV_file_in_PV_dir(pv_dir_path=pv_dir_path, flight_name=flight_name)

def add_PV(ds):
    departure_time = pd.Timestamp(ds.attrs['departure_UTC_time'])
    pv_file_path = get_PV_file_path(d_time=departure_time, flight_name=ds.attrs["flight_name"])
    if pv_file_path.exists():
        ds[cts.PV_VARNAME] = xr.open_dataset(pv_file_path)[cts.PV_VARNAME]
        return ds
    else:
        raise FileNotFoundError(f'Unable to retrieve PV values: file {pv_file_path} does NOT exist')


def get_PV(ds, print_debug=False):
    if print_debug:
        print(f'PV in ds.keys() BEFORE get_PV: {cts.PV_VARNAME in list(ds.keys())}')
    if not cts.PV_VARNAME in list(ds.keys()):
        try:
            ds = cts.add_PV(ds)
        except Exception as e:
            warnings.warn(f'No PV found for flight {ds.attrs["flight_name"]}\nException: {e}')
            # TODO: sort de la loop pour ce vol, on continue pas si on a pas de PV
    if print_debug:
        print(f'PV in ds.keys() AFTER get_PV: {cts.PV_VARNAME in list(ds.keys())}')
        print('---')
    return ds


# -------------------- LiNOx filters --------------------

#TODO: faire une fonction pour chaque filtre ?
def _keep_cruise(ds, NOx_varname, print_debug=False):
    if print_debug:
        print(f'{NOx_varname}.notnull().sum() BEFORE cruise filter: {ds[NOx_varname].notnull().sum().values}')
    ds = ds.where(ds[f'{cts.AIRPRESS_VARNAME}_validity_flag'] == 0).where(ds[cts.AIRPRESS_VARNAME] < 30000)
    if print_debug:
        print(f'{NOx_varname}.notnull().sum() AFTER cruise filter: {ds[NOx_varname].notnull().sum().values}')
        print('---')
    return ds

#TODO: pas sûre de l'appeler comme ça
def _remove_CO_excess(ds, CO_q3_da, NOx_varname, CO_varname, print_debug):
    ds[f'{NOx_varname}_tropo_CO_filter'] = ds[f'{NOx_varname}'].where(ds[CO_varname] < CO_q3_da)
    if print_debug:
        print(
            f'{NOx_varname}_tropo_CO_filtered.notnull().sum() AFTER strato + CO filter: {ds[f"{NOx_varname}_tropo_CO_filter"].notnull().sum().values}')
        print('---')
    return ds

#TODO: ça peut PAS marcher parce que q3_ds même dims que ds tout court
def get_q3_attrs(ds, q3_ds):
    q3_attrs = {}
    for month in ds.UTC_time.dt.month.groupby(ds.UTC_time.dt.month):
        month = month[0]
        for geo_region in ds.geo_region.groupby(ds.geo_region):
            geo_region = geo_region[0]
            q3_attrs[f'{month}_{geo_region}_CO_q3'] = int(
                q3_ds.mean('year')['CO_q3'].sel(month=month, geo_region=geo_region).values)
            q3_attrs[f'{month}_{geo_region}_NOx_q3'] = int(
                q3_ds.mean('year')['NOx_q3'].sel(month=month, geo_region=geo_region).values)
    q3_attrs

# TODO: voir si je passe q3_ds ou autre chose ?
def apply_LiNOx_plume_filters(ds, cruise_only, smoothed_data, q3_ds_path, print_debug=False, write_NOx_q3_to_json=False,
                              local=False):
    """
    Function to apply filters on the NOx timeseries to remove stratospheric, anthropogenic and background influence.
    If cruise_only = True: only keep data where air pressure < 30000 Pa
    - Stratospheric influence: remove NOx data where PV > 2
    - Anthropogenic influence: remove NOx data vhere CO value > CO_q3
    - Background influence: remove NOx data < NOx_q3 (only keep NOx excess)
    :param ds: <xarray.Dataset> flight ds
    :param cruise_only: <bool> if True, only keep data where air pressure < 30000 Pa
    :param smoothed_data: <bool> if True, apply filters on smoothed NOx timeseries (rolling mean)
    :param q3_ds: <xarray.Dataset>
    :param flight_type: <str> flight type (accepted values: 'IAGOS-CORE', 'CORE', 'IAGOS-CARIBIC', 'CARIBIC', 'IAGOS-MOZAIC', 'MOZAIC')
    :param print_debug: <bool> for testing purposes, print debug
    :param write_NOx_q3_to_json: <bool> for testing purposes, writes value of NOx_q3 into a specific json file
    :param local: <bool> for testing purposes, indicates if running program on local machine
    :return: <xarray.Dataset> filtered version of the flight ds
    """
    NOx_varname = get_NOx_varname(ds.attrs['program'], smoothed=smoothed_data, filtered=False)

    # only keep cruise data
    if cruise_only:
        ds = _keep_cruise(ds=ds, NOx_varname=NOx_varname, print_debug=print_debug)

    # remove stratospheric influence
    ds = ds.where(ds[cts.PV_VARNAME] < 2)

    # get q3_ds for ds month and geo region #TODO <!> mean('year')
    q3_ds = xr.open_dataset(q3_ds_path).mean('year')

    q3_ds_sel = q3_ds.sel(month=ds['UTC_time'].dt.month, geo_region=ds['geo_region'])
    CO_varname = get_CO_varname(ds.attrs['program'], filtered=False)

    # remove anthropogenic influence #TODO <!> mean('year')
    ds = _remove_CO_excess(ds=ds, CO_q3_da=q3_ds_sel['CO_q3'], NOx_varname=NOx_varname, CO_varname=CO_varname, print_debug=print_debug)

    # keep NOx excess (NOx > q3)
    ds[f'{NOx_varname}_filtered'] = ds[f'{NOx_varname}_tropo_CO_filter']\
                    .where(ds[f'{NOx_varname}_tropo_CO_filter'] > q3_ds_sel['NOx_q3'])

    ds = ds.assign_attrs(get_q3_attrs(ds=ds, q3_ds=q3_ds))

    return ds