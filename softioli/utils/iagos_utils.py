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