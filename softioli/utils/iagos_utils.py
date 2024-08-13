import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import pathlib
import re #TODO: supprimer après fin des tests
import warnings
import xarray as xr

from . import constants as cts
from .utils_functions import str_to_path, date_to_pd_timestamp


def get_NOx_varname(flight_program, smoothed, tropo, filtered):
    if flight_program == cts.CORE or flight_program == f'{cts.IAGOS}-{cts.CORE}':
        NOx_varname = cts.CORE_NOx_VARNAME
    elif flight_program == cts.CARIBIC or flight_program == f'{cts.IAGOS}-{cts.CARIBIC}':
        NOx_varname = cts.CARIBIC_NOx_VARNAME
    else:
        raise KeyError(f'{flight_program} {cts.FLIGHT_PROGRAM_KEYERROR_MSG}')
    # add suffix to NOx varname
    if smoothed:
        NOx_varname = f'{NOx_varname}_smoothed'
    if tropo:
        NOx_varname = f'{NOx_varname}_tropo'
    if filtered:
        NOx_varname = f'{NOx_varname}_filtered'
    return NOx_varname


def get_CO_varname(flight_program, tropo, filtered):
    if flight_program == cts.CORE or flight_program == f'{cts.IAGOS}-{cts.CORE}':
        CO_varname = cts.CO_VARNAME
    elif flight_program == cts.CARIBIC or flight_program == f'{cts.IAGOS}-{cts.CARIBIC}':
        CO_varname = cts.CARIBIC_CO_VARNAME
    else:
        raise KeyError(f'{flight_program} {cts.FLIGHT_PROGRAM_KEYERROR_MSG}')
    # add suffix to CO varname
    if tropo:
        CO_varname = f'{CO_varname}_tropo'
    if filtered:
        CO_varname = f'{CO_varname}_filtered'
    return CO_varname


def get_O3_varname(flight_program, tropo):
    if flight_program in [cts.CORE, f'{cts.IAGOS}-{cts.CORE}', cts.CARIBIC, f'{cts.IAGOS}-{cts.CARIBIC}']:
        O3_varname = cts.O3_VARNAME
    else:
        raise KeyError(f'{flight_program} {cts.FLIGHT_PROGRAM_KEYERROR_MSG}')
    # add suffix to CO varname
    if tropo:
        O3_varname = f'{O3_varname}_tropo'
    return O3_varname


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
            ds = add_PV(ds)
        except Exception as e:
            warnings.warn(f'No PV found for flight {ds.attrs["flight_name"]}\nException: {e}')
            # TODO: sort de la loop pour ce vol, on continue pas si on a pas de PV
    if print_debug:
        print(f'PV in ds.keys() AFTER get_PV: {cts.PV_VARNAME in list(ds.keys())}')
        print('---')
    return ds


# -------------------- LiNOx filters --------------------
def keep_cruise(ds, NOx_varname, print_debug=False):
    if print_debug:
        print(f'{NOx_varname}.notnull().sum() BEFORE cruise filter: {ds[NOx_varname].notnull().sum().values}')
    ds = ds.where(ds[f'{cts.AIRPRESS_VARNAME}_validity_flag'] == 0).where(ds[cts.AIRPRESS_VARNAME] < 30000)
    if print_debug:
        print(f'{NOx_varname}.notnull().sum() AFTER cruise filter: {ds[NOx_varname].notnull().sum().values}')
        print('---')
    return ds


def keep_tropo(ds, var_list, print_debug=False):
    for var in var_list:
        if print_debug:
            print(f'{var}.notnull().sum() BEFORE PV < 2 filter: {ds[var].notnull().sum().values}')
        ds[f"{var}_tropo"] = ds[var].where(ds[cts.PV_VARNAME] < 2)
        if print_debug:
            print(f'{var}_tropo.notnull().sum() AFTER PV < 2 filter: {ds[f"{var}_tropo"].notnull().sum().values}')
            print('---')
    return ds


def remove_CO_excess(ds, CO_q3, NOx_varname, CO_varname, print_debug=False):
    if print_debug:
        print(
            f'{NOx_varname}.notnull().sum() BEFORE strato + CO filter: {ds[f"{NOx_varname}"].notnull().sum().values}')
    ds[f'{NOx_varname}_tropo_CO_filter'] = ds[f'{NOx_varname}'].where(ds[CO_varname] < CO_q3)
    if print_debug:
        print(
            f'{NOx_varname}_tropo_CO_filtered.notnull().sum() AFTER strato + CO filter: {ds[f"{NOx_varname}_tropo_CO_filter"].notnull().sum().values}')
        print('---')
    return ds

#TODO: ajouter des attributs pour qu'on sache à quoi les variables correspondent (noms trop obscurs)
def keep_NOx_excess(ds, NOx_varname, NOx_q3, print_debug=False):
    if print_debug:
        print(
            f'{NOx_varname}_tropo_CO_filter.notnull().sum() BEFORE NOx q3 filter: {ds[f"{NOx_varname}_tropo_CO_filter"].notnull().sum().values}')
    ds[f'{NOx_varname}_filtered'] = ds[f'{NOx_varname}_tropo_CO_filter'] \
        .where(ds[f'{NOx_varname}_tropo_CO_filter'] > NOx_q3)
    if print_debug:
        print(
            f'{NOx_varname}_filtered.notnull().sum() AFTER NOx q3 filter: {ds[f"{NOx_varname}_filtered"].notnull().sum().values}')
        print('---')
    return ds

#TODO: poubelle si on fait pas avec les régions
def get_q3_attrs(ds, q3_ds):
    q3_attrs = {}
    for month in np.unique(ds.UTC_time.dt.month):
        for geo_region in np.unique(ds.geo_region):
            q3_attrs[f'{month}_{geo_region}_CO_q3'] = "{:.3f}".format(float(
                q3_ds['CO_q3'].sel(month=month, geo_region=geo_region).values))
            q3_attrs[f'{month}_{geo_region}_NOx_q3'] = "{:.3f}".format(float(
                q3_ds['NOx_q3'].sel(month=month, geo_region=geo_region).values))
    return q3_attrs



params = {'legend.fontsize': 'x-large',
          'figure.figsize': (20, 6),
          'axes.labelsize': 'x-large',
          'axes.titlesize': 'x-large',
          'xtick.labelsize': 'x-large',
          'ytick.labelsize': 'x-large'}


# TODO: poubelle ensuite, c'est juste pour les tests là (ou alors mettre au propre si je laisse)
def plot_NOx_CO_PV_RHL_O3(ds, q3_ds, NOx_plumes=False, NOx_tropo=False, NOx_spike=False,
                          NOx_spike_id=[], show_region_names=False,
                          PV=False, RHL=False, CO=True, O3=False, scatter_NOx_tropo=False, scatter_NOx_excess=False,
                          params=params, save_fig=False, plot_dirpath=None, show_fig=False, x_axis='UTC_time',
                          x_lim=None, title=None, fig_name=None, fig_name_prefix='', fig_name_suffix=''):
    plt.rcParams.update(params)
    fig, ax1 = plt.subplots()

    if x_axis == 'lon':
        x_label = 'Longitude (°)'
    elif x_axis == 'UTC_time':
        x_label = 'UTC_time'
    else:
        raise ValueError(f'Invalid x_axis dimension name ({x_axis})')

    if x_lim is None:
        ax1.set_xlim([ds[x_axis].min().values, ds[x_axis].max().values])
    else:
        ax1.set_xlim(x_lim)

    title_suffix = ''

    # PV
    if PV:
        ax_PV = ax1.twinx()
        ax_PV.plot(ds[x_axis], ds['PV'] * 100, color='tab:gray', label='PV * 100')
        ax_PV.set_ylim([0, 200])
        ax_PV.get_yaxis().set_ticks([])
        ax_PV.set_zorder(0)

    flight_program = ds.attrs['program']
    if isinstance(q3_ds, xr.Dataset):
        q3_ds = q3_ds.sel(geo_region=ds['geo_region'], month=ds['UTC_time'].dt.month)

    # CO
    if CO:
        CO_varname = get_CO_varname(flight_program=flight_program, tropo=False, filtered=False)
        if not np.isnan(ds[CO_varname]).all():
            ax_CO = ax1.twinx()
            ax_CO.set_ylabel('CO (ppb)')
            ax_CO.tick_params(axis='y', colors='tab:cyan')
            CO_plot = ax_CO.plot(ds[x_axis], ds[CO_varname].where(ds['PV'] < 2), color='tab:cyan', label='CO tropo')
            if isinstance(q3_ds, xr.Dataset):
                ax_CO.plot(ds[x_axis], q3_ds['CO_q3'], color='tab:cyan', linestyle='--')
            else:
                ax_CO.axhline(y=q3_ds['CO_q3'], linestyle='--', color='tab:cyan')
            ax_CO.set_zorder(1)
        else:
            title_suffix = '\n<!> CO values all nan <!>'

    # O3
    if O3:
        if not np.isnan(ds['O3_P1']).all():
	        ax_O3 = ax1.twinx()
	        ax_O3.plot(ds[x_axis], ds['O3_P1'].where(ds['PV'] < 2), label='O3 tropo', color='tab:blue', alpha=0.85)
	        """if CO:
	            label = 'CO & O3 (ppb)'
	        else:"""
	        label = 'O3 (ppb)'
	        ax_O3.set_ylabel(label)
	        ax_O3.set_ylim([0, ds['O3_P1'].where(ds['PV'] < 2).max().values + 0.05])
	        ax_O3.tick_params(axis='y', colors='tab:blue')
	        ax_O3.set_zorder(1)
	        if CO:
	            ax_O3.spines.right.set_position(("axes", 1.075))
        else:
            if len(title_suffix) > 0:
                title_suffix = ' | <!> O3 values all nan <!>'	
            else:
                title_suffix = '\n<!> O3 values all nan <!>'	
            

    # RHL
    if RHL:
        ax_RHL = ax1.twinx()
        ax_RHL.plot(ds[x_axis], ds['RHL_P1'].where(ds['PV'] < 2), label='RHL tropo', color='tab:purple', alpha=0.85)
        ax_RHL.set_ylabel('RHL')
        ax_RHL.set_ylim([0, ds['RHL_P1'].max().values + 0.05])
        ax_RHL.tick_params(axis='y', colors='tab:purple')
        ax_RHL.set_zorder(0.5)
        if CO or O3:
            ax_RHL.spines.right.set_position(("axes", 1.15))

    # NOx
    # setup NOx axis
    if NOx_tropo:
        NOx_tropo_varname = get_NOx_varname(flight_program=flight_program, smoothed=True, tropo=True, filtered=False)
        if scatter_NOx_tropo:
            ax1.scatter(ds[x_axis], ds[NOx_tropo_varname], color='tab:green', label='NOx_tropo',
                        linewidths=0.5)
        else:
            ax1.plot(ds[x_axis], ds[NOx_tropo_varname], color='tab:green', label='NOx_tropo')
    NOx_tropo_filtered_varname = get_NOx_varname(flight_program=flight_program, smoothed=True, tropo=True, filtered=True)
    if not np.isnan(ds[NOx_tropo_filtered_varname]).all():
        if scatter_NOx_excess:
            NOx_plot = ax1.scatter(ds[x_axis], ds[NOx_tropo_filtered_varname], color='red',
                                   label='NOx_filtered')
        else:
            NOx_plot = ax1.plot(ds[x_axis], ds[NOx_tropo_filtered_varname], color='red',
                                label='NOx_filtered')
        ax1.set_ylim([0, ds[NOx_tropo_varname].max().values + 0.05])
    else:
        title_suffix += '\n<!> NOx tropo values all nan <!>'

    if NOx_spike and len(NOx_spike_id) > 0:
        ax1.scatter(ds[x_axis].isel(UTC_time=NOx_spike_id),
                    ds[NOx_tropo_filtered_varname].isel(UTC_time=NOx_spike_id), color='lime',
                    label='aircraft_spike', linewidths=0.75, marker='o', edgecolor='black', s=70)

    if NOx_plumes:
        if not (np.isnan(ds[cts.NOx_PLUME_ID_VARNAME].where(ds[cts.NOx_PLUME_ID_VARNAME] > 0)).all()):
            for plume_id in np.unique(ds[cts.NOx_PLUME_ID_VARNAME]):
                if plume_id > 0:
                    plume_ds_UTC_time_array = ds.where(ds[cts.NOx_PLUME_ID_VARNAME] == plume_id, drop=True)['UTC_time'].values
                    ax1.axvspan(plume_ds_UTC_time_array[0], plume_ds_UTC_time_array[-1], color='orangered', alpha=0.5)

    if show_region_names:
        for geo_region in np.unique(ds['geo_region']):
            reg_ds_time_array = ds.where(ds['geo_region'] == geo_region, drop=True)['UTC_time'].values
            if geo_region != 'NONE':
                mid_time = reg_ds_time_array[0] + (reg_ds_time_array[-1] - reg_ds_time_array[0]) / 2
            else:
                mid_time = reg_ds_time_array[0]
            plt.text(x=mid_time, y=0.75, s=geo_region, fontsize=20)

    ax1.set_ylabel('NOx (ppb)')
    ax1.set_xlabel(x_label)
    ax1.tick_params(axis='y', colors='red')
    if isinstance(q3_ds, xr.Dataset):
        ax1.plot(q3_ds[x_axis], q3_ds['NOx_q3'], color='red', linestyle='--')
    else:
        ax1.axhline(y=q3_ds['NOx_q3'], linestyle='--', color='red')
    ax1.set_zorder(3)
    ax1.patch.set_visible(False)  # pour que fond soit transparent sinon on voit pas ce qu'il y a derrière

    # legend
    fig.legend(loc='upper left')

    # title
    if title is None or title.lower() == "default":
        plt.title(f'flight {ds.attrs["flight_name"]} - {ds.attrs["departure_airport"]} --> {ds.attrs["arrival_airport"]} - CO_q3: {q3_ds["CO_q3"]} - NOx_q3: {q3_ds["NOx_q3"]:.4f} {title_suffix}')
    else:
        plt.title(f'{title} {title_suffix}')
    # save fig
    if save_fig:
        if fig_name is None:
            fig_name = f'{fig_name_prefix}{ds.attrs["flight_name"]}_NOx_'
            if CO:
                fig_name += 'CO_'
            if PV:
                fig_name += 'PV_'
            if RHL:
                fig_name += 'RH_'
            if O3:
                fig_name += 'O3_'
            fig_name += f'over_{x_axis}{fig_name_suffix}.png'
        else:
            fig_name = f'{fig_name_prefix}{fig_name}{fig_name_suffix}'
            if not '.png' in fig_name:
                fig_name = f'{fig_name}.png'
        plt.savefig(f'{plot_dirpath}/{fig_name}')
        print(f'Saved plot {plot_dirpath}/{fig_name}')
    # show fig
    if show_fig:
        plt.show()
    plt.close(fig)
