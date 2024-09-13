import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import pathlib
from re import match
import warnings
import xarray as xr

from . import constants as cts


def get_NOx_varname(flight_program, smoothed, tropo, filtered):
    if filtered:
        return cts.NOx_FILTERED_VARNAME
    elif smoothed:
        NOx_varname = cts.NOx_SMOOTHED_VARNAME
        if tropo:
            NOx_varname = f'{NOx_varname}_tropo'
        return NOx_varname

    elif flight_program is not None:
        # if "raw" NOx values
        if flight_program == cts.CORE or flight_program == f'{cts.IAGOS}-{cts.CORE}':
            return cts.CORE_NOx_VARNAME
        elif flight_program == cts.CARIBIC or flight_program == f'{cts.IAGOS}-{cts.CARIBIC}':
            return cts.CARIBIC_NOx_VARNAME
        else:
            raise KeyError(f'{flight_program} {cts.FLIGHT_PROGRAM_KEYERROR_MSG}')

    else:
        raise ValueError(f'Cannot find NOx varname')


def get_CO_varname(flight_program, smoothed, tropo):
    if smoothed:  # when smoothed, same variable name no matter the program
        CO_varname = cts.CO_SMOOTHED_VARNAME
        if tropo:
            CO_varname = f'{CO_varname}_tropo'
        return CO_varname

    elif flight_program:
        if flight_program == cts.CORE or flight_program == f'{cts.IAGOS}-{cts.CORE}':
            return cts.CO_VARNAME
        elif flight_program == cts.CARIBIC or flight_program == f'{cts.IAGOS}-{cts.CARIBIC}':
            return cts.CARIBIC_CO_VARNAME
        else:
            raise KeyError(f'{flight_program} {cts.FLIGHT_PROGRAM_KEYERROR_MSG}')

    else:
        raise ValueError(f'Cannot find CO varname')


def get_O3_varname(flight_program, tropo):
    if tropo:
        return cts.O3_TROPO_VARNAME

    elif flight_program:
        if flight_program == cts.CORE or flight_program == f'{cts.IAGOS}-{cts.CORE}':
            return cts.O3_VARNAME
        elif flight_program == cts.CARIBIC or flight_program == f'{cts.IAGOS}-{cts.CARIBIC}':
            return cts.CARIBIC_O3_VARNAME
        else:
            raise KeyError(f'{flight_program} {cts.FLIGHT_PROGRAM_KEYERROR_MSG}')
    else:
        raise ValueError(f'Cannot find O3 varname')


def get_var_list(flight_program):
    """
    Returns list of variables names corresponding to a given flight program (air pressure, CO, O3, NO, NO2 and NOx)
    :param flight_program:
    :return: <list> [ <str>, ..., <str> ]
    """
    if flight_program == cts.CORE or flight_program == f'{cts.IAGOS}-{cts.CORE}':
        var_list = [cts.CO_VARNAME, cts.O3_VARNAME, cts.CORE_NO_VARNAME, cts.CORE_NO2_VARNAME, cts.CORE_NOx_VARNAME, cts.AIRPRESS_VARNAME]
    elif flight_program == cts.CARIBIC or flight_program == f'{cts.IAGOS}-{cts.CARIBIC}':
        var_list = [cts.CARIBIC_CO_VARNAME, cts.CARIBIC_O3_VARNAME, cts.CARIBIC_NO_VARNAME, cts.CARIBIC_NO2_VARNAME, cts.AIRPRESS_VARNAME]
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
                                   flight_type=None, flight_id_list=None, airports_list=cts.SOFTIOLI_AIRPORTS, print_debug=False):
    """
    Returns list of NOx flights urls (from iagos v3 catalogue) between
    :param iagos_cat_path:
    :param start_flight_id:
    :param end_flight_id:
    :param flight_type:
    :param flight_id_list:
    @param airports_list:
    :return:
    """
    if not pathlib.Path(iagos_cat_path).exists():
        raise ValueError(f"Invalid IAGOS catalogue path, {iagos_cat_path} does NOT exist!")
    df_cat = pd.read_parquet(iagos_cat_path)
    # get all NO2 data variable names
    NO2_vars = [varname for varname in df_cat.columns if match(r'data_vars_NO2_P\w{2}$', varname)]
    # get all NOx flights (flights with NO2 variables > 0)
    NOx_flights = df_cat.loc[(df_cat[NO2_vars] > 0).any(axis='columns')]
    # if we only want flights between two flight ids or specific flights in a given list or in specific region
    if flight_id_list is not None:
        NOx_flights = NOx_flights.loc[flight_id_list]
    elif (start_flight_id is not None) and (end_flight_id is not None):
        NOx_flights = NOx_flights.loc[start_flight_id:end_flight_id]
    elif airports_list is not None:
        NOx_flights = NOx_flights.loc[(NOx_flights['attrs_departure_airport'].isin(airports_list) &
                                       NOx_flights['attrs_arrival_airport'].isin(airports_list))]
    # if we only want flights from one specific program
    if flight_type is not None:
        if flight_type in ['CARIBIC', 'CORE', 'MOZAIC']:
            flight_type = f'IAGOS-{flight_type}'
        elif flight_type not in ['IAGOS-CARIBIC', 'IAGOS-CORE', 'IAGOS-MOZAIC']:
            raise ValueError(
                f"Invalid IAGOS program type ({flight_type}), expecting any of the following: ['IAGOS-CARIBIC', 'CARIBIC', 'IAGOS-CORE', 'CORE', 'IAGOS-MOZAIC', 'MOZAIC']")
        NOx_flights = NOx_flights.loc[NOx_flights.attrs_program == flight_type]

    if print_debug:
        print(f'len(NOx_flights) = {len(NOx_flights.general_url)}')
        print('---')

    return NOx_flights.general_url


# --------------- PV ---------------
def find_PV_file_in_PV_dir(pv_dir_path, flight_name):
    pv_dir_path = pathlib.Path(pv_dir_path)
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
    d_time = pd.Timestamp(d_time)
    pv_dir_path = pathlib.Path(f'{cts.IAGOSv3_PV_PATH}/{d_time.year}{d_time.month:02d}')
    if not pv_dir_path.exists():
        raise FileNotFoundError(f'Unable to retrieve PV values: directory {pv_dir_path} does NOT exist')
    else:
        return find_PV_file_in_PV_dir(pv_dir_path=pv_dir_path, flight_name=flight_name)


def add_PV(ds):
    pv_file_path = get_PV_file_path(d_time=ds.attrs['departure_UTC_time'], flight_name=ds.attrs["flight_name"])
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
def keep_cruise(ds, print_debug=False):
    if print_debug:
        print(f'{cts.NOx_SMOOTHED_VARNAME}.notnull().sum() BEFORE cruise filter: {ds[cts.NOx_SMOOTHED_VARNAME].notnull().sum().values}')
    ds = ds.where(ds[f'{cts.AIRPRESS_VARNAME}_validity_flag'] == 0).where(ds[cts.AIRPRESS_VARNAME] < 30000)
    if print_debug:
        print(f'{cts.NOx_SMOOTHED_VARNAME}.notnull().sum() AFTER cruise filter: {ds[cts.NOx_SMOOTHED_VARNAME].notnull().sum().values}')
        print('---')
    return ds


def keep_tropo(ds, var_list, print_debug=False):
    for var in var_list:
        if print_debug:
            print(f'{var}.notnull().sum() BEFORE PV < 2 filter: {ds[var].notnull().sum().values}')
        if "O3" in var:
            ds[cts.O3_TROPO_VARNAME] = ds[var].where(ds[cts.PV_VARNAME] < 2)
        else:
            ds[f"{var}_tropo"] = ds[var].where(ds[cts.PV_VARNAME] < 2)
        if print_debug:
            if "O3" in var:
                print(f'{var}_tropo.notnull().sum() AFTER PV < 2 filter: {ds[cts.O3_TROPO_VARNAME].notnull().sum().values}')
            else:
                print(f'{var}_tropo.notnull().sum() AFTER PV < 2 filter: {ds[f"{var}_tropo"].notnull().sum().values}')
            print('---')
    return ds


def remove_CO_excess(ds, CO_q3, NOx_varname, CO_varname, print_debug=False):
    if print_debug:
        print(
            f'{NOx_varname}.notnull().sum() BEFORE strato + CO filter: {ds[f"{NOx_varname}"].notnull().sum().values}')
    ds[cts.NOx_FILTERED_VARNAME] = ds[f'{NOx_varname}'].where(ds[CO_varname] < CO_q3)
    if print_debug:
        print(
            f'{cts.NOx_FILTERED_VARNAME}.notnull().sum() AFTER strato + CO filter: {ds[cts.NOx_FILTERED_VARNAME].notnull().sum().values}')
        print('---')
    return ds

#TODO: ajouter des attributs pour qu'on sache à quoi les variables correspondent (noms trop obscurs)
def keep_NOx_excess(ds, NOx_varname, NOx_q3, print_debug=False):
    if print_debug:
        print(
            f'{NOx_varname}.notnull().sum() BEFORE NOx q3 filter: {ds[NOx_varname].notnull().sum().values}')
    ds[NOx_varname] = ds[NOx_varname] \
        .where(ds[NOx_varname] > NOx_q3)
    if print_debug:
        print(
            f'{NOx_varname}.notnull().sum() AFTER NOx q3 filter: {ds[NOx_varname].notnull().sum().values}')
        print('---')
    return ds


def get_spike_indices(NOx_filtered_da, window_size, roll_mean_multiplier=1.5):
    roll_mean = NOx_filtered_da.rolling(UTC_time=window_size, min_periods=1).mean().values
    spike_id = []
    spike_offset = 0
    NOx_filtered_array = NOx_filtered_da.values
    for i in range(len(roll_mean) - 1):
        # compare NOx_filtered[i+1] to rolling mean[i] * 1.5 (or another coefficient)
        # spike_offset so that we keep comparing spike values to the same "normal" values
        if NOx_filtered_array[i + 1] > (roll_mean_multiplier * roll_mean[i - spike_offset]):
            spike_id.append(i + 1)
            spike_offset += 1
        else:  # if not spike, spike_offset bask to 0
            spike_offset = 0
    return spike_id


def remove_aircraft_spikes(ds, print_debug=False):
    spike_indices = get_spike_indices(NOx_filtered_da=ds[cts.NOx_FILTERED_VARNAME],
                                      window_size=cts.WINDOW_SIZE[ds.attrs[cts.PROGRAM_ATTR]],
                                      roll_mean_multiplier=1.5)
    ds[cts.AIRCRAFT_SPIKE_VARNAME] = xr.DataArray(np.full(ds.sizes['UTC_time'], False), dims='UTC_time')
    ds[cts.AIRCRAFT_SPIKE_VARNAME][spike_indices] = True
    if print_debug:
        print(
            f'{cts.NOx_FILTERED_VARNAME}.notnull().sum() BEFORE spike filter: {ds[cts.NOx_FILTERED_VARNAME].notnull().sum().values}')
    ds[cts.NOx_FILTERED_VARNAME] = ds[cts.NOx_FILTERED_VARNAME].where(ds[cts.AIRCRAFT_SPIKE_VARNAME] == False, drop=True)
    if print_debug:
        print(
            f'{cts.NOx_FILTERED_VARNAME}.notnull().sum() AFTER spike filter: {ds[cts.NOx_FILTERED_VARNAME].notnull().sum().values}')
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
def plot_NOx_CO_PV_RHL_O3(ds, q3_ds, plot_q3=True, NOx_plumes=False, NOx_tropo=False, NOx_tropo_filtered=False,
                          raw_NOx=False, NOx_smoothed=True, NOx_spike=False, NOx_spike_id=[], show_region_names=False,
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

    flight_program = ds.attrs[cts.PROGRAM_ATTR]
    if plot_q3:
        if isinstance(q3_ds, xr.Dataset):
            q3_ds = q3_ds.sel(geo_region=ds['geo_region'], month=ds['UTC_time'].dt.month)

    # CO
    if CO:
        CO_varname = get_CO_varname(flight_program=flight_program, tropo=False, smoothed=True)
        if not np.isnan(ds[CO_varname]).all():
            ax_CO = ax1.twinx()
            ax_CO.set_ylabel('CO (ppb)')
            ax_CO.tick_params(axis='y', colors='tab:cyan')
            CO_plot = ax_CO.plot(ds[x_axis], ds[CO_varname].where(ds['PV'] < 2), color='tab:cyan', label='CO tropo')
            if plot_q3:
                if isinstance(q3_ds, xr.Dataset):
                    ax_CO.plot(ds[x_axis], q3_ds['CO_q3'], color='tab:cyan', linestyle='--')
                else:
                    ax_CO.axhline(y=q3_ds['CO_q3'], linestyle='--', color='tab:cyan')
            ax_CO.set_zorder(1)
        elif np.isnan(ds[CO_varname].where(ds['PV'] < 2)).all():
            title_suffix = '\n<!> CO tropo values all nan <!>'
        else:
            title_suffix = '\n<!> CO values all nan <!>'

    # O3
    if O3:
        O3_varname = get_O3_varname(ds.attrs[cts.PROGRAM_ATTR], tropo=False)
        if not np.isnan(ds[O3_varname]).all():
            ax_O3 = ax1.twinx()
            ax_O3.plot(ds[x_axis], ds[O3_varname].where(ds['PV'] < 2), label='O3 tropo', color='tab:blue', alpha=0.85)
            label = 'O3 (ppb)'
            ax_O3.set_ylabel(label)
            if not np.isnan(ds[O3_varname].where(ds['PV'] < 2)).all():
                ax_O3.set_ylim([0, ds[O3_varname].where(ds['PV'] < 2).max().values + 5])
            else:
                if len(title_suffix) > 0:
                    title_suffix = ' | <!> O3 tropo values all nan <!>'
                else:
                    title_suffix = '\n<!> O3 tropo values all nan <!>'
            """else: en fait j'avais fait ca parce que sinon O3 trop grand à cause strato (je pense?)
                ax_O3.set_ylim([0, 200]) #TODO: valeur arbitraire pour que ça plot même si pas O3 dans tropo"""
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
        if 'RHL_P1' in list(ds.keys()):
            ax_RHL = ax1.twinx()
            ax_RHL.plot(ds[x_axis], ds['RHL_P1'].where(ds['PV'] < 2), label='RHL tropo', color='tab:purple', alpha=0.85)
            ax_RHL.set_ylabel('RHL')
            """ pas besoin en fait: ax_RHL.set_ylim([0, ds['RHL_P1'].max().values + 0.05])"""
            ax_RHL.tick_params(axis='y', colors='tab:purple')
            ax_RHL.set_zorder(0.5)
            if CO or O3:
                ax_RHL.spines.right.set_position(("axes", 1.15))

    # NOx
    # setup NOx axis
    if raw_NOx:
        NOx_varname = get_NOx_varname(flight_program=flight_program, smoothed=NOx_smoothed, tropo=False, filtered=False)
        if not np.isnan(ds[NOx_varname]).all():
            ax1.plot(ds[x_axis], ds[NOx_varname], color='tab:green', label='NOx_smoothed (NOT filtered)')
        else:
            title_suffix += '\n<!> NOx smoothed values all nan <!>'

    if NOx_tropo:
        NOx_tropo_varname = get_NOx_varname(flight_program=flight_program, smoothed=True, tropo=True, filtered=False)
        if not np.isnan(ds[NOx_tropo_varname]).all():
            if scatter_NOx_tropo:
                ax1.scatter(ds[x_axis], ds[NOx_tropo_varname], color='tab:green', label='NOx_tropo',
                            linewidths=0.5)
            else:
                ax1.plot(ds[x_axis], ds[NOx_tropo_varname], color='tab:green', label='NOx_tropo')
        else:
            title_suffix += '\n<!> NOx tropo values all nan <!>'

    if NOx_tropo_filtered:
        NOx_tropo_filtered_varname = get_NOx_varname(flight_program=flight_program, smoothed=True, tropo=True,
                                                     filtered=True)
        if not np.isnan(ds[NOx_tropo_filtered_varname]).all():
            if scatter_NOx_excess:
                NOx_plot = ax1.scatter(ds[x_axis], ds[NOx_tropo_filtered_varname], color='red',
                                       label='NOx_filtered')
            else:
                NOx_plot = ax1.plot(ds[x_axis], ds[NOx_tropo_filtered_varname], color='red',
                                    label='NOx_filtered')
            ax1.set_ylim([0, ds[cts.NOx_SMOOTHED_TROPO_VARNAME].max().values + 0.05])

    if NOx_spike:
        if len(NOx_spike_id) > 0:
            ax1.scatter(ds[x_axis].isel(UTC_time=NOx_spike_id),
                        ds[cts.NOx_FILTERED_VARNAME].isel(UTC_time=NOx_spike_id), color='lime',
                        label='aircraft_spike', linewidths=0.75, marker='o', edgecolor='black', s=70)
        else:
            ax1.scatter(ds[x_axis].where(ds[cts.AIRCRAFT_SPIKE_VARNAME], drop=True),
                        ds[cts.NOx_SMOOTHED_TROPO_VARNAME].where(ds[cts.AIRCRAFT_SPIKE_VARNAME], drop=True), color='lime',
                        label='aircraft_spike', linewidths=0.75, marker='o', edgecolor='black', s=70)

    if NOx_plumes:
        if not (np.isnan(ds[cts.NOx_PLUME_ID_VARNAME].where(ds[cts.NOx_PLUME_ID_VARNAME] > 0)).all()):
            for plume_id in np.unique(ds[cts.NOx_PLUME_ID_VARNAME]):
                if plume_id > 0:
                    plume_ds_UTC_time_array = ds.where(ds[cts.NOx_PLUME_ID_VARNAME] == plume_id, drop=True)[
                        'UTC_time'].values
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
    if plot_q3:
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
        if not plot_q3:
            q3_ds = {'CO_q3': 0, 'NOx_q3': 0}
        plt.title(
            f'flight {ds.attrs["flight_name"]} - {ds.attrs["departure_airport"]} --> {ds.attrs["arrival_airport"]} - CO_q3: {q3_ds["CO_q3"]} - NOx_q3: {q3_ds["NOx_q3"]:.4f} {title_suffix}')
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
