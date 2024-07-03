"""
Main objective:
- look at each flight in the catalogue, detect if NOx anomalies and store information about the anomaly (date, location, etc.)

A faire annexe:
-réfléchir à l'architecture des fichiers pour récup les résultats FP + garder les infos plume qqpart
- Où et comment stocker les infos plume (JSON ou autre)

Main steps:
- get NOx flights from catalogue (<!> regarder si NOx flights ont déjà été analysés ou pas)
        --> COMMENT ? check si flight déjà dans flight info
            --> réfléchir architecture résultat write plume info JSP COMMENT FAIRE POUR LE MOMENT

POUR CHAQUE VOL:
- get flight ds
        --> open IAGOS netcdf file
        --> only keep values with validity flag = 0 --> <!> variable names != if MOZAIC/CORE ou CARIBIC
        --> get pv
        --> get rolling mean NOx
        --> apply filters on smoothed data
        :return: filtered_ds
- find plumes
        --> get labeled array (using scipy function)
        --> get start_id and end_id from labeled array
        --> merge plumes if start_id[i+1] - end_id[i] < 100 seconds (ou 200 ???)
        --> create new data variable to store plume_id
        --> remove plumes smaller than 25 km from the list (put their id to -1)
- write plume info + plot
        --> pour chaque vol --> dossier dans lequel on a:
            - fichier json avec plume info (<?> à voir, faut qu'on puisse loop through les différents panaches si plusieurs)
            - plot plume
            - logs ??
            - config FP
            - résultats FP
        ?? why not semble ok je pense, y re-réfléchir si besoin

- fonction "main":
    --> récup NOx flights et/ou take list of NOx_flights en entrée
    --> for flight in NOx_flight_list:
        --> get_flight_ds (get_valid_data + get_PV + apply_LiNOx_plume_filters)
        --> find plumes
        --> write plume info qqpart

"""
import numpy as np
import pandas as pd
from scipy.ndimage import label
import xarray as xr

from common.utils import timestamp_now_formatted

from utils import constants as cts
from utils import iagos_utils, regions_utils
from utils.common_coords import GEO_REGIONS
from utils.utils_functions import write_plume_info_to_json_file


def get_flight_ds(flight_path, geo_regions_dict=GEO_REGIONS, print_debug=False):
    # return flight ds with PV and only valid data
    if isinstance(flight_path, xr.Dataset):
        ds = flight_path  # in case we give the dataset directly instead of the path
    else:
        ds = xr.open_dataset(flight_path)
    ds = iagos_utils.get_valid_data(var_list=iagos_utils.get_var_list(flight_program=ds.attrs['program']), ds=ds,
                                    print_debug=print_debug)
    ds = iagos_utils.get_PV(ds=ds, print_debug=print_debug)

    NOx_varname = iagos_utils.get_NOx_varname(flight_program=ds.attrs['program'], tropo=False, smoothed=False, filtered=False)
    # if CARIBIC flight --> calculate NOx variable from NO and NO2 measurements
    if ds.attrs['program'] == f'{cts.IAGOS}-{cts.CARIBIC}':
        ds[NOx_varname] = ds[cts.CARIBIC_NO_VARNAME] + ds[cts.CARIBIC_NO2_VARNAME]
    # smooth NOx timeseries (rolling mean with window size = min plume length)
    NOx_smoothed_varname = iagos_utils.get_NOx_varname(flight_program=ds.attrs['program'], smoothed=True,
                                                       tropo=False, filtered=False)
    ds[NOx_smoothed_varname] = ds[NOx_varname] \
        .rolling(UTC_time=cts.WINDOW_SIZE[ds.attrs['program']], min_periods=1) \
        .mean()
    # add regions to each data point
    ds = regions_utils.assign_geo_region_to_ds(ds=ds, geo_regions_dict=geo_regions_dict)

    return ds


def apply_LiNOx_plume_filters(ds, cruise_only, smoothed_data, CO_q3, q3_ds_path=None, print_debug=False):
    """
    Function to apply filters on the NOx timeseries to remove stratospheric, anthropogenic and background influence.
    If cruise_only = True: only keep data where air pressure < 30000 Pa
    - Stratospheric influence: remove NOx data where PV > 2
    - Anthropogenic influence: remove NOx data vhere CO value > CO_q3
    - Background influence: remove NOx data < NOx_q3 (only keep NOx excess)
    :param ds: <xarray.Dataset> flight ds
    :param cruise_only: <bool> if True, only keep data where air pressure < 30000 Pa
    :param smoothed_data: <bool> if True, apply filters on smoothed NOx timeseries (rolling mean)
    :param q3_ds_path: <str> or <pathlib.Path> url to q3_ds netcdf file
    :param print_debug: <bool> for testing purposes, print debug
    :return: <xarray.Dataset> filtered version of flight ds
    """
    NOx_varname = iagos_utils.get_NOx_varname(ds.attrs['program'], tropo=False, smoothed=smoothed_data, filtered=False)

    # only keep cruise data
    if cruise_only:
        ds = iagos_utils.keep_cruise(ds=ds, NOx_varname=NOx_varname, print_debug=print_debug)

    CO_varname = iagos_utils.get_CO_varname(ds.attrs['program'], tropo=False, filtered=False)

    # remove stratospheric influence
    ds = iagos_utils.keep_tropo(ds=ds, var_list=[NOx_varname, CO_varname], print_debug=print_debug)

    if CO_q3 is not None:
        q3_ds = { 'NOx_q3': cts.NOx_Q3, 'CO_q3': CO_q3 }
        ds.assign_attrs(q3_ds)
    else:
        # get q3_ds for ds month and geo region #TODO <!> mean('year')
        q3_ds_complete = xr.open_dataset(q3_ds_path).mean('year')
        q3_ds = q3_ds_complete.sel(geo_region=ds['geo_region'], month=ds['UTC_time'].dt.month)
        ds = ds.assign_attrs(iagos_utils.get_q3_attrs(ds=ds, q3_ds=q3_ds_complete))

    # remove anthropogenic influence
    NOx_varname = iagos_utils.get_NOx_varname(ds.attrs['program'], tropo=True, smoothed=smoothed_data, filtered=False)
    ds = iagos_utils.remove_CO_excess(ds=ds, CO_q3=q3_ds['CO_q3'], NOx_varname=NOx_varname,
                                      CO_varname=CO_varname, print_debug=print_debug)

    # remove background influence (keep NOx > q3)
    CO_varname = iagos_utils.get_CO_varname(ds.attrs['program'], tropo=True, filtered=False)
    ds = iagos_utils.keep_NOx_excess(ds=ds, NOx_varname=NOx_varname, NOx_q3=q3_ds['NOx_q3'], print_debug=print_debug)

    return ds


#TODO: min plume length CARIBIC et CORE différent peut-être ?
def find_plumes(ds, end_of_plume_duration=100, write_plume_info_to_json=False, json_path=None):
    NOx_varname = iagos_utils.get_NOx_varname(flight_program=ds.attrs['program'], tropo=True, smoothed=True, filtered=True)
    # get labeled array
    labeled, ncomponents = label(xr.where(ds[NOx_varname] > 0, True, False))
    # get start_id (min index of group) and end_id (max index of group) from labeled array
    start_id = [ np.min(np.nonzero(labeled == i)) for i in range(1, ncomponents+1) ]
    end_id = [ np.max(np.nonzero(labeled == i)) for i in range(1, ncomponents+1) ]
    # merge plumes if start_id[i+1] - end_id[i] < 100 seconds (ou 200 ???)
    id_offset = 0
    for i in range(len(start_id)-1):
        # if start next plume - end current plume < end of plume duration
        if ds['UTC_time'][start_id[i+1-id_offset]] - ds['UTC_time'][end_id[i-id_offset]] < pd.Timedelta(seconds=end_of_plume_duration):
            # "merge" both plumes (remove index end current plume and index start next plume
            del start_id[i+1-id_offset], end_id[i-id_offset]
            id_offset += 1
    # create new data variable to store plume_id
    ds[cts.NOx_PLUME_ID_VARNAME] = xr.DataArray(dims=['UTC_time'], coords={"UTC_time": ds.UTC_time})
    # remove plumes smaller than 25 km (100 seconds) from the list (put their id to -1)
    plume_id = 1
    min_plume_length = cts.WINDOW_SIZE[ds.attrs['program']] # min length = smoothing window size
    #TODO: <!> min length dépend nombre de data points != si CARIBIC
    for i in range(len(start_id)):
        # if plume too small (plume_length < min_plume_length) --> plume_id = -1
        if end_id[i] - start_id[i] < min_plume_length:
            ds[cts.NOx_PLUME_ID_VARNAME].isel(UTC_time=slice(start_id[i], end_id[i] + 1)).values[:] = -1
        else:
            ds[cts.NOx_PLUME_ID_VARNAME].isel(UTC_time=slice(start_id[i], end_id[i] + 1)).values[:] = plume_id
            plume_id += 1
    ds[cts.NOx_PLUME_ID_VARNAME].attrs = {"id_values": '[nan, -1, 1... ]',
                                 'id_meanings': ['not_a_plume', 'plume_too_small', 'plume_id']}

    if write_plume_info_to_json:
        plume_info_dict = {}
        if not (np.isnan(ds[cts.NOx_PLUME_ID_VARNAME].where(ds[cts.NOx_PLUME_ID_VARNAME] > 0)).all()):
            for plume_id, plume_array in ds[cts.NOx_PLUME_ID_VARNAME].groupby(
                    ds[cts.NOx_PLUME_ID_VARNAME].where(ds[cts.NOx_PLUME_ID_VARNAME] > 0)):
                #TODO: faire plus propre pour récup lon et lat varname
                plume_info_dict[int(plume_id)] = {
                    'start': str(plume_array.UTC_time[0].values),
                    'end': str(plume_array.UTC_time[-1].values),
                    'count': int(plume_array.count()),
                    'lon_start': str(plume_array.lon[0].values),
                    'lon_end': str(plume_array.lon[-1].values),
                    'lat_start': str(plume_array.lat[0].values),
                    'lat_end': str(plume_array.lat[-1].values),
                    'geo_region': str(np.unique(plume_array.geo_region))
                }
            print(plume_info_dict)
            #TODO: faire un truc plus propre pour si on a pas donné json_path mais pas le temps là
            write_plume_info_to_json_file(flight_name=ds.attrs['flight_name'],
                                          info_name=None, missing_ok=True,
                                          data={'total_plumes': len(plume_info_dict),
                                                'info_plumes': plume_info_dict},
                                          json_path=json_path
                                          )

    return ds


def get_LiNOX_plumes(start_flight_id=None, end_flight_id=None, flight_type=None, flight_id_list=None,
                     cruise_only=True, CO_q3=None, print_debug=False, filtered_ds_to_netcdf=False,
                     plume_ds_to_netcdf=False, plot_flight=False, save_fig=False, file_suffix='',
                     show_region_names=False):
    # get NOx flights url (L2 files)
    NOx_flights_url = iagos_utils.get_NOx_flights_from_catalogue(iagos_cat_path=cts.IAGOSv3_CAT_PATH,
                                                                 start_flight_id=start_flight_id,
                                                                 end_flight_id=end_flight_id, flight_type=flight_type,
                                                                 flight_id_list=flight_id_list)

    timenow = timestamp_now_formatted("%Y-%m-%d_%H%M", tz='CET')

    for flight_path in NOx_flights_url:
        if print_debug:
            print('##################################################')
            print(f'flight {flight_path}')
            print('##################################################')
        flight_ds = get_flight_ds(flight_path=flight_path, print_debug=print_debug)
        filtered_flight_ds = apply_LiNOx_plume_filters(ds=flight_ds, cruise_only=cruise_only, smoothed_data=True, CO_q3=CO_q3,  q3_ds_path=cts.Q3_DS_PATH, print_debug=print_debug)

        if filtered_ds_to_netcdf:
            filtered_flight_ds.to_netcdf(f'/o3p/patj/SOFT-IO-LI_output/2024-07-03_testsPlumeDetection/{timenow}_filtered-ds_{filtered_flight_ds.attrs["flight_name"]}{file_suffix}.nc')

        plume_ds = find_plumes(ds=filtered_flight_ds, write_plume_info_to_json=True,
                               json_path=f'/o3p/patj/SOFT-IO-LI_output/2024-07-03_testsPlumeDetection/{timenow}_plume_info{file_suffix}.json')
        if plume_ds_to_netcdf:
            plume_ds.to_netcdf(
            f'/o3p/patj/SOFT-IO-LI_output/2024-07-03_testsPlumeDetection/{timenow}_plume-ds_{filtered_flight_ds.attrs["flight_name"]}{file_suffix}.nc')

        if plot_flight: #TODO: for testing purposes, si ça se trouve q3_ds sert à rien en fait
            if CO_q3 is not None:
                q3_ds = { 'NOx_q3': cts.NOx_Q3, 'CO_q3': CO_q3 }
            else:
                q3_ds = xr.open_dataset(cts.Q3_DS_PATH).mean('year')
            iagos_utils.plot_NOx_CO_PV_RHL_O3(ds=plume_ds, q3_ds=q3_ds,
                                              NOx_plumes=True, NOx_tropo=True, show_region_names=show_region_names,
                                              NOx_spike=False, NOx_spike_id=[],
                                              PV=True, RHL=True, CO=True, O3=True,
                                              scatter_NOx_tropo=False, scatter_NOx_excess=False,
                                              save_fig=save_fig, show_fig=True,
                                              fig_name=None, fig_name_prefix=f'{timenow}_', fig_name_suffix=file_suffix,
                                              plot_dirpath='/o3p/patj/SOFT-IO-LI_output/2024-07-03_testsPlumeDetection/',
                                              x_axis='UTC_time', x_lim=None, title=None)



if __name__ == "__main__":
    # TODO: regarder si OK que si id = int ou si ok quand id = str
    CO_q3 = 120
    get_LiNOX_plumes(flight_id_list=['2018060302172202', '2018060508235702', '2018060522312902', '2018060612335502', '2018060702191102', '2018061013043302', '2018061102095702', '2018061712343202', '2018061802164802', '2018062312572002', '2018062402164402', '2018062713274902'],
                     CO_q3=CO_q3, show_region_names=False,
                     print_debug=True, save_fig=True, file_suffix=f'_COq3-{CO_q3}_NOxq3-{cts.NOx_Q3}',
                     filtered_ds_to_netcdf=False, plume_ds_to_netcdf=True, plot_flight=True)

    """iagos_utils.plot_NOx_CO_PV_RHL_O3(
        ds=xr.open_dataset('/o3p/patj/SOFT-IO-LI_output/2024-07-03_testsPlumeDetection/2024-07-03_1131_plume-ds_2018060508235702.nc'),
                                      q3_ds=xr.open_dataset(cts.Q3_DS_PATH).mean('year'),
                                      NOx_plumes=True, NOx_tropo=True, region_names=True,
                                      NOx_spike=False, NOx_spike_id=[],
                                      PV=True, RHL=True, CO=True, O3=True,
                                      scatter_NOx_tropo=False, scatter_NOx_excess=False,
                                      save_fig=False, fig_name=None, fig_name_prefix=None,
                                      plot_dirpath='/o3p/patj/SOFT-IO-LI_output/2024-07-03_testsPlumeDetection/',
                                      show_fig=True, x_axis='UTC_time', x_lim=None, title=None)"""


# /o3p/iagos/iagosv3/L2/201806/IAGOS_timeseries_2018060302172202_L2_3.1.2.nc4
#get_LiNOX_plumes(flight_id_list=['2018060302172202', '2018060508235702'], print_debug=True,
#                     filtered_ds_to_netcdf=False, plume_ds_to_netcdf=True)
#['2018060302172202', '2018060508235702', '2018060522312902', '2018060612335502', '2018060702191102', '2018061013043302', '2018061102095702', '2018061712343202', '2018061802164802', '2018062312572002', '2018062402164402', '2018062713274902']