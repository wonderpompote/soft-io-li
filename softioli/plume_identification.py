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
import xarray as xr

from common.utils import timestamp_now_formatted

from utils import constants as cts
from utils import iagos_utils, regions_utils
from utils.common_coords import GEO_REGIONS


def get_flight_ds(flight_path, geo_regions_dict=GEO_REGIONS, print_debug=False):
    # return flight ds with PV and only valid data
    if isinstance(flight_path, xr.Dataset):
        ds = flight_path  # in case we give the dataset directly instead of the path
    else:
        ds = xr.open_dataset(flight_path)
    ds = iagos_utils.get_valid_data(var_list=iagos_utils.get_var_list(flight_program=ds.attrs['program']), ds=ds,
                                    print_debug=print_debug)
    ds = iagos_utils.get_PV(ds=ds, print_debug=print_debug)

    NOx_varname = iagos_utils.get_NOx_varname(flight_program=ds.attrs['program'], smoothed=False, filtered=False)
    # if CARIBIC flight --> calculate NOx variable from NO and NO2 measurements
    if ds.attrs['program'] == f'{cts.IAGOS}-{cts.CARIBIC}':
        ds[NOx_varname] = ds[cts.CARIBIC_NO_VARNAME] + ds[cts.CARIBIC_NO2_VARNAME]
    # smooth NOx timeseries (rolling mean with window size = min plume length)
    NOx_smoothed_varname = iagos_utils.get_NOx_varname(flight_program=ds.attrs['program'], smoothed=True,
                                                       filtered=False)
    ds[NOx_smoothed_varname] = ds[NOx_varname] \
        .rolling(UTC_time=cts.WINDOW_SIZE[ds.attrs['program']], min_periods=1) \
        .mean()
    # add regions to each data point
    ds = regions_utils.assign_geo_region_to_ds(ds=ds, geo_regions_dict=geo_regions_dict)

    return ds


def apply_LiNOx_plume_filters(ds, cruise_only, smoothed_data, q3_ds_path, print_debug=False):
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
    NOx_varname = iagos_utils.get_NOx_varname(ds.attrs['program'], smoothed=smoothed_data, filtered=False)

    # only keep cruise data
    if cruise_only:
        ds = iagos_utils.keep_cruise(ds=ds, NOx_varname=NOx_varname, print_debug=print_debug)

    # remove stratospheric influence
    ds = ds.where(ds[cts.PV_VARNAME] < 2)

    # get q3_ds for ds month and geo region #TODO <!> mean('year')
    q3_ds = xr.open_dataset(q3_ds_path).mean('year')

    q3_ds_sel = q3_ds.sel(month=ds['UTC_time'].dt.month, geo_region=ds['geo_region'])
    CO_varname = iagos_utils.get_CO_varname(ds.attrs['program'], filtered=False)

    # remove anthropogenic influence
    ds = iagos_utils.remove_CO_excess(ds=ds, CO_q3_da=q3_ds_sel['CO_q3'], NOx_varname=NOx_varname,
                                      CO_varname=CO_varname, print_debug=print_debug)

    # remove background influence (keep NOx > q3)
    ds = iagos_utils.keep_NOx_excess(ds=ds, NOx_varname=NOx_varname, NOx_q3_da=q3_ds_sel['NOx_q3'], print_debug=print_debug)

    ds = ds.assign_attrs(iagos_utils.get_q3_attrs(ds=ds, q3_ds=q3_ds))

    return ds


def get_LiNOX_plumes(start_flight_id=None, end_flight_id=None, flight_type=None, flight_id_list=None,
                     cruise_only=True, print_debug=False):
    # get NOx flights url (L2 files)
    NOx_flights_url = iagos_utils.get_NOx_flights_from_catalogue(iagos_cat_path=cts.IAGOSv3_CAT_PATH,
                                                                 start_flight_id=start_flight_id,
                                                                 end_flight_id=end_flight_id, flight_type=flight_type,
                                                                 flight_id_list=flight_id_list)

    for flight_path in NOx_flights_url:
        if print_debug:
            print('##################################################')
            print(f'flight {flight_path}')
            print('##################################################')
        flight_ds = get_flight_ds(flight_path=flight_path, print_debug=print_debug)
        filtered_flight_ds = apply_LiNOx_plume_filters(ds=flight_ds, cruise_only=cruise_only, smoothed_data=True, q3_ds_path=cts.Q3_DS_PATH, print_debug=print_debug)

        timenow = timestamp_now_formatted("%Y-%m-%d_%H%M", tz='CET')
        filtered_flight_ds.to_netcdf(f'/o3p/patj/SOFT-IO-LI_output/2024-07-02_testsPlumeDetection/{timenow}_filtered-ds_{filtered_flight_ds.attrs["flight_name"]}.nc')


    # STEP 2:
    # --> open flight into xarray dataset et applique tous les fitres
    #       OK--> open IAGOS netcdf file
    #       OK--> only keep values with validity flag == 0 (good)moiraine
    #       OK--> get PV
    #       OK--> smooth NOx values (je le fais tout le temps je pense, au pire option not_smoothed)
    #       OK--> ajoute les régions
    #       OK--> applique les filtres
    #           --> cruise
    #           --> tropo
    #           --> assign geo-regions to each data point #TODO: <!!>
    #           --> filtre CO #TODO: <!!> en fonction de la région et saisoooon (CO_q3_ds pour chaque region et saison)
    #           --> filtre NOx #TODO: <!!> en fonction de la région et saisoooon (NOx_q3_ds pour chaque region et saison)
    #       --> find plumes function
    #           --> recup les résultats et les stocke qqpart mais jsp où ni sous quelle forme


if __name__ == "__main__":
    # TODO: regarder si OK que si id = int ou si ok quand id = str
    get_LiNOX_plumes(flight_id_list=['2018060302172202'], print_debug=True)

# /o3p/iagos/iagosv3/L2/201806/IAGOS_timeseries_2018060302172202_L2_3.1.2.nc4