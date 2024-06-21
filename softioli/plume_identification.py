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

from utils import constants as cts
from utils import iagos_utils


# TODO: gérer window size et tout quand c'est du CARIBIC! 25 datapoints CORE != 25 datapoints CARIBIC
def get_flight_ds(flight_path, print_debug=False):
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
    # TODO: ajouter les régions --> faire une fonction qui va dans quoi?
    # apply filters
    # TODO: mettre en place filtrage CO et NOx par région et par mois

    return ds


def get_LiNOX_plumes(start_flight_id=None, end_flight_id=None, flight_type=None, flight_id_list=None,
                     print_debug=False):
    # get NOx flights url (L2 files)
    NOx_flights_url = iagos_utils.get_NOx_flights_from_catalogue(iagos_cat_path=cts.IAGOSv3_CAT_PATH,
                                                                 start_flight_id=start_flight_id,
                                                                 end_flight_id=end_flight_id, flight_type=flight_type,
                                                                 flight_id_list=flight_id_list)

    for flight_path in NOx_flights_url:
        flight_ds = get_flight_ds(flight_path=flight_path, print_debug=print_debug)
    # STEP 2:
    # --> open flight into xarray dataset et applique tous les fitres
    #       OK--> open IAGOS netcdf file
    #       OK--> only keep values with validity flag == 0 (good)moiraine
    #       OK--> get PV
    #       OK--> smooth NOx values (je le fais tout le temps je pense, au pire option not_smoothed)
    #       --> ajoute les régions
    #       --> applique les filtres
    #           --> cruise
    #           --> tropo
    #           --> assign geo-regions to each data point #TODO: <!!>
    #           --> filtre CO #TODO: <!!> en fonction de la région et saisoooon (CO_q3_ds pour chaque region et saison)
    #           --> filtre NOx #TODO: <!!> en fonction de la région et saisoooon (NOx_q3_ds pour chaque region et saison)
    #       --> find plumes function
