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
import pandas as pd

from utils import constants as cts
import utils

def get_NOx_flights_from_catalogue(iagos_cat_path=cts.IAGOSv3_CAT_PATH, start_flight_id=None, end_flight_id=None, flight_type=None, flight_id_list=None):
    if not utils.str_to_path(iagos_cat_path).exists():
        raise ValueError(f"Invalid IAGOS catalogue path, {iagos_cat_path} does NOT exist!")
    df_cat = pd.read_parquet(iagos_cat_path)
    # get all NO2 data variable names
    NO2_vars = [varname for varname in df_cat if varname.startswith("data_vars_") and ("NO2") in varname]
    # get all NOx flights (flights with NO2 variables > 0)
    NOx_flights = df_cat.loc[(df_cat[NO2_vars] > 0).any(axis='columns')]
    # if we only want flights between two flight ids or specific flights in a given list
    if flight_id_list is not None:
        NOx_flights = NOx_flights.loc[flight_id_list]
    elif start_flight_id is not None or end_flight_id is not None:
        NOx_flights = NOx_flights.loc[start_flight_id, end_flight_id]
        # TODO: recup flights from several programs instead of just one (passe une liste)
    # if we only want flights from one specific program
    if flight_type is not None:
        if flight_type in ['CARIBIC', 'CORE', 'MOZAIC']:
            flight_type = f'IAGOS-{flight_type}'
        elif flight_type not in ['IAGOS-CARIBIC', 'IAGOS-CORE', 'IAGOS-MOZAIC']:
            raise ValueError(
                f"Invalid IAGOS program type ({flight_type}), expecting any of the following: ['IAGOS-CARIBIC', 'CARIBIC', 'IAGOS-CORE', 'CORE', 'IAGOS-MOZAIC', 'MOZAIC']")
        NOx_flights = NOx_flights.loc[NOx_flights.attrs_program == flight_type]