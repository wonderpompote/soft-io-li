"""
Main objective:
- look at each flight in the catalogue, detect if NOx anomalies and store information about the anomaly (date, location, etc.)

Main functions in old version:
- set_paths: set paths to catalogue, PV, launch create result directory (in which plume info, plots, flight_time_txt (?) will be stored) & recup paths to result dict
    --> not needed for paths --> constants
- create_result_dir: create result_dir w/ subdirectories plume_info_all_cuts, plots, flight_time_txt
    --> not necessary if we don't plot je pense
    le mettre dans la grosse fonction qui lancera tout (ça prend une ligne avec pathlib)
- find_NOx_flights: va chercher les vols de NOx dans le catalogue IAGOS
    --> OUI: rename

A faire annexe:
-réfléchir à l'architecture des fichiers pour récup les résultats FP + garder les infos plume qqpart
- Où et comment stocker les infos plume (JSON ou autre)
"""

