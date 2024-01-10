"""
Regrid hourly satellite files to match FLEXPART output grid (usually 0.5° x 0.5°)

Satellite data supported:
- GLM (GOES-E and GOES-W)

Main goal: given a list of pre-regrid hourly nc files
--> regrid each of them and store them in regrid_hourly_<sat_name> directory
"""

# <!!> pour GLM --> faire une fonction qui appelle la bonne fonction en fonction du satellite
def generate_glm_hourly_regrid_file(pre_regrid_file_url, grid_resolution, overwrite):
    # attributes needed: overwrite (bool), grid_resolution (<!> int!)
    # STEP 1: recup pre-regrid file start date (year, day, hour) --> avec GLMPathParser / <sat>PathParser
    # STEP 2: create result nc file path (? pourquoi ici ?)
    # STEP 3: if directory/ies containing result nc file path does NOT exist --> create it/them
    # STEP 4: check if regrid file exists and if it doesn't OR if overwrite == True --> "create it"
    #       STEP 4.1: generate empty dataset with correctly gridded lat et lon
    #       STEP 4.2: open pre-regrid glm file
    #           STEP 4.2.1: assign_coords de target ds au pre-regrid ds
    #           STEP 4.2.2: assign attrs de pre-regrid file à target ds (vérifier lesquels je garde)
    #   QUESTION: est-ce que je garde mon truc de passer les paramètres avec un dictionnaire ?
    #               + la boucle for vu qu'en soit je vais toujours faire pareil non ?
    #                   --> une fois qu'on décide ce qu'on garde come info je vais toujours faire ça donc bon
    #           STEP 4.2.3: for loop to apply the operations on each variable (count or histogram for now)
    #           STEP 4.2.4: après chaque opération OK, merge le ds résultat_operation aec notre result_ds
    # STEP 5: ajoute pre-regrid date + attribute regrid_file_creation_date au result ds
    # STEP 6: result ds to_netcdf (<!> modifier encoding de la date, ça prend de la palce pour rien)
    # STEP 7: return result ds (???)

    return


def regrid_sat_files(file_list, sat_name):
    # <!!> sat_name OU lightning_instrument_name (?)
    # for each file --> apply regrid function (depending on sat_name) (<?> est-ce que je peux avoir une var = function?)
    return