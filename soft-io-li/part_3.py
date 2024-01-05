"""
STEP 1:
- verif fp out file (amélioration: faire avec methodes fpsim de pawel)
STEP 2:
- check if we have all GLM files *
STEP 3:
- retrieve all REGRID glm files into one dataset
STEP 4:
- recup flash count pondéré
STEP 5:
- mettre trucs dans la bdd iagos ou jsp trop quoi

* CHECK IF WE HAVE ALL FILES:
- recup liste de dossiers journaliers entre start et end date
    --> si manque des dossiers on stock la liste des dossiers manquants dans une liste (faudra récup la date à partir de ça)
        --> regarde si dossiers manquants (noms SANS 05deg devant) sont dans "raw"
            si NON:
            --> va chercher sur icare les fichiers manquants
            --> concat en fichiers horaires
        --> regrid fichiers horaires
- recup liste des fichiers dont on a besoin:
    - if start_date:
        --> tous les fichiers >= start_time
    - if end_date:
        --> tous les fichiers <= end_time
    - else:
        --> tous les fichiers du dossier
- merge tous les fichiers avec xr.open_mfdataset

"""
import argparse
import pathlib

from utils import check_file_exists_with_suffix, get_fp_glm_ds


def get_weighted_flash_count(spec001_mr_da, flash_count_da):
    return (spec001_mr_da * flash_count_da).sum(['latitude', 'longitude']) / 3600


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    # change type quand je vais juste passer les dossiers PAS les fichiers
    parser.add_argument('-f', '--fpout_path', required=True, help='Path to flexpart output netcdf file',
                        type=pathlib.Path)

    # optional arguments for fp out
    parser.add_argument('--sum_height', action='store_true', help='sum fp out values over altitude', default=True)
    parser.add_argument('--load_fpout', action='store_true', help='load fp_out dataArray into memory')

    # glm args --> give the path to the 7 day GLM regrid file (will be deprecated) or nothing and we'll look for the files
    parser.add_argument('-g', '--glm_path', help='Path to 7-day GLM netcdf file', type=pathlib.Path)

    # dry run --> only display args for now
    parser.add_argument('--dry_run', action='store_true', help='dry run (fp_out NOT loaded into memory and weighted flash count NOT calculated)')

    args = parser.parse_args()

    if args.dry_run:
        args.load_fpout = True
        print(args)

    else:
        print(args)

        if args.glm_path is not None:
            # recup fp_glm_ds (merge de fp_out et glm sur 7 jours)
            if not (check_file_exists_with_suffix(args.fpout_path) and check_file_exists_with_suffix(args.glm_path)):
                raise ValueError(f'Incorrect fpout_path and/or glm_path attributes, expecting existing netcdf files')
            else:
                fp_glm_ds = get_fp_glm_ds(fp_da=args.fpout_path, glm_da=args.glm_path,
                                          sum_height=args.sum_height, load_fp_da=args.load_fpout)
                print(fp_glm_ds)

                ######### !!!!!!!!!! #########
                # BOUGER CA EN DEHORS QUAND j'ai récup les fichiers GLM en 1 dataset
                # recup les nb flash
                fp_glm_ds['weighted_flash_count'] = get_weighted_flash_count(spec001_mr_da=fp_glm_ds['spec001_mr'],
                                                                             flash_count_da=fp_glm_ds['flash_count'])
                print()
                print(fp_glm_ds['weighted_flash_count'])

        # NO 7-day GLM file
        else:
            print('no glm file')