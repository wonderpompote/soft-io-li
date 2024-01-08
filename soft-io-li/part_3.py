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


<!> améliorations:
- pour l'instant indique cts.GOES_SATELLITE en dur quand doit passer satellite argument dans les fonction
    ---> faudra que ce soit un param pour que plus tard on puisse utiliser avec d'autres satellites
"""
import argparse
from datetime import datetime
import numpy as np
import pathlib

import pandas as pd

from utils import check_file_exists_with_suffix, get_fp_glm_ds, get_fp_da, GLMPathParser
from utils import constants as cts


def generate_sat_dir_list_between_start_end_date(start_date, end_date, satellite, regrid,
                                                 regrid_res=cts.REGRID_RES):
    """
    Generate list of daily directory path containing satellite data between start and end date
    :param start_date: <pandas.Timestamp> or <numpy.datetime64> or <datetime.datetime>
    :param end_date: <pandas.Timestamp> or <numpy.datetime64> or <datetime.datetime>
    :param satellite: <str> satellite name
    :param regrid: <bool> indicates if the directory contains regridded files
    :param regrid_res: <str> regrid resolution
    :return:
    """
    start_date_dirpath = generate_sat_dir_path(date=start_date, satellite=satellite, regrid=regrid,
                                               regrid_res=regrid_res)
    end_date_dirpath = generate_sat_dir_path(date=end_date, satellite=satellite, regrid=regrid, regrid_res=regrid_res)
    dir_list = [start_date_dirpath]
    for i in range(1, (end_date - start_date).days + 1):
        dir_list.append(
            generate_sat_dir_path(date=start_date + pd.Timedelta(i, 'D'), satellite=satellite, regrid=regrid))
    return dir_list


def generate_sat_dir_path(date, satellite, regrid, regrid_res=cts.REGRID_RES):
    """
    Generate the path to the directory containing the satellite data for a specific date (regridded or not)
    <!> The path does not necessarily point to an existing directory, if it does not exist it will need to be created and filled with the correct data files
    :param date: <pandas.Timestamp> or <numpy.datetime64> or <datetime.datetime> or <GLMPathParser>
    :param satellite: <str> satellite name
    :param regrid: <bool> indicates if the directory contains regridded files
    :param regrid_res: <str> regrid resolution (if regrid == True)
    :return: <pathlib.Path> object to satellite data directory for a specific date
    """
    # expecting a pd.Timestamp OR a datetime object OR np.datetime64 object OR GLMPathParser
    if not isinstance(date, pd.Timestamp):
        if isinstance(date, datetime) or isinstance(date, np.datetime64):
            date = pd.Timestamp(date)
        elif isinstance(date, pathlib.PurePath):
            date = GLMPathParser(date, directory=True, regrid=True).get_start_date_pdTimestamp(
                ignore_missing_start_hour=True)
        elif isinstance(date, GLMPathParser):
            date = date.get_start_date_pdTimestamp(ignore_missing_start_hour=True)
        else:
            raise TypeError('Expecting pandas.Timestamp, datetime.datime, xarray.DataArray or GLMPathParser object')
    # now that we have the pandas.Timestamp we can generate the path
    if satellite == 'GOES':
        if regrid:
            return pathlib.Path(
                f'{cts.REGRID_GLM_ROOT_DIR}/{date.year}/{regrid_res}_{cts.GLM_DIRNAME}_{date.year}_{date.dayofyear:03d}')
        else:
            return pathlib.Path(
                f'{cts.PRE_REGRID_GLM_ROOT_DIR}/{date.year}/{cts.GLM_DIRNAME}_{date.year}_{date.dayofyear:03d}')
    else:
        raise ValueError('Only GOES satellite supported for now')


def generate_sat_regrid_filename_pattern(sat_name, regrid, regrid_res=cts.REGRID_RES):
    """
    Generate filename pattern for a specific satellite and regrid resolution (to be used with pathlib glob function)
    :param sat_name: <str>
    :param regrid: <bool>
    :param regrid_res: <str>
    :return: <str> filename pattern for the satellite
    """
    if sat_name == cts.GOES_SATELLITE:
        # OR_GLM-L2-LCFA_Gxx_YYYY_DDD_HH-HH.nc
        filename_pattern = f'{cts.GLM_DIRNAME}_{cts.Gxx_PATTERN}_{cts.YYYY_pattern}_{cts.DDD_pattern}_{cts.HH_pattern}-{cts.HH_pattern}.nc'
    else:
        raise ValueError(f'{sat_name} NOT supported yet. Supported satellite so far: "GOES"')
    if regrid:
        return f'{regrid_res}_{filename_pattern}'
    else:
        return filename_pattern


def get_weighted_flash_count(spec001_mr_da, flash_count_da):
    """

    :param spec001_mr_da:
    :param flash_count_da:
    :return:
    """
    return (spec001_mr_da * flash_count_da).sum(['latitude', 'longitude']) / 3600


if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    group = parser.add_mutually_exclusive_group()  # soit fpout, soit start et end date

    # change type quand je vais juste passer les dossiers PAS les fichiers
    group.add_argument('-f', '--fpout_path', help='Path to flexpart output netcdf file', type=pathlib.Path)
    # optional arguments for fp out
    parser.add_argument('--sum_height', action='store_true', help='sum fp out values over altitude', default=True)
    parser.add_argument('--load_fpout', action='store_true', help='load fp_out dataArray into memory')

    # soit fpout, soit start et end date
    group.add_argument('--start_date',
                       help='Start date (dry-run mode only) <!> format: YYYY-MM-DD (YYYY: year, MM: month, DD: day)')
    parser.add_argument('--end_date',
                        help='End date (dry-run mode only) <!> format: YYYY-MM-DD (YYYY: year, MM: month, DD: day)')

    # glm args --> give the path to the 7 day GLM regrid file (will be deprecated) or nothing and we'll look for the files
    parser.add_argument('-g', '--glm_path', help='Path to 7-day GLM netcdf file', type=pathlib.Path)

    # dry run --> only display args for now
    parser.add_argument('--dry_run', action='store_true',
                        help='dry run (fp_out and glm_out NOT loaded into memory and weighted flash count NOT calculated)')

    args = parser.parse_args()

    if args.dry_run:
        args.load_fpout = False

    print(args)

    # check FP OUT path
    if not args.dry_run and not check_file_exists_with_suffix(args.fpout_path):
        raise ValueError(f'Incorrect fpout_path, {args.fpout_path} does not exist')

    # 7-day GLM file
    elif not args.dry_run and args.glm_path is not None:
        # recup fp_glm_ds (merge de fp_out et glm sur 7 jours)
        if not check_file_exists_with_suffix(args.glm_path):
            raise ValueError(f'Incorrect glm_path attribute, expecting existing netcdf file')
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

    # NO 7-day GLM file (+ dry_run OK)
    else:
        print('no glm file')
        # IL VA ME FALLOIR LES CONSTANTES
        # <!> we assume that if the directory exists it means that it contains all the GLM files available for that day
        # STEP 1: get start and end date (pd.Timestamps)
        if not args.dry_run:
            fp_da = get_fp_da(args.fpout_path)
            start_date, end_date = pd.Timestamp(fp_da.time.min().values), pd.Timestamp(fp_da.time.max().values)
        else:
            start_date, end_date = pd.Timestamp(args.start_date), pd.Timestamp(args.end_date)
        # STEP 2: génère liste des dossiers qu'on doit avoir (fonction à mettre qqpart ou pas ?)
        regrid_daily_dir_list = generate_sat_dir_list_between_start_end_date(
            start_date=start_date, end_date=end_date, satellite=cts.GOES_SATELLITE, regrid=True
        )
        # STEP 3: regarde si tous les dossiers de la liste existent et récup missing dir list (RAW dir name !!!)
        missing_raw_daily_dir_list = []
        for d in regrid_daily_dir_list:
            if not d.exists():
                missing_date = GLMPathParser(d, directory=True, regrid=True).get_start_date_pdTimestamp(
                    ignore_missing_start_hour=True)
                missing_raw_daily_dir_list.append(
                    generate_sat_dir_path(date=missing_date, satellite=cts.GOES_SATELLITE, regrid=False)
                )
        """ ######################## PRINTY PRINT ######################## """
        print(f'start_date : {start_date}\nend_date : {end_date}')
        print('regrid_daily_dir_list : \n' + "\n".join([str(p) for p in regrid_daily_dir_list]) + '\n')
        """ ######################## PRINTY PRINT ######################## """
        # STEP 4: if missing dir list PAS vide --> regarde s'ils existent (dans le dossier raw du coup)
        if len(missing_raw_daily_dir_list) > 0:
            dir_to_regrid_list = []
            for r_dir in missing_raw_daily_dir_list:
                if r_dir.exists():
                    missing_raw_daily_dir_list.remove(r_dir)
                    dir_to_regrid_list.append(r_dir)

            """ ######################## PRINTY PRINT ######################## """
            print('missing_raw_daily_dir_list : \n' + "\n".join([str(p) for p in missing_raw_daily_dir_list]) + '\n')
            if missing_raw_daily_dir_list:
                print('dir_to_regrid_list : \n' + "\n".join([str(p) for p in dir_to_regrid_list]))
            """ ######################## PRINTY PRINT ######################## """

            #   a) si certains n'existent PAS --> warning / error qui dit d'aller les chercher sur ICARE
            if len(missing_raw_daily_dir_list) > 0:
                missing_dates = [
                    GLMPathParser(m_dir, regrid=False, directory=True) \
                        .get_start_date_pdTimestamp(ignore_missing_start_hour=True) \
                        .strftime('%Y-%j')
                    for m_dir in missing_raw_daily_dir_list
                ]
                raise FileNotFoundError(
                    f'The GLM files for the following dates are missing: {missing_dates}\nPlease download them from the ICARE server and try again')
            #   b) s'ils existent --> les mets dans to_regrid_list et fait regrid sur la liste
            elif len(dir_to_regrid_list) > 0:
                print('PASSER LA LISTE DES DOSSIERS A REGRID')
                print(f'to regrid : {dir_to_regrid_list}')

        # STEP 5: recup tous les path des fichiers qui sont dans les dossiers de la dir list originelle
        #   <!> entre start et end date !!
        """ <!!> me fait une list of list + comme sorted ça charge tout en mémoire !!!!! """
        regrid_daily_file_list = [
            sorted(dirpath.glob(generate_sat_regrid_filename_pattern(cts.GOES_SATELLITE, regrid=True)))
            for dirpath in regrid_daily_dir_list
        ]
        """ ######################## PRINTY PRINT ######################## """
        print('regrid_daily_file_list : \n' + ("\n".join([str(p) for p in regrid_daily_file_list])) + '\n')
        """ ######################## PRINTY PRINT ######################## """
        # STEP 6: create glm_ds puis fp_glm_ds et récup le nombre d'éclairs pondérés
