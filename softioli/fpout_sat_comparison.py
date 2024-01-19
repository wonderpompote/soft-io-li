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
import pandas as pd
import pathlib
import xarray as xr

import fpout

from . import utils
from .utils import constants as cts
from .utils import GLMPathParser


def generate_sat_dir_list_between_start_end_date(start_date, end_date, satellite, regrid,
                                                 regrid_res_str=cts.GRID_RESOLUTION_STR):
    """
    Generate list (iter) of daily directory path containing satellite data between start and end date
    :param start_date: <pandas.Timestamp> or <numpy.datetime64> or <datetime.datetime>
    :param end_date: <pandas.Timestamp> or <numpy.datetime64> or <datetime.datetime>
    :param satellite: <str> satellite name
    :param regrid: <bool> indicates if the directory contains regridded files
    :param regrid_res_str: <str> regrid resolution
    :return: <generator>
    """
    # make sure the dates are pd.Timestamps
    start_date = utils.date_to_pd_timestamp(start_date)
    end_date = utils.date_to_pd_timestamp(end_date)
    start_date_dirpath = utils.generate_sat_dir_path(date=start_date, satellite=satellite, regrid=regrid,
                                                     regrid_res_str=regrid_res_str)
    dir_list = [start_date_dirpath]
    for i in range(1, (end_date - start_date).days + 1):
        dir_list.append(
            utils.generate_sat_dir_path(date=start_date + pd.Timedelta(i, 'D'), satellite=satellite, regrid=regrid,
                                        regrid_res_str=regrid_res_str))
    return dir_list


def get_weighted_flash_count(spec001_mr_da, flash_count_da, satellite):
    """

    :param spec001_mr_da:
    :param flash_count_da:
    :return:
    """
    return (spec001_mr_da * flash_count_da).sum(['latitude', 'longitude']) / 3600


# TODO: dask !!
def get_fp_out_da(fpout_path, sum_height=True, load=False, chunks='auto', max_chunk_size=1e8,
                  assign_releases_position_coords=False):
    """

    @param fpout_path:
    @param sum_height:
    @param load:
    @param chunks:
    @param max_chunk_size:
    @param assign_releases_position_coords:
    @return:
    """
    if not utils.str_to_path(fpout_path).exists():
        raise ValueError(f'fp_path {fpout_path} does NOT exist')
    fp_ds = fpout.open_fp_dataset(fpout_path, chunks=chunks, max_chunk_size=max_chunk_size,
                                  assign_releases_position_coords=assign_releases_position_coords)
    fp_da = fp_ds.spec001_mr
    fp_da = fp_da.squeeze()
    if 'pointspec' in fp_da.dims:
        fp_da = fp_da.assign_coords(pointspec=fp_da.pointspec)
    if sum_height:
        fp_da = fp_da.sum('height')
    if load:
        fp_da.load()
    return fp_da

# TODO: découper en plusieurs fonctions ? genre get_sat_data
def get_satellite_ds(start_date, end_date, sat_name):
    start_date, end_date = utils.date_to_pd_timestamp(start_date), utils.date_to_pd_timestamp(end_date)
    # list of daily directories containing the hourly satellite data files between start and end date
    regrid_daily_dir_list = generate_sat_dir_list_between_start_end_date(start_date=start_date, end_date=end_date,
                                                                         satellite=sat_name, regrid=True)
    if sat_name == cts.GOES_SATELLITE:
        SatPathParser = GLMPathParser
    else:
        raise ValueError(f'{sat_name} {cts.SAT_VALUE_ERROR}')
    # get list of missing regrid sat dir
    missing_raw_daily_dir_list = []
    for d in regrid_daily_dir_list:
        if not d.exists():
            # get missing date from dir_path and generate and add path to the pre-regrid daily satellite directory
            missing_raw_daily_dir_list.append(
                utils.generate_sat_dir_path(
                    date=SatPathParser(d, directory=True, regrid=True) \
                                .get_start_date_pdTimestamp(ignore_missing_start_hour=True),
                    satellite=sat_name, regrid=False)
            )
    # check if missing_raw_daily_dir_list is empty, if not --> check if pre-regrid directories exist
    if len(missing_raw_daily_dir_list) > 0:
        dir_to_regrid_list = []
        for r_dir in missing_raw_daily_dir_list:
            if r_dir.exists():
                # remove existing pre_regrid from missing_raw_daily_dir_list
                missing_raw_daily_dir_list.remove(r_dir)
                # and add it to the list of directories to regrid
                dir_to_regrid_list.append(r_dir)
        # regrid the files in the missing directories
        if len(dir_to_regrid_list) > 0:
            # TODO: regrid files from a directory list
            print(f"DIRS TO REGRID: {dir_to_regrid_list}")
    # if we still have some missing pre-regrid directories --> FileNotFoundError
    if len(missing_raw_daily_dir_list) > 0:
        # get the missing dates from the remaining missing directory paths to display them in the error message
        missing_dates = utils.get_list_of_dates_from_list_of_sat_path(path_list=missing_raw_daily_dir_list,
                                                                      directory=True, satellite=sat_name,
                                                                      regrid=False, date_str=True)
        raise FileNotFoundError(f'The GLM files for the following dates are missing: {missing_dates}\nPlease download '
                                f'them from the ICARE server and try again')

    # TODO: recup fp_sat_ds
    # TODO: STEP1: recup liste des fichiers de données satellite entre start et end date
    """
    regrid_daily_file_list = []
        for d_path in regrid_daily_dir_list:
            regrid_daily_file_list.extend(
                sorted(d_path.glob(generate_sat_hourly_filename_pattern(cts.GOES_SATELLITE, regrid=True))))
    """
    # TODO: STEP2: recup sat_ds entre start et end date puis fp_sat_ds puis weighted flash count
    """
    if not args.dry_run:
        glm_ds_between_start_end_date = xr.open_mfdataset(regrid_daily_file_list)  # <?> utiliser dask ???
        fp_glm_ds = get_fp_glm_ds(args.fpout_path, glm_ds_between_start_end_date, load_fp_da=args.load_fpout)
        fp_glm_ds['weighted_flash_count'] = get_weighted_flash_count(spec001_mr_da=fp_glm_ds['spec001_mr'],
                                                                     flash_count_da=fp_glm_ds['flash_count'])
    """


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
    if not args.dry_run and not utils.check_file_exists_with_suffix(args.fpout_path):
        raise ValueError(f'Incorrect fpout_path, {args.fpout_path} does not exist')

    # 7-day GLM file
    elif not args.dry_run and args.glm_path is not None:
        # recup fp_glm_ds (merge de fp_out et glm sur 7 jours)
        if not utils.check_file_exists_with_suffix(args.glm_path):
            raise ValueError(f'Incorrect glm_path attribute, expecting existing netcdf file')
        else:
            fp_glm_ds = utils.get_fp_glm_ds(fp_da=args.fpout_path, glm_da=args.glm_path,
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
            fp_da = utils.get_fp_da(args.fpout_path)
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
                    utils.generate_sat_dir_path(date=missing_date, satellite=cts.GOES_SATELLITE, regrid=False)
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
        regrid_daily_file_list = []
        for d_path in regrid_daily_dir_list:
            regrid_daily_file_list.extend(
                sorted(d_path.glob(generate_sat_hourly_filename_pattern(cts.GOES_SATELLITE, regrid=True))))
        """ ######################## PRINTY PRINT ######################## """
        print('regrid_daily_file_list : \n' + ("\n".join([str(p) for p in regrid_daily_file_list[:10]])) + '\n')
        """ ######################## PRINTY PRINT ######################## """
        # STEP 6: create glm_ds puis fp_glm_ds et récup le nombre d'éclairs pondérés
        if not args.dry_run:
            glm_ds_between_start_end_date = xr.open_mfdataset(regrid_daily_file_list)  # <?> utiliser dask ???
            fp_glm_ds = get_fp_glm_ds(args.fpout_path, glm_ds_between_start_end_date, load_fp_da=args.load_fpout)
            fp_glm_ds['weighted_flash_count'] = get_weighted_flash_count(spec001_mr_da=fp_glm_ds['spec001_mr'],
                                                                         flash_count_da=fp_glm_ds['flash_count'])
