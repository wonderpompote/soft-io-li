import argparse
import pathlib

from utils import constants as cts
from sat_regrid import regrid_sat_files

"""
recup fp ds
recup start et end date
appelle get_satellite_ds pour ABI et GLM avec start et end dates
    get_satellite_ds recup les liens des dossiers journaliers nécessaires, si regrid_dir existe pas, regarde ce qu'il y a dans pre_regrid dir
    + regrid appelle sat_regrid.py regrid_sat_files en passant la liste des directories to regrid
    + recup tous les fichiers regrid et les ouvre dans un seul dataset

DONC moi ici ce que j'aimerais faire c'est passer au script un dossier journalier et un nom des satellite et ça regrid pour ce jour là
    OBJ: pouvoir le faire tourner avec des job array et je donnne un fichier text avec liste des dossiers à regrid
    
"""

if __name__ == '__main__':
    parser = argparse.ArgumentParser()

    parser.add_argument('--dir-list', nargs="+", required=True,
                        help='List of daily directory paths that need to be regridded')
    parser.add_argument('--sat-name', required=True, choices=[cts.GOES_SATELLITE_ABI, cts.GOES_SATELLITE_GLM],
                        help=f'Satellite name, supported values: {cts.GOES_SATELLITE_ABI} or {cts.GOES_SATELLITE_GLM}')

    parser.add_argument('-d', '--print-debug', action='store_true')

    # regrid parameters
    parser.add_argument('--regrid-res-str', help=f'grid resolution (str), default = "{cts.GRID_RESOLUTION_STR}"',
                        default=cts.GRID_RESOLUTION_STR)
    parser.add_argument('--regrid-res', help=f'grid resolution (float), default = {cts.GRID_RESOLUTION}',
                        default=cts.GRID_RESOLUTION)

    parser.add_argument('--result-dir-path',
                        help='For testing purposes, root directory in which regrid files should be stored (if None, path by default will be used)')

    parser.add_argument('--overwrite', '-o', action='store_true',
                        help='indicates if regrid file should be overwritten if it already exists', default=False)
    parser.add_argument('--rm-pre-regrid-files', action='store_true', help='Indicates if pre regrid hourly files '
                                                                           'should be deleted once the corresponding '
                                                                           'regrid file has been generated (to save '
                                                                           'some space)')

    args = parser.parse_args()
    print(args)

    args.dir_list = [pathlib.Path(d_path) for d_path in args.dir_list]

    if args.print_debug:
        print("launching regrid_sat_files on : {args.dir_list}")

    regrid_sat_files(path_list=args.dir_list, sat_name=args.sat_name, dir_list=True,
                     overwrite=args.overwrite, rm_pre_regrid_file=args.rm_pre_regrid_files,
                     grid_res=args.regrid_res, grid_res_str=args.regrid_res_str,
                     result_dir_path=args.result_dir_path, print_debug=args.print_debug)

    print("end of file: regrid_daily_dir_file")
