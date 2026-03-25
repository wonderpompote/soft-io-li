import argparse
import pathlib

from utils import constants as cts
from sat_regrid import regrid_sat_files
from common.utils import list_from_file

if __name__ == '__main__':
    parser = argparse.ArgumentParser()

    path_list_group = parser.add_mutually_exclusive_group(required=True)
    path_list_group.add_argument('--dir-list', help='Path to txt file containing list of daily directory paths that need to be regridded (1 path/line)')
    path_list_group.add_argument('--dir-path', help='Path to a single daily directory that needs to be regridded (useful when regridding using a slurm array)')
    path_list_group.add_argument('--file-list', help='Path to txt file containing list of paths to hourly files that need to be regridded (1 path/line)')
    path_list_group.add_argument('--file-path', help='Path to hourly .nc file that needs to be regridded (useful when regridding using a slurm array)')

    parser.add_argument('--sat-name', required=True, choices=[cts.GOES_SATELLITE_ABI, cts.GOES_SATELLITE_GLM, cts.MTG_LI],
                        help=f'Satellite name, supported values: {cts.GOES_SATELLITE_ABI}, {cts.GOES_SATELLITE_GLM} or {cts.MTG_LI}')

    parser.add_argument('-d', '--print-debug', action='store_true')

    # regrid parameters
    parser.add_argument('--regrid-res-str', help=f'grid resolution (str), default = "{cts.GRID_RESOLUTION_STR}"',
                        default=cts.GRID_RESOLUTION_STR)
    parser.add_argument('--regrid-res', help=f'grid resolution (float), default = {cts.GRID_RESOLUTION}',
                        default=cts.GRID_RESOLUTION, type=float)

    parser.add_argument('--result-dir-path',
                        help='For testing purposes, root directory in which regrid files should be stored (if None, path by default will be used)')

    parser.add_argument('--overwrite', '-o', action='store_true',
                        help='indicates if regrid file should be overwritten if it already exists')
    parser.add_argument('--rm-pre-regrid-files', action='store_true', help='Indicates if pre regrid hourly files '
                                                                           'should be deleted once the corresponding '
                                                                           'regrid file has been generated (to save '
                                                                           'some space)')

    args = parser.parse_args()
    print(args)

    # directory path
    if args.dir_list: # txt file with several directory paths
        path_list = [pathlib.Path(d_path) for d_path in list_from_file(args.dir_list, header=0, ignore_blank_lines=True)]
        is_dir_list = True
    elif args.dir_path: # directly path to directory
        path_list = [pathlib.Path(args.dir_path)]
        is_dir_list = True
    # or file path
    elif args.file_list: # txt file with several .nc file paths
        path_list = [pathlib.Path(d_path) for d_path in list_from_file(args.file_list, header=0, ignore_blank_lines=True)]
        is_dir_list = False
    else: # directly path to .nc file
        path_list = [pathlib.Path(args.file_path)]
        is_dir_list=False

    if args.print_debug:
        print(f"launching regrid_sat_files on : {path_list}")

    regrid_sat_files(path_list=path_list, sat_name=args.sat_name, dir_list=is_dir_list,
                     overwrite=args.overwrite, rm_pre_regrid_file=args.rm_pre_regrid_files,
                     grid_res=args.regrid_res, grid_res_str=args.regrid_res_str,
                     result_dir_path=args.result_dir_path, print_debug=args.print_debug)

    print("end of file: regrid_daily_dir_file")

