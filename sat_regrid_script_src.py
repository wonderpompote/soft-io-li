"""

"""

import argparse
import logging
import pathlib
from sys import argv

import common.log
from common.log import logger
from common.utils import timestamp_now_formatted, short_list_repr

""" <!> depuis src/ faire $ python -m scripts.<nom_script> sinon ça marche pas à cause des imports jsp quoi"""
from softioli import regrid_sat_files
from softioli import constants as cts
from softioli.utils import generate_sat_dirname_pattern, generate_sat_hourly_filename_pattern, OLD_GLM_NOTATION, OLD_GLM_PRE_REGRID_TEMP_NOTATION

if __name__ == '__main__':
    parser = argparse.ArgumentParser()

    default_logdir = pathlib.Path(cts.DEFAULT_LOGDIR, 'sat_regrid_script')
    parser.add_argument('-l', '--logdir', default=default_logdir, help=f'log directory; default is {default_logdir}',
                        type=pathlib.Path)
    parser.add_argument('--logname',
                        help='Log file prefix, resulting log file will be of the form "YYYY-MM-DD_HHmm_<log_file_prefix>.log" with YYYY: year, MM: month, DD: day, HH: hour, mm: minutes',
                        default='sat_regrid')
    parser.add_argument('--loglevel', help='logging level, default=logging.DEBUG(10) - other values: INFO=10, WARNING=30, ERROR=40, CRITICAL=50', default=logging.DEBUG, type=int)

    # nargs donc -f file1 file2 file3 ... PAS -f file1 -f file2 -f file3 ... (ça c'est quand action='append')
    parser.add_argument('-f', '--file-list', required=True, help='List of GLM files to regrid (or directories)', nargs='+', type=pathlib.Path)
    parser.add_argument('-s', '--sat-name', help='satellite name, supported satellites so far: "GOES_GLM" (default value)', default='GOES_GLM')
    parser.add_argument('--parent-dir', help='indicates if directory path passed with -f is a parent directory containing the subdirectories we need to go through to find the glm files', action='store_true')

    parser.add_argument('--regrid-res-str', help='grid resolution (str), default = "05deg"',
                        default=cts.GRID_RESOLUTION_STR)
    parser.add_argument('--regrid-res', help='grid resolution (float), default = 0.5',
                        default=cts.GRID_RESOLUTION)

    parser.add_argument('--res-path', help='result netcdf file path (mostly used when testing)')
    parser.add_argument('--tests', help='test mode (using default test result path if --res-path arg was forgotten', action='store_true')

    parser.add_argument('--old-glm-filename', help='old GLM name (GLM_array_DDD_HH1-HH2.nc for file or GLM_array(_05deg)_DDD for dir)', action='store_true')
    parser.add_argument('--old-temp-glm-filename', help='old temp GLM name (GLM_array_DDD_temp_HH.nc for files and OR_GLM-L2-LCFA_Gxx_sYYYYDDD for dirs)', action='store_true')
    parser.add_argument('--overwrite', '-o', action='store_true', help='indicates if regrid file should be overwritten if it already exists', default=False)

    args = parser.parse_args()

    # logs
    if not args.logdir.exists():
        args.logdir.mkdir(parents=True)
    timenow = timestamp_now_formatted("%Y-%m-%d_%H%M")
    logfile = str(pathlib.Path(default_logdir, f'{timenow}_{args.logname}.log'))
    # TODO: convert logging level str to logging level value (genre DEBUG == logging.DEBUG == 10)
    common.log.start_logging(logfile, logging_level=args.loglevel)

    if args.tests and args.res_path is None:
        args.res_path = pathlib.Path(f'/o3p/patj/test-glm/regrid_tests_{timenow.split("_")[0][-5:]}/') # recup MM-DD de timenow
        if not args.res_path.exists():
            args.res_path.mkdir(parents=True)
            logger().debug(f'Creating result dir: {args.res_path}')

    print(args)

    cmd_line = ' '.join(argv)
    logger().info(f'Running: {cmd_line}')
    logger().debug(f'Arguments passed : {args}')

    """if args.parent_dir:
        # get list of directories containing sat data files
        if args.macc_glm_dirname:
            dirname_pattern = 'OR_GLM-L2-LCFA_G1[6-7]_s[0-2][0-9][0-9][0-9][0-3][0-6][0-9]'
        else:
            dirname_pattern = generate_sat_dirname_pattern(sat_name=args.sat_name, regrid=False)
        dir_list = []
        for parent_dir_path in args.file_list:
            dir_list.extend(parent_dir_path.glob(dirname_pattern))
        logger().debug(f'List of directories: {dir_list}')
        print(f'\nList of directories: {sorted(dir_list)}')
        # get list of data files
        if args.old_glm_filename:
            filename_pattern = 'GLM_array_[0-3][0-6][0-9]_[0-2][0-9]-[0-2][0-9].nc'
        elif args.old_temp_glm_filename:
            filename_pattern = 'GLM_array_[0-3][0-6][0-9]_temp_[0-2][0-9].nc'
        else:
            filename_pattern = generate_sat_hourly_filename_pattern(sat_name=args.sat_name, regrid=False)
        args.file_list = []
        for dir_path in dir_list:
            args.file_list.extend(dir_path.glob(filename_pattern))
        logger().debug(f'List of data files: {short_list_repr(args.file_list)}')
        print(f'\nList of data files: {short_list_repr(sorted(args.file_list))}')"""

    if args.old_glm_filename:
        naming_convention = OLD_GLM_NOTATION
    elif args.old_temp_glm_filename:
        naming_convention = OLD_GLM_PRE_REGRID_TEMP_NOTATION
    else:
        naming_convention = None

    regrid_sat_files(path_list=sorted(args.file_list), sat_name=args.sat_name,
                     grid_res=args.regrid_res, grid_res_str=args.regrid_res_str,
                     dir_list=args.parent_dir,
                     overwrite=args.overwrite, result_dir_path=args.res_path, naming_convention=naming_convention)

"""
Examples: (??)
- test on single pre_regrid GLM file:
python sat_regrid_script_src.py --logname <logname> -f <pre_regrid_file_path> --res-path <res_path> --overwrite
- regrid ALL pre_regrid hourly GLM files in /o3p/macc/glm

- tests parent dir
python sat_regrid_script_src.py --logname new_tests_regrid_dir_parent -f /o3p/macc/glm/ --parent-dir --tests --old-temp-glm-filename
"""