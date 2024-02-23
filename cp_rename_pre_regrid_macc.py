"""
Used for testing, main goal = testing sat_regrid and fp_sat_comparison
Main functions:
- copy pre-regrid files from /o3p/macc/glm to /o3p/patj/glm/pre_regrid_hourly_glm/
- rename the copied files to comply with the default notation
"""

import argparse
from logging import DEBUG
import pathlib
from re import fullmatch
from sys import argv

from common.log import logger, start_logging
from common.utils import timestamp_now_formatted, short_list_repr

from softioli.utils import GLMPathParser, OLD_GLM_PRE_REGRID_TEMP_NOTATION, OLD_GLM_NOTATION, generate_sat_dir_path, generate_sat_hourly_filename_pattern, generate_sat_hourly_file_path, generate_sat_dirname_pattern
from softioli.utils import constants as cts


if __name__ == '__main__':
    parser = argparse.ArgumentParser()

    default_logdir = pathlib.Path(cts.DEFAULT_LOGDIR, 'cp_rename_pre_regrid_macc')

    parser.add_argument('-l', '--logdir', default=default_logdir, type=pathlib.Path,
                        help=f'log directory; default is {default_logdir}')
    parser.add_argument('--logname', default='cp_rename_pre_regrid_macc_files',
                        help='Log file prefix, resulting log file will be of the form "YYYY-MM-DD_HHmm_<log_file_prefix>.log" with YYYY: year, MM: month, DD: day, HH: hour, mm: minutes')
    parser.add_argument('--loglevel', default=DEBUG, type=int,
                        help='logging level, default=logging.DEBUG(10) - other values: INFO=10, WARNING=30, ERROR=40, CRITICAL=50')

    # nargs donc -f file1 file2 file3 ... PAS -f file1 -f file2 -f file3 ... (ça c'est quand action='append')
    parser.add_argument('-p', '--path-list', required=True, nargs='+', type=pathlib.Path,
                        help='List of file or directory paths containing pre-regrid GLM hourly files')
    parser.add_argument('--parent-dir', action='store_true', help='Indicates if path passed are parent directory paths (containing subdirectories in which the files we want to copy are stored')

    parser.add_argument('-s', '--sat-name', default=cts.GOES_SATELLITE_GLM,
                        help='satellite name, supported satellites so far: "GOES_GLM" (default value)')
    parser.add_argument('--regrid', default=False,
                        help="Indicates if we're dealing with regridded files/dirs (default=False)")

    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument('-n', '--naming-convention', choices=[OLD_GLM_NOTATION, OLD_GLM_PRE_REGRID_TEMP_NOTATION, 'None'],
                       help='Naming convention used for the directories and/or files')
    group.add_argument('--old-glm', action='store_true',
                       help='old GLM name: GLM_array_DDD_HH1-HH2.nc for files or GLM_array(_05deg)_DDD for dirs')
    group.add_argument('--old-temp-glm', action='store_true',
                       help='old temp GLM name: GLM_array_DDD_temp_HH.nc for files and OR_GLM-L2-LCFA_Gxx_sYYYYDDD for dirs')

    parser.add_argument('--res-path', type=pathlib.Path, default='/o3p/patj/glm/',
                        help='Directory in which the files should be copied, default=/o3p/patj/glm/')

    parser.add_argument('--overwrite', action='store_true', default=False)

    args = parser.parse_args()

    # logs
    if not args.logdir.exists():
        args.logdir.mkdir(parents=True)
    timenow = timestamp_now_formatted("%Y-%m-%d_%H%M")
    logfile = str(pathlib.Path(default_logdir, f'{timenow}_{args.logname}.log'))
    start_logging(logfile, logging_level=args.loglevel)

    print(args)

    cmd_line = ' '.join(argv)
    logger().info(f'Running: {cmd_line}')
    logger().debug(f'Arguments passed : {args}')

    if not args.res_path.exists():
        args.res_path.mkdir(parents=True)
        logger().debug(f'Creating result dir: {args.res_path}')

    if args.naming_convention == 'None':
        args.naming_convention = None

    if args.naming_convention is None and (args.old_glm or args.old_temp_glm):
        if args.old_glm:
            args.naming_convention = OLD_GLM_NOTATION
        elif args.old_temp_glm:
            args.naming_convention = OLD_GLM_PRE_REGRID_TEMP_NOTATION

    # if parent dir --> get list of subdirectories containing the files
    if args.parent_dir:
        dirname_pattern = generate_sat_dirname_pattern(sat_name=args.sat_name, regrid=args.regrid, naming_convention=args.naming_convention)
        args.path_list[:] = [
            dir_path
            for parent_path in sorted(args.path_list)
            for dir_path in parent_path.glob(dirname_pattern)
        ]
        logger().debug(f'Subdirectories paths: {short_list_repr(args.path_list)}')

    old_file_pattern = generate_sat_hourly_filename_pattern(sat_name=args.sat_name, regrid=args.regrid,
                                                            naming_convention=OLD_GLM_NOTATION)
    old_temp_file_pattern = generate_sat_hourly_filename_pattern(sat_name=args.sat_name, regrid=args.regrid,
                                                                 naming_convention=OLD_GLM_PRE_REGRID_TEMP_NOTATION)
    default_file_pattern = generate_sat_hourly_filename_pattern(sat_name=args.sat_name, regrid=args.regrid,
                                                                naming_convention=None)

    for dir_path in args.path_list:
        # get dirname date
        dirname_date = GLMPathParser(file_url=dir_path, regrid=args.regrid, directory=True,
                                     naming_convention=args.naming_convention)\
                            .get_start_date_pdTimestamp(ignore_missing_start_hour=True)
        logger().debug(f'dirname ({dir_path.name})\tdate: {dirname_date}')

        # create new directory path
        new_directory_path = generate_sat_dir_path(date=dirname_date, sat_name=args.sat_name, regrid=args.regrid, target_dir=args.res_path)
        logger().debug(f'new_directory_path: {new_directory_path}')

        new_directory_path.mkdir(parents=True, exist_ok=True)

        for file_path in dir_path.glob('*.nc'):
            if fullmatch(old_temp_file_pattern, file_path.name):
                file_naming_convention = OLD_GLM_PRE_REGRID_TEMP_NOTATION
            elif fullmatch(old_file_pattern, file_path.name):
                file_naming_convention = OLD_GLM_NOTATION
            else:
                raise ValueError('Expecting only old or old temp notation')
            logger().debug('---------------------------------------------------')
            logger().debug(f'file_path: {file_path}')

            file_parser = GLMPathParser(file_url=file_path, regrid=args.regrid, hourly=True,
                                      naming_convention=file_naming_convention, directory=False)
            new_file_path = generate_sat_hourly_file_path(date=file_parser.get_start_date_pdTimestamp(),
                                                          satellite=args.sat_name,
                                                          sat_version=file_parser.satellite_version,
                                                          regrid=args.regrid, dir_path=args.res_path)
            logger().debug(f'new_file_path: {new_file_path}')

            if not new_file_path.exists() or args.overwrite:
                new_file_path.write_bytes(file_path.read_bytes())
                logger().info(f'Copying {file_path} into {new_file_path}')


"""
Tests dans un dossier tests:
python cp_rename_pre_regrid_macc.py -p /o3p/macc/glm/ --parent-dir -n OLD_TEMP --res-path /o3p/patj/test-glm/cp_rename_macc_pre_regrid
Lancer sur tous les fichiers dans /o3p/macc/glm
python cp_rename_pre_regrid_macc.py -p /o3p/macc/glm/ --parent-dir -n OLD_TEMP qqq
"""
