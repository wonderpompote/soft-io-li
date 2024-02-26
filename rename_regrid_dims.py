import argparse
import logging
import pathlib
from sys import argv
import xarray as xr

import common.log
from common.log import logger
from common.utils import timestamp_now_formatted, short_list_repr

from softioli import constants as cts
from softioli.utils import get_list_of_sat_files


def rename_regrid_ds_vars_dims(regrid_ds, file_path):
    if 'log_flash_area_bin' in regrid_ds.dims:
        regrid_ds = regrid_ds.rename_dims({'log_flash_area_bin': 'flash_area_log_bin'})
    if 'log_flash_energy_bin' in regrid_ds.dims:
        regrid_ds = regrid_ds.rename_dims({'log_flash_energy_bin': 'flash_energy_log_bin'})
    # rename "old" sat_file_path temporarily to avoid weird permission errors
    temp_file_path = file_path.rename(pathlib.Path(f'{file_path.parent}/temp_{file_path.name}'))
    logger().debug(f'temp_file_path: {temp_file_path}')
    # store file with new attributes to replace the old one
    logger().info(f'Rewritting file with new dim names: {file_path}')
    regrid_ds.to_netcdf(path=pathlib.Path(f'{file_path}'), mode='w')
    # remove temporary sat files with old attributes
    temp_file_path.unlink()
    logger().debug(f'Removing temporary file: {temp_file_path}')
    logger().info('-------------------------------------------------')


if __name__ == '__main__':
    parser = argparse.ArgumentParser()

    default_logdir = pathlib.Path(cts.DEFAULT_LOGDIR, 'rename_regrid_variables')
    parser.add_argument('-l', '--logdir', default=default_logdir, help=f'log directory; default is {default_logdir}',
                        type=pathlib.Path)
    parser.add_argument('--logname',
                        help='Log file prefix, resulting log file will be of the form "YYYY-MM-DD_HHmm_<log_file_prefix>.log" with YYYY: year, MM: month, DD: day, HH: hour, mm: minutes',
                        default='rename_regrid_vars')
    parser.add_argument('--loglevel',
                        help='logging level, default=logging.DEBUG(10) - other values: INFO=10, WARNING=30, ERROR=40, CRITICAL=50',
                        default=logging.DEBUG, type=int)

    parser.add_argument('-d', '--dir-list', required=True,
                        help='List of directories containing the satellite data files', nargs='+',
                        type=pathlib.Path)
    parser.add_argument('-s', '--sat-name',
                        help=f'satellite name, supported satellites so far: "{cts.GOES_SATELLITE_GLM}" (default value)',
                        default=cts.GOES_SATELLITE_GLM)
    parser.add_argument('--parent-dir',
                        help='indicates if directory path passed with -d is a parent directory containing the subdirectories we need to go through to find the glm files',
                        action='store_true')

    parser.add_argument('--regrid', help='indicates if files are regridded, default=True', action='store_true',
                        default=True)
    parser.add_argument('--regrid-res-str', help=f'grid resolution (str), default = "{cts.GRID_RESOLUTION_STR}"',
                        default=cts.GRID_RESOLUTION_STR)
    parser.add_argument('--regrid-res', help=f'grid resolution (float), default = {cts.GRID_RESOLUTION}',
                        default=cts.GRID_RESOLUTION)

    args = parser.parse_args()

    # logs
    if not args.logdir.exists():
        args.logdir.mkdir(parents=True)
    timenow = timestamp_now_formatted("%Y-%m-%d_%H%M")
    logfile = str(pathlib.Path(default_logdir, f'{timenow}_{args.logname}.log'))
    common.log.start_logging(logfile, logging_level=args.loglevel)

    print(args)
    cmd_line = ' '.join(argv)
    logger().info(f'Running: {cmd_line}')
    logger().debug(f'Arguments passed : {args}')

    file_list = get_list_of_sat_files(sat_dir_path=args.dir_list, parent_dir=args.parent_dir,
                                      sat_name=args.sat_name, regrid=args.regrid,
                                      regrid_res_str=args.regrid_res_str)

    print(f'List of files:\n{short_list_repr(file_list)}')
    logger().info(f'List of files:\n{short_list_repr(file_list)}')

    for file_path in file_list:
        logger().debug(f'sat_file_path: {file_path}')
        with xr.open_dataset(file_path) as regrid_ds:
            if 'flash_area_log_bin' not in regrid_ds.dims or 'flash_energy_log_bin' not in regrid_ds.dims:
                rename_regrid_ds_vars_dims(regrid_ds, file_path)
