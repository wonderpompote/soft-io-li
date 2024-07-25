"""
python add_data_var_attributes_glm_regrid_files.py --logname test_add_attrs_one_file -d /o3p/patj/glm/regrid_hourly_glm/2018/ --regrid --parent-dir
"""
import argparse
import logging
import pathlib
from sys import argv
import xarray as xr

import common.log
from common.log import logger
from common.utils import timestamp_now_formatted, short_list_repr

from softioli.utils import generate_sat_hourly_file_path, get_list_of_sat_files
from softioli import constants as cts


def add_attrs_lightning_sat_file(sat_ds, sat_name, sat_version, regrid, regrid_res_str, grid_res, res_path):
    # flash_count attrs
    if (not 'long_name' in sat_ds['flash_count'].attrs) or \
            not f'{grid_res}° x {grid_res}° x 1h grid cell' in sat_ds['flash_count'].attrs['long_name']:
        sat_ds['flash_count'].attrs['long_name'] = f'Number of flash occurrences in a {grid_res}° x {grid_res}° x 1h grid cell'
        logger().debug('Adding flash_count long_name attribute')

    # flash area hist attrs
    if not 'long_name' in sat_ds['flash_area_log_hist'].attrs or \
            not f'{grid_res}° x {grid_res}° x 1h grid cell' in sat_ds['flash_area_log_hist'].attrs['long_name']:
        sat_ds['flash_area_log_hist'].attrs.update({
            'long_name': f'Number of flash occurrences in log10(flash_energy) bin in a {grid_res}° x {grid_res}° x 1h grid cell' })
        logger().debug('Adding flash_area_log_hist long_name attribute')
    if not 'comment' in sat_ds['flash_area_log_hist'].attrs:
        sat_ds['flash_area_log_hist'].attrs.update({
            'comment': 'log10(flash_area) bins between 1.5 and 4.5, step between bins = 0.1' })
        logger().debug('Adding flash_area_log_hist comment attribute')
    if not 'long_name' in sat_ds['log_flash_area_bin'].attrs:
        sat_ds['log_flash_area_bin'].attrs.update({
            'long_name': 'log10(flash_area) bins' })
        logger().debug('Adding log_flash_area_bin long_name attribute')
    if not 'comment' in sat_ds['log_flash_area_bin'].attrs:
        sat_ds['log_flash_area_bin'].attrs.update({
            'comment': '1.5 <= bin <= 4.5, bin_step = 0.1' })
        logger().debug('Adding log_flash_area_bin comment attribute')

    # flash energy hist attrs
    if not 'long_name' in sat_ds['flash_energy_log_hist'].attrs or \
            not f'{grid_res}° x {grid_res}° x 1h grid cell' in sat_ds['flash_energy_log_hist'].attrs['long_name']:
        sat_ds['flash_area_log_hist'].attrs.update({
            'long_name': f'Number of flash occurrences in log10(flash_energy) bin in a {grid_res}° x {grid_res}° x 1h grid cell' })
        logger().debug('Adding flash_energy_log_hist long_name attribute')
    if not 'comment' in sat_ds['flash_energy_log_hist'].attrs:
        sat_ds['flash_area_log_hist'].attrs.update({
            'comment': 'log10(flash_energy) bins between -15 and -10, step between bins = 0.1' })
        logger().debug('Adding flash_energy_log_hist comment attribute')
    if not 'long_name' in sat_ds['log_flash_energy_bin'].attrs:
        sat_ds['log_flash_energy_bin'].attrs.update({
            'long_name': 'log10(flash_energy) bins' })
        logger().debug('Adding log_flash_energy_bin long_name attribute')
    if not 'comment' in sat_ds['log_flash_energy_bin'].attrs:
        sat_ds['log_flash_energy_bin'].attrs.update({
            'comment': '-15 <= bin <= -10, bin_step = 0.1' })
        logger().debug('Adding log_flash_energy_bin comment attribute')

    # pre-regrid file attribute
    if (not 'pre_regrid_satellite_file' in sat_ds.attrs) or ('05deg' in sat_ds.attrs['pre_regrid_satellite_file']):
        pre_regrid_filename = generate_sat_hourly_file_path(date=sat_ds.time.values[0],
                                                            satellite=sat_name, sat_version=sat_version,
                                                            regrid=False, regrid_res_str=regrid_res_str,
                                                            dir_path=res_path).name

        sat_ds.attrs['pre_regrid_satellite_file'] = pre_regrid_filename
        logger().debug('Adding pre_regrid_satellite_file attribute')

    return sat_ds


if __name__ == '__main__':
    parser = argparse.ArgumentParser()

    default_logdir = pathlib.Path(cts.DEFAULT_LOGDIR, 'add_attrs_sat_regrid_files')
    parser.add_argument('-l', '--logdir', default=default_logdir, help=f'log directory; default is {default_logdir}',
                        type=pathlib.Path)
    parser.add_argument('--logname',
                        help='Log file prefix, resulting log file will be of the form "YYYY-MM-DD_HHmm_<log_file_prefix>.log" with YYYY: year, MM: month, DD: day, HH: hour, mm: minutes',
                        default='add_regrid_attrs')
    parser.add_argument('--loglevel',
                        help='logging level, default=logging.DEBUG(10) - other values: INFO=10, WARNING=30, ERROR=40, CRITICAL=50',
                        default=logging.DEBUG, type=int)

    # nargs donc -f file1 file2 file3 ... PAS -f file1 -f file2 -f file3 ... (ça c'est quand action='append')
    parser.add_argument('-d', '--dir-list', required=True,
                        help='List of directories containing the satellite data files', nargs='+',
                        type=pathlib.Path)

    parser.add_argument('-s', '--sat-name',
                        help='satellite name, supported satellites so far: "GOES_GLM" (default value)',
                        default=cts.GOES_SATELLITE_GLM)
    parser.add_argument('--sat-version', default='G16', help='satellite version (default = G16)')

    parser.add_argument('--res-path', help='result netcdf file path (mostly used when testing)')
    parser.add_argument('--tests', help='test mode (using default test result path if --res-path arg was forgotten)',
                        action='store_true')
    parser.add_argument('--parent-dir',
                        help='indicates if directory path passed with -f is a parent directory containing the subdirectories we need to go through to find the glm files',
                        action='store_true')

    parser.add_argument('--regrid', help='indicates if files are regridded, default=True', action='store_true',
                        default=True)
    parser.add_argument('--regrid-res-str', help='grid resolution (str), default = "05deg"',
                        default=cts.GRID_RESOLUTION_STR)
    parser.add_argument('--regrid-res', help='grid resolution (float), default = 0.5',
                        default=cts.GRID_RESOLUTION)
    """parser.add_argument('--overwrite', '-o', action='store_true',
                        help='indicates if regrid file should be overwritten if it already exists', default=False)"""

    args = parser.parse_args()

    # logs
    if not args.logdir.exists():
        args.logdir.mkdir(parents=True)
    timenow = timestamp_now_formatted(cts.TIMESTAMP_FORMAT)
    logfile = str(pathlib.Path(default_logdir, f'{timenow}_{args.logname}.log'))
    common.log.start_logging(logfile, logging_level=args.loglevel)

    if args.tests and args.res_path is None:
        args.res_path = pathlib.Path('/o3p/patj/test-glm/add_attrs_sat_regrid_files')

    print(args)

    cmd_line = ' '.join(argv)
    logger().info(f'Running: {cmd_line}')
    logger().debug(f'Arguments passed : {args}')

    # recup list of satellite data files
    file_list = get_list_of_sat_files(sat_dir_path=args.dir_list, parent_dir=args.parent_dir,
                                      sat_name=args.sat_name, regrid=args.regrid,
                                      regrid_res_str=args.regrid_res_str)

    print(f'List of files:\n{short_list_repr(file_list)}')
    logger().info(f'List of files:\n{short_list_repr(file_list)}')

    for sat_file_path in file_list:
        logger().debug(f'sat_file_path: {sat_file_path}')
        sat_ds = xr.open_dataset(sat_file_path)

        # add attributes
        sat_ds = add_attrs_lightning_sat_file(sat_ds, sat_name=args.sat_name, sat_version=args.sat_version,
                                              regrid=args.regrid, regrid_res_str=args.regrid_res_str,
                                              res_path=args.res_path, grid_res=args.regrid_res)
        if args.tests:
            res_dir_path = args.res_path
        else:
            res_dir_path = sat_file_path.parent

        # rename "old" sat_file_path temporarily to avoid weird permission errors
        temp_sat_file_path = sat_file_path.rename(pathlib.Path(f'{sat_file_path.parent}/temp_{sat_file_path.name}'))
        logger().debug(f'temp_sat_file_path: {temp_sat_file_path}')

        # store file with new attributes to replace the old one
        logger().info(f'Rewritting file with attributes: {res_dir_path}/{sat_file_path.name}')
        sat_ds.to_netcdf(path=pathlib.Path(f'{res_dir_path}/{sat_file_path.name}'), mode='w')

        # remove temporary sat files with old attributes
        temp_sat_file_path.unlink()

        logger().debug(f'Removing temporary file: {temp_sat_file_path}')
        logger().info('-------------------------------------------------')
