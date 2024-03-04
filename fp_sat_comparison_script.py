import argparse
import logging
import pathlib
import pandas as pd
import sys

import common.log
from common.log import logger
from common.utils import timestamp_now_formatted

from softioli import constants as cts
from softioli import fpout_sat_comparison as fs_comp
from softioli import utils

if __name__ == '__main__':
    parser = argparse.ArgumentParser()

    default_logdir = pathlib.Path(cts.DEFAULT_LOGDIR, 'fp_sat_comparison_script')
    parser.add_argument('-l', '--logdir', default=default_logdir, help=f'log directory; default is {default_logdir}',
                        type=pathlib.Path)
    parser.add_argument('--logname', default='add_regrid_attrs',
                        help='Log file prefix, resulting log file will be of the form "YYYY-MM-DD_HHmm_<log_file_prefix>.log" with YYYY: year, MM: month, DD: day, HH: hour, mm: minutes')
    parser.add_argument('--loglevel', default=logging.DEBUG, type=int,
                        help='logging level, default=logging.DEBUG(10) - other values: INFO=10, WARNING=30, ERROR=40, CRITICAL=50')

    group = parser.add_mutually_exclusive_group()  # soit fpout, soit start et end date

    group.add_argument('-f', '--fpout-path', help='Path to flexpart output netcdf file', type=pathlib.Path)
    # optional arguments for fp out
    parser.add_argument('--sum-height', action='store_true', help='sum fp out values over altitude (default=True)', default=True)
    parser.add_argument('--load-fpout', action='store_true', help='load fp_out dataArray into memory (default=False)', default=False)
    # soit fpout, soit start et end date (for testing purposes !)
    group.add_argument('--start-date', type=pd.Timestamp,
                       help='Start date (test mode only) <!> format: YYYY-MM-DD (YYYY: year, MM: month, DD: day)')
    parser.add_argument('--end-date', type=pd.Timestamp,
                        help='End date (test mode only) <!> format: YYYY-MM-DD (YYYY: year, MM: month, DD: day)')
    parser.add_argument('-s', '--sat-name', help='Name of the satellite (only \'GOES\' supported for now',
                        default=cts.GOES_SATELLITE_GLM)
    # dry run --> fp_out and glm_out NOT loaded into memory and weighted flash count NOT calculated
    parser.add_argument('--dry-run', action='store_true',
                        help='dry run (fp_out and glm_out NOT loaded into memory and weighted flash count NOT calculated)')
    parser.add_argument('-t', '--test', action='store_true',
                        help='Test mode (usually to test search for files to regrid between 2 dates)')

    args = parser.parse_args()

    if args.dry_run:
        args.load_fpout = False
    """elif not args.dry_run and not args.test and not args.load_fpout:
        args.load_fpout = True # au cas ou j'oublie de mettre load=True"""

    if not args.logdir.exists():
        args.logdir.mkdir(parents=True)

    print(args)

    timenow = timestamp_now_formatted("%Y-%m-%d_%H%M", tz='CET')
    logfile = str(pathlib.Path(default_logdir, f'{timenow}_{args.logname}.log'))
    # TODO: convert logging level str to logging level value (genre DEBUG == logging.DEBUG == 10)
    common.log.start_logging(logfile, logging_level=args.loglevel)

    cmd_line = ' '.join(sys.argv)
    logger().info(f'Running: {cmd_line}')

    logger().debug(f'Arguments passed : {args}')

    # check FP OUT path if NOT dry run
    if not args.test:
        if not utils.str_to_path(args.fpout_path).exists():
            raise ValueError(f'Incorrect fpout_path, {args.fpout_path} does not exist')
        else:
            fp_da = fs_comp.get_fp_out_da(args.fpout_path, sum_height=args.sum_height, load=args.load_fpout)
            start_date, end_date = pd.Timestamp(fp_da.time.min().values), pd.Timestamp(fp_da.time.max().values)

    else:
        # check qu'on a bien start ET end date
        if args.start_date is None or args.end_date is None:
            raise ValueError(f'Missing start_date (value: {args.start_date} or end_date {args.end_date}')

        start_date, end_date = utils.date_to_pd_timestamp(args.start_date), utils.date_to_pd_timestamp(args.end_date)

        logger().info(f'start_date: {start_date} ({start_date.year}-{start_date.dayofyear}) \tend_date: {end_date} ({start_date.year}-{start_date.dayofyear})')

    """ ######################## PRINTY PRINT ######################## """
    print(f'start_date : {start_date} ({start_date.year}-{start_date.dayofyear}) \tend_date: {end_date} ({end_date.year}-{end_date.dayofyear})')
    """ ######################## PRINTY PRINT ######################## """

    logger().info('<!> only running get_satellite_ds function NOT get_weighted_ds <!>')
    sat_ds = fs_comp.get_satellite_ds(start_date=start_date, end_date=end_date, sat_name=args.sat_name, dry_run=args.dry_run)

    print(sat_ds)
    logger().debug(f'sat_ds:\n{sat_ds}')

    sat_ds.to_netcdf(f'/o3p/patj/test-glm/fpout_sat_comp_tests/{timenow}_sat_ds_WRONG-START-END-HOUR.nc')
    logger().debug(f'writing sat_ds to /o3p/patj/test-glm/fpout_sat_comp_tests/{timenow}_sat_ds_WRONG-START-END-HOUR.nc')
    print('----------------------')
    logger().debug('----------------------')
    
    fp_sat_ds = fs_comp.get_weighted_fp_sat_ds(fp_ds=fp_da, sat_ds=sat_ds)
    fp_sat_ds.to_netcdf(f'/o3p/patj/test-glm/fpout_sat_comp_tests/{timenow}_fp_sat_ds_WRONG-START-END-HOUR.nc')
    logger().debug(f'writing fp_sat_ds to /o3p/patj/test-glm/fpout_sat_comp_tests/{timenow}_fp_sat_ds_WRONG-START-END-HOUR.nc')

