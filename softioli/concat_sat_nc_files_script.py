import argparse
import pandas as pd
import pathlib
from re import match
import xarray as xr

from utils.GLMPathParser import GLMPathParser
from utils.sat_utils import generate_sat_hourly_file_path, generate_sat_filename_pattern
import utils.constants as cts

if __name__ == '__main__':
    parser = argparse.ArgumentParser()

    parser.add_argument('--dir-list', nargs="+", required=True,
                        help='List of daily directory paths that need to be regridded')
    parser.add_argument('--sat-name', choices=[cts.GOES_SATELLITE_GLM], default=cts.GOES_SATELLITE_GLM,
                        help=f'Satellite name, supported values: {cts.GOES_SATELLITE_GLM}')

    parser.add_argument('-d', '--print-debug', action='store_true')

    parser.add_argument('--overwrite', '-o', action='store_true',
                        help='indicates if concat file should be overwritten if it already exists')

    args = parser.parse_args()
    print(args)

    args.dir_list = [pathlib.Path(d_path) for d_path in args.dir_list]

    if args.sat_name == cts.GOES_SATELLITE_GLM:
        # pour chaque dossier
        for dir_p in args.dir_list:
            if args.print_debug:
                print(f'Directory {dir_p}: Concatenating 20 sec netcdf files into hourly files')
            dir_date = GLMPathParser(file_url=dir_p, regrid=False, directory=True).get_start_date_pdTimestamp(
                ignore_missing_start_hour=True)
            for h in range(24):
                if args.print_debug:
                    print('---')
                    print(f'Hour: {h:02d}')
                filename_pattern = generate_sat_filename_pattern(
                    sat_name=args.sat_name, regrid=False, hourly=False,
                    YYYY=dir_date.year, DDD=f'{dir_date.dayofyear:03d}',
                    start_HH=f'{h:02d}'
                )
                if args.print_debug:
                    print(f'Filename pattern: {filename_pattern}')
                # get list of all 15-min files for the corresponding hour
                h_file_list = sorted(pathlib.Path(f'{dir_p}/temp').glob(filename_pattern))
                if args.print_debug:
                    print(f"{len(h_file_list)} files to concatenate")
                if not h_file_list:
                    continue
                #TODO: faire le truc des différentes versions mais là j'ai poooo le temps, je vais faire à la main en cracra pour 2019 et basta
                """ 
                sat_version_set = {GLMPathParser(f, regrid=False, hourly=False).satellite_version for f in h_file_list}
                if len(sat_version_set) > 0:
                    satellite_version = '+'.join(sat_version_set)
                    for sat_version in sat_version_set:
                        # recup un ds par sat version
                        sat_version_filename_pattern = generate_sat_filename_pattern(
                            sat_name=args.sat_name, sat_version_pattern=sat_version, regrid=False, hourly=False,
                            YYYY=dir_date.year, MM=f'{dir_date.month:02d}', DD=f'{dir_date.day:02d}',
                            start_HH=f'{h:02d}'
                        )
                        sat_file_list = [p for p in h_file_list if match(sat_version_filename_pattern, p)]
                else:
                    satellite_version = sat_version_set[0]
                """
                satellite_version = GLMPathParser(h_file_list[0], regrid=False, hourly=False).satellite_version
                result_hourly_filename = generate_sat_hourly_file_path(
                    date=dir_date + pd.Timedelta(h, 'h'),
                    sat_name=args.sat_name, satellite=satellite_version,
                    regrid=False, dir_path=None)
                if not pathlib.Path(result_hourly_filename).exists() or (pathlib.Path(result_hourly_filename).exists() and args.overwrite):
                    concat_ds = xr.open_mfdataset(h_file_list)
                    concat_ds = concat_ds.expand_dims({
                        'time': [dir_date + pd.Timedelta(h, 'h').to_datetime64()],
                        'satellite': satellite_version
                    })
                    concat_ds.to_netcdf(
                        path=result_hourly_filename, mode='w',
                        encoding={"time": {"dtype": 'float64', 'units': 'nanoseconds since 1970-01-01'}}
                    )
                else:
                    print(f'{result_hourly_filename} already exists!')
                break

    else:
        raise ValueError(f'Satellite {args.sat_name} not supported yet')
