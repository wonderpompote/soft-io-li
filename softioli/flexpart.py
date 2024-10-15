import argparse
import numpy as np
import pandas as pd
import pathlib

from common.utils import timestamp_now_formatted
import fpsim

from utils import constants as cts
from utils.utils_functions import generate_flight_output_dir, get_list_of_files_between_two_values
from utils import fp_utils


# list of all flexpart directories created by fpsim.install_simulation
fp_output_dir_list = []

def install_softioli_fp_simulation(flight_name, flights_output_dirpath, flight_dirname_suffix='', timestep=cts.FP_LOUTSTEP, duration=cts.FP_DURATION, grid_resolution=cts.GRID_RESOLUTION, outheight_min=cts.FP_OUTHEIGHT_MIN, outheight_max=cts.FP_OUTHEIGHT_MAX, outheight_step=cts.FP_OUTHEIGHT_STEP, print_debug=False, overwrite=False, fp_output_dirname='flexpart'):
    # get flight_output directory
    flight_output_dir = generate_flight_output_dir(output_dirpath=flights_output_dirpath, flight_name=flight_name,
                                                   missing_ok=False, dirname_suffix=flight_dirname_suffix)
    # get csv file
    plume_csv_filename = list(flight_output_dir.glob(f'{flight_name}_arrivaltime-*.csv'))[0]
    arrival_timestamp = fp_utils.get_arrival_timestamp_from_plume_csv_filename(plume_csv_filename)
    if print_debug:
        print(f'Plume info csv: {plume_csv_filename}')
        print(f'Arrival timestamp: {arrival_timestamp}')

    # command file
    command = fpsim.create_command(
        begin_time=arrival_timestamp - pd.Timedelta(duration, 'd'),
        end_time=fp_utils.get_timestamp_next_hour(arrival_timestamp),
        loutstep=timestep,
        ldirect=-1 #backward
    )
    if print_debug:
        print('---')
        print(f'Command: {command}')

    # outgrid
    outgrid = fpsim.create_outgrid(
        outheights=np.append(np.arange(outheight_min, outheight_max + outheight_step, outheight_step), 50000), # + add last level = 50000m
        dlon=grid_resolution
    )
    if print_debug:
        print('---')
        print(f'Outgrid: {outgrid}')

    # realeases
    plume_df = pd.read_csv(plume_csv_filename, index_col=0)
    min_max_dic = {}
    # get min and max from start and end lon, lat + substract 0.25° from min and add 0.25° from max
    lon_lat_cols = ['lon', 'lat']
    for col in lon_lat_cols:
        min_max_dic[f'{col}_min'] = plume_df[[f'start_{col}', f'end_{col}']].min(axis=1) - 0.25
        min_max_dic[f'{col}_max'] = plume_df[[f'start_{col}', f'end_{col}']].max(axis=1) + 0.25
    # get min and max pressure +/- 5000 Pa
    min_max_dic['press_min'] = plume_df[['start_press', 'end_press']].min(axis=1) - 5000
    min_max_dic['press_max'] = plume_df[['start_press', 'end_press']].min(axis=1) + 5000
    # Concatenate min max df into a single DataFrame
    min_max_plume_df = pd.concat(min_max_dic.values(), axis=1, keys=min_max_dic.keys())
    # convert dates (str) to timestamp
    plume_df['start_UTC_time'] = pd.to_datetime(plume_df['start_UTC_time'])
    plume_df['end_UTC_time'] = pd.to_datetime(plume_df['end_UTC_time'])
    # generate releases dataframe (must take middle point for each variable)
    releases_df = fpsim.releases_as_dataframe(
        lon=(min_max_plume_df['lon_max'] + min_max_plume_df['lon_min']) / 2,
        dlon=min_max_plume_df['lon_max'] - min_max_plume_df['lon_min'],
        lat=(min_max_plume_df['lat_max'] + min_max_plume_df['lat_min']) / 2,
        dlat=min_max_plume_df['lat_max'] - min_max_plume_df['lat_min'],
        alt=(min_max_plume_df['press_max'] + min_max_plume_df['press_min']) / 2,
        dalt=(min_max_plume_df['press_max'] - min_max_plume_df['press_min']),
        time=plume_df['start_UTC_time'] + (plume_df['end_UTC_time'] - plume_df['start_UTC_time']) / 2,
        dtime=plume_df['end_UTC_time'] - plume_df['start_UTC_time'],
        nparts=1e5
    )
    if print_debug:
        print('---')
        print(f'Releases df: {releases_df}')

    fpsim.install_simulation(
        target_dir=pathlib.Path(f'{flight_output_dir}/{fp_output_dirname}'),
        command=command,
        releases_df=releases_df,
        outgrid=outgrid,
        meteo_fields_dir='/o3p/wolp/ECMWF/ERA5/050deg_1h_T319_eta1/', #05deg ERA5 data
        horizontal_resol=grid_resolution,
        overwrite=overwrite
    )

    return pathlib.Path(f'{flight_output_dir}/{fp_output_dirname}')

if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    # output directory
    dir_group = parser.add_argument_group('Output directory')
    dir_group.add_argument('-fo', '--flights-output-dir', required=True, type=pathlib.Path, help='Path to output directory (directory containing all flight output directories)')
    dir_group.add_argument('--flight-dirname-suffix', default='',
                              help='suffix to add to flight output directory name')
    dir_group.add_argument('-o', '--fp-output-dirname', default='flexpart', help='Name of the directory where the flexpart output will be stored (default="flexpart")')
    dir_group.add_argument('--overwrite', action='store_true', help='Indicates if existing flexpart output directory should be overwritten (default=False)')

    parser.add_argument('-d', '--print-debug', action='store_true', help='print debug (default=False)')

    # flight list
    flight_group = parser.add_argument_group('Flights')
    flight_group.add_argument('--flight-list', action='store_true',
                                   help='Indicates if a list of flight ids/names will be passed')
    flight_group.add_argument('--flight-range', action='store_true',
                                   help='Indicates if start and end flight ids/names will be passed')
    # range
    flight_group.add_argument('-s', '--start-id',
                               help='Start flight name/id (in case we only want to retrieve NOx flights between two flight ids)')
    flight_group.add_argument('-e', '--end-id',
                               help='End flight name/id (in case we only want to retrieve NOx flights between two flight ids)')
    # list
    flight_group.add_argument('--flight-id-list', nargs='+', default=[], help='List of flight ids/names (default = None)')

    # flexpart parameters
    fp_group = parser.add_argument_group('Flexpart parameters')
    fp_group.add_argument('-t', '--timestep', default='1h', help='Timestep for the flexpart simulation (loutstep), (default="1h")')
    # fp simu duration
    fp_group.add_argument('-sd', '--simu-duration', default=10, type=int, help='Flexpart simulation duration in days, (default=10)')
    # fp out grid resolution
    fp_group.add_argument('-gr', '--grid-res', default=cts.GRID_RESOLUTION, type=float, help=f'Flexpart output grid resolution (default={cts.GRID_RESOLUTION})')
    # run simu
    fp_group.add_argument('--run-simu', action='store_true', help='Indicates if flexpart simulation should be run')
    # slurm node on which fp simu should be launched
    fp_group.add_argument('--slurm-partition', default='o3pwork', help='Slurm partition on which flexpart should be run (default="o3pwork")')

    args = parser.parse_args()
    print(args)

    if args.flight_range:
        flight_range_list = get_list_of_files_between_two_values(args.flights_output_dir, start_name=args.start_id,
                                                                 end_name=args.end_id,
                                                                 glob_pattern=f'{cts.YYYY_pattern}{cts.MM_pattern}{cts.DD_pattern}*')

        args.flight_id_list = list(set(args.flight_id_list + flight_range_list))

    if args.fp_output_dirname != "flexpart":
        args.fp_output_dirname = f"flexpart_{timestamp_now_formatted(cts.TIMESTAMP_FORMAT, tz='CET')}_{args.fp_output_dirname}"

    for flight_id in sorted(args.flight_id_list):
        if args.print_debug:
            print('##################################################')
            print(f'flight {flight_id}')
            print('##################################################')
        # install fp simulation
        fpsim_dirpath = install_softioli_fp_simulation(flight_name=flight_id, flights_output_dirpath=args.flights_output_dir,
                                       flight_dirname_suffix=args.flight_dirname_suffix, fp_output_dirname=args.fp_output_dirname,
                                       duration=args.simu_duration, grid_resolution=args.grid_res,
                                       print_debug=args.print_debug, overwrite=args.overwrite)
        # run simulation
        if args.run_simu:
            if args.print_debug:
                print('---')
                print(f'Running fp simulation on partition {args.slurm_partition} from directory: {fpsim_dirpath}')
            fpsim.run_simulation(
                sim_dir=fpsim_dirpath,
                slurm_partition=args.slurm_partition,
            )


        #TODO: recup la liste des fichiers .nc créés ? ou au moins liste des dossiers flexpart/output our la partie 3