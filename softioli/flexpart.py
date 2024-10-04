"""
for a given list of flights:
- create flexpart directory with all files (command, release, outgrid etc.)
- launch simulation
==> DONC il faut que je puisse récupérer le dossier résultat

<!> + fonction run_simulation de pawel faut lui passer juste un path d'un seul dossier donc faut l'appeler dans un for
        ==> garder qqpart la liste de tous les dossiers flexpart créés
"""
import argparse
import pandas as pd
import pathlib

import fpsim
from utils import constants as cts
from utils.utils_functions import generate_flight_output_dir
from utils import fp_utils


# list of all flexpart directories created by fpsim.install_simulation
fp_output_dir_list = []

def install_softioli_fp_simulation(flight_name, output_dirpath, flight_dirname_suffix='', timestep=cts.FP_LOUTSTEP, duration=cts.FP_DURATION, grid_resolution=cts.GRID_RESOLUTION, print_debug=False, overwrite=False):
    # get flight_output directory
    flight_output_dir = generate_flight_output_dir(output_dirpath=output_dirpath, flight_name=flight_name,
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

    # read csv plume file
    plume_df = pd.read_csv(plume_csv_filename, index_col=0)
    releases = None

    # outgrid
    outgrid = fpsim.create_outgrid(
        outheights=[], #TODO: get outheights
        dlon=grid_resolution
    )
    if print_debug:
        print('---')
        print(f'Outgrid: {outgrid}')

    """fpsim.install_simulation(
        target_dir=pathlib.Path(f'{flight_output_dir}/flexpart'),
        command=command,
        releases_df=releases,
        outgrid=outgrid,
        meteo_fields_dir=None, #TODO: <!> je sais pas quoi mettre ?????
        overwrite=overwrite
    )"""

if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    # output directory
    dir_group = parser.add_argument_group('Output directory')
    dir_group.add_argument('-o', '--output-dir', required=True, type=pathlib.Path, help='Path to output directory (directory containing all flight output directories)')
    dir_group.add_argument('--flight-dirname-suffix', default='',
                              help='suffix to add to flight output directory name')
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
    fp_group.add_argument('-t', '--timestep', default='1h', help='Timestep for the flexpart simulation (loutstep), default="1h"')
    # fp simu duration
    fp_group.add_argument('-sd', '--simu-duration', default=10, type=int, help='Flexpart simulation duration in days, default=10')
    # fp out grid resolution
    fp_group.add_argument('-gr', '--grid-res', default=cts.GRID_RESOLUTION, type=float, help=f'Flexpart output grid resolution (default={cts.GRID_RESOLUTION}')
    # run simu
    fp_group.add_argument('--run-simu', action='store_true', help='Indicates if flexpart simulation should be run')

    args = parser.parse_args()
    print(args)

    if args.flight_range:
        flight_range_list = []
        # grep pour trouver tous les noms de vols présents dans le dossier résultat (start_id >= nom_dossier >= end_id)

        args.flight_id_list = list(set(args.flight_id_list + flight_range_list))

    for flight_id in sorted(args.flight_id_list):
        if args.print_debug:
            print('##################################################')
            print(f'flight {flight_id}')
            print('##################################################')
        # install fp simulation
        install_softioli_fp_simulation(flight_name=flight_id, output_dirpath=args.output_dir,
                                       flight_dirname_suffix=args.flight_dirname_suffix,
                                       duration=args.simu_duration, grid_resolution=args.grid_res,
                                       print_debug=args.print_debug, overwrite=args.overwrite)
        # run simulation ??
        pass