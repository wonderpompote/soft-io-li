import argparse
import numpy as np
import pandas as pd
import pathlib
from scipy.ndimage import label, find_objects
import xarray as xr

from common.utils import timestamp_now_formatted, list_from_file

from utils import constants as cts
from utils import iagos_utils, regions_utils
from utils.common_coords import GEO_REGIONS
from utils.plume_info_utils import write_plume_info_to_csv_file
from utils.utils_functions import create_root_output_dir, create_flight_output_dir, _none_or_variable
from utils.iagos_utils import MissingVariableError


def get_flight_ds_with_PV_and_valid_data(ds, geo_regions_dict=GEO_REGIONS, print_debug=False):
    """
    Adds PV values to flight dataset, only keeps valid data and assign geographical region id to each value along the flight
    :param ds: <xarray.Dataset> iagos flight dataset
    :param geo_regions_dict: <dict> dictionary with geographical regions with the following structure: { <region_id> : { "REGION_NAME": <str>, "LON_MIN": <float>, "LON_MAX": <float>, "LAT_MIN": <float>, "LAT_MAX": <float> }, ... }
    :param print_debug: <bool> prints debug messages (for testing purposes)
    :return: <xarray.Dataset>
    """
    # return flight ds with PV and only valid data
    flight_program = ds.attrs[cts.PROGRAM_ATTR]
    ds = iagos_utils.get_valid_data(var_list=iagos_utils.get_var_list(flight_program=flight_program), ds=ds,
                                    print_debug=print_debug)
    ds = iagos_utils.get_PV(ds=ds, print_debug=print_debug)

    NOx_varname = iagos_utils.get_NOx_varname(flight_program=flight_program, tropo=False, smoothed=False, filtered=False)
    # if CARIBIC flight --> calculate NOx variable from NO and NO2 measurements
    if flight_program == f"{cts.IAGOS}-{cts.CARIBIC}":
        ds[NOx_varname] = ds[cts.CARIBIC_NO_VARNAME] + ds[cts.CARIBIC_NO2_VARNAME]
    # smooth NOx timeseries (rolling mean with window size = min plume length)
    ds[cts.NOx_SMOOTHED_VARNAME] = ds[NOx_varname] \
                                        .rolling(UTC_time=cts.WINDOW_SIZE[flight_program], min_periods=1) \
                                        .mean()
    # smooth CO timeseries (rolling mean with window size = min plume length)
    CO_varname = iagos_utils.get_CO_varname(flight_program=flight_program, smoothed=False, tropo=False)
    if CO_varname in ds:
        ds[cts.CO_SMOOTHED_VARNAME] = ds[CO_varname] \
                                            .rolling(UTC_time=cts.WINDOW_SIZE[flight_program], min_periods=1) \
                                            .mean()
    else:
        raise MissingVariableError(f'<!> {CO_varname} NOT found in dataset for flight {ds.attrs[cts.FLIGHT_NAME_ATTR]}')
    # add regions to each data point
    ds = regions_utils.assign_geo_region_to_ds(ds=ds, geo_regions_dict=geo_regions_dict)

    return ds


def apply_LiNOx_plume_filters(ds, cruise_only, CO_q3=None, NOx_q3=None, q3_ds_complete=None, print_debug=False):
    """
    Function to apply filters on the NOx timeseries to remove stratospheric, anthropogenic and background influence.
    If cruise_only = True: only keep data where air pressure < 30000 Pa
    - Stratospheric influence: remove NOx data where PV > 2
    - Anthropogenic influence: remove NOx data vhere CO value > CO_q3
    - Background influence: remove NOx data < NOx_q3 (only keep NOx excess)
    :param ds: <xarray.Dataset> flight ds
    :param cruise_only: <bool> if True, only keep data where air pressure < 30000 Pa
    :param q3_ds_complete: <xr.Dataset> q3_ds for all regions and variables
    :param print_debug: <bool> for testing purposes, print debug
    :return: <xarray.Dataset> filtered version of flight ds
    """
    # only keep cruise data
    if cruise_only:
        ds = iagos_utils.keep_cruise(ds=ds, print_debug=print_debug)
    else: # else remove PBL only
        ds = iagos_utils.remove_PBL(ds=ds, print_debug=print_debug)

    # remove stratospheric influence
    ds = iagos_utils.keep_tropo(ds=ds, print_debug=print_debug,
                                var_list=[cts.NOx_SMOOTHED_VARNAME, cts.CO_SMOOTHED_VARNAME,
                                             iagos_utils.get_O3_varname(ds.attrs[cts.PROGRAM_ATTR], tropo=False)])

    if q3_ds_complete is not None:
        # get q3_ds for ds month and geo region
        q3_ds = q3_ds_complete.sel(geo_region=ds['geo_region'], month=ds['UTC_time'].dt.month)
        ds = ds.assign_attrs(iagos_utils.get_q3_attrs(ds=ds, q3_ds=q3_ds_complete))
    else:
        q3_ds = {'NOx_q3': NOx_q3 if NOx_q3 is not None else cts.NOx_Q3,
                 'CO_q3': CO_q3 if CO_q3 is not None else cts.CO_Q3}
        ds = ds.assign_attrs(q3_ds)


    # remove anthropogenic influence
    ds = iagos_utils.remove_CO_excess(ds=ds, CO_q3=q3_ds['CO_q3'], NOx_varname=cts.NOx_SMOOTHED_TROPO_VARNAME,
                                      CO_varname=cts.CO_SMOOTHED_TROPO_VARNAME, print_debug=print_debug)

    # remove background influence (keep NOx > q3)
    ds = iagos_utils.keep_NOx_excess(ds=ds, NOx_varname=cts.NOx_FILTERED_VARNAME, NOx_q3=q3_ds['NOx_q3'], print_debug=print_debug)

    # remove aircraft exhaust spikes
    ds = iagos_utils.remove_aircraft_spikes(ds=ds, print_debug=print_debug)

    return ds


def find_plumes(ds, flight_output_dirpath, min_plume_length=cts.MIN_PLUME_LENGTH, write_plume_info_to_csv=True, filename_suffix='', print_debug=False):
    """
    Merge plumes together if gap between them is smaller than minimum plume length + remove plumes with length smaller than minimum plume length
    @param ds:
    @param flight_output_dirpath: <pathlib.Path> or <str>
    @param min_plume_length: <int>
    @param write_plume_info_to_csv: <bool>
    @return:
    """
    flight_program = ds.attrs[cts.PROGRAM_ATTR]
    NOx_varname = iagos_utils.get_NOx_varname(flight_program=flight_program, tropo=True, smoothed=True, filtered=True)

    # get labeled array
    labeled, ncomponents = label((ds[NOx_varname] > 0).values)
    # get start_id (start of slice) and end_id (stop of slice) from labeled array
    slices = find_objects(labeled) # recup an array of slices for each label (e.g. [ (slice(x,x), ), ... , (slice(x,x), ) ] )
    start_id = [s[0].start for s in slices]
    end_id = [s[0].stop -1 for s in slices] # - 1 because slice.stop is exclusive
    # merge plumes if start_id[i+1] - end_id[i] < 100 seconds
    start_id_merged, end_id_merged = [], []
    if start_id: # if no plumes no need to enter loop
        start_id_merged = [start_id[0]]
        end_id_merged = [end_id[0]]
        # loop through start and end ids (starting at index 1 because 0 is already in start_id_merged/end_id_merged)
        for next_start_id, next_end_id in zip(start_id[1:], end_id[1:]):
            # check gap between start_id[i+1] and end_id[i]
            gap = ds['UTC_time'][next_start_id] - ds['UTC_time'][end_id_merged[-1]]
            # if gap smaller than min plume length, merge plumes (end_id plume i = end_id plume i+1)
            if gap < pd.Timedelta(seconds=min_plume_length):
                end_id_merged[-1] = next_end_id # update end_id_merged and keep start_id_merged unchanged
            else:
                start_id_merged.append(next_start_id)
                end_id_merged.append(next_end_id)
    start_id, end_id = start_id_merged, end_id_merged
    # assign plume_id + remove plumes smaller than 27.5 km (100 seconds) from the list (put their id to -1)
    min_plume_datapoints = cts.WINDOW_SIZE[flight_program] # min datapoints = smoothing window size = 100sec
    array_size = ds.sizes['UTC_time']
    plume_id_array = np.full(array_size, np.nan)
    plume_id = 1
    for start, end in zip(start_id, end_id):
        # if plume too small (plume_length < min_plume_datapoints) --> plume_id = -1
        if end - start < min_plume_datapoints:
            plume_id_array[start:end+1] = -1 # +1 because exclusive
        else:
            plume_id_array[start:end + 1] = plume_id
            plume_id += 1
    # create new data variable to store plume_id
    ds[cts.NOx_PLUME_ID_VARNAME] = xr.DataArray(
        data=plume_id_array, dims=['UTC_time'], coords={'UTC_time': ds.UTC_time},
        attrs={"id_values": '[nan, -1, 1... ]',
         'id_meanings': ['not_a_plume', 'plume_too_small', 'plume_id']}
    )

    if write_plume_info_to_csv and flight_output_dirpath is not None:
        write_plume_info_to_csv_file(ds, output_dirpath=flight_output_dirpath, filename_suffix=filename_suffix, print_debug=print_debug)

    return ds



def get_LiNOX_plumes(start_flight_id=None, end_flight_id=None, flight_type=None, flight_ids_list=None, only_softioli=False, flight_urls_list=None,
                     cruise_only=True, CO_q3=None, NOx_q3=None, use_q3_ds=False, q3_ds_path=cts.Q3_DS_PATH, print_debug=False, save_output=True,
                     filtered_ds_to_netcdf=False, plume_ds_to_netcdf=False, end_of_plume_duration=100,
                     plot_flight=False, show_region_names=False, save_fig=False, show_fig=False, file_suffix='',
                     output_dirname_suffix='', flight_dirname_suffix='',
                     root_output_dirpath=cts.OUTPUT_ROOT_DIR, timenow=None):
    """
    Main function to retrieve potential LiNOx plumes from a list of flights
    :param start_flight_id: <str>
    :param end_flight_id: <str>
    :param flight_type: <str> Expecting 'IAGOS-CARIBIC', 'CARIBIC', 'IAGOS-CORE', 'CORE', 'IAGOS-MOZAIC', 'MOZAIC' or None if all kind of flights are analysed
    :param flight_ids_list: <list> [ <str>, ... , <str> ] List of flight names
    :param cruise_only: <bool> indicates if only values during the cruise stage of the flight should be kept for analysis
    :param CO_q3: CO filter
    :param NOx_q3: NOx q3 value
    :param use_q3_ds: <bool> useful when using different CO q3 values depending on the geographical region #TODO: to be deleted
    :param print_debug: <bool> prints debug messages (for testing purposes)
    :param save_output: <bool> Indicates if the information gathered about potential LiNOx plume should be saved
    :param filtered_ds_to_netcdf: <bool> Indicates if the flight dataset on which the filters have been applied should be saved
    :param plume_ds_to_netcdf: <bool> Indicates if the flight dataset on which the filters have been applied AND the plumes identified should be saved
    :param end_of_plume_duration: <int> number of seconds (containing NaN values) after which a plume is considered to be over
    :param plot_flight: <bool> plot timeseries of the flight after plume detection
    :param show_region_names: <bool> Indicates if geo region names should be displayed on the flight plot
    :param save_fig: <bool> Indicates if flight plot should be saved
    :param show_fig: <bool> Indicates if flight plot should be displayed when it is generated
    :param file_suffix: <str> suffix to add to each output file
    :param output_dirname_suffix: <str> suffix to add to the general output directory name
    :param flight_dirname_suffix: <str> suffix to add to the flight output directory name
    :param root_output_dirpath: <pathlib.Path> or <str> path to the directory in which the outputs will be saved
    :param timenow: <str> date
    :return:
    """
    if flight_urls_list is None:
        # get NOx flights url (L2 files)
        if only_softioli:
            airports_list = cts.SOFTIOLI_AIRPORTS
        else:
            airports_list = None
        NOx_flights_url = iagos_utils.get_NOx_flights_from_catalogue(start_flight_id=start_flight_id,
                                                                     end_flight_id=end_flight_id, flight_type=flight_type,
                                                                     flight_id_list=flight_ids_list,
                                                                     airports_list=airports_list, print_debug=print_debug,
                                                                     iagos_cat_path=cts.IAGOSv3_L2_CAT_PATH)
    else:
        NOx_flights_url = []
        invalid_flight_urls = []
        for flight_path in flight_urls_list:
            if pathlib.Path(flight_path).is_file(): #TODO: check extension ?
                NOx_flights_url.append(flight_path)
            else:
                invalid_flight_urls.append(flight_path)
        if invalid_flight_urls:
            print(f'<!> The following urls do not exist: {invalid_flight_urls}')

    if timenow is None:
        timenow = timestamp_now_formatted(cts.TIMESTAMP_FORMAT, tz='CET')

    if save_output:
        output_dirpath = create_root_output_dir(date=timenow, dirname_suffix=output_dirname_suffix,
                                                root_dirpath=root_output_dirpath)
    else:
        output_dirpath = None

    q3_ds_complete = xr.open_dataset(q3_ds_path).mean('year') if use_q3_ds else None

    skipped_flights = []

    for flight_path in NOx_flights_url:
        if print_debug:
            print('##################################################')
            print(f'flight {flight_path}')
            print('##################################################')
        with xr.open_dataset(flight_path) as flight_ds:
            try:
                flight_ds = get_flight_ds_with_PV_and_valid_data(ds=flight_ds, print_debug=print_debug)
            except MissingVariableError as e:
                print(f'<!> Skipping flight {flight_path}: {str(e)}')
                skipped_flights.append(flight_path)
                continue

            filtered_flight_ds = apply_LiNOx_plume_filters(ds=flight_ds, cruise_only=cruise_only,
                                                           CO_q3=CO_q3, NOx_q3=NOx_q3,
                                                           q3_ds_complete=q3_ds_complete, print_debug=print_debug)

            NOx_tropo_varname = iagos_utils.get_NOx_varname(flight_program=filtered_flight_ds.attrs[cts.PROGRAM_ATTR],
                                                            smoothed=True, tropo=True, filtered=False)

            # only look for plumes if NOx tropo measurements are NOT all nan
            if not np.isnan(filtered_flight_ds[NOx_tropo_varname]).all():
                if save_output:
                    flight_output_dirpath = create_flight_output_dir(output_dirpath=output_dirpath,
                                                                       flight_name=filtered_flight_ds.attrs['flight_name'],
                                                                       dirname_suffix=flight_dirname_suffix)
                    if print_debug:
                        print(f'---\nCreated output dirpath {flight_output_dirpath}\n---')
                    if filtered_ds_to_netcdf:
                        filtered_flight_ds.to_netcdf(f'{flight_output_dirpath}/filtered-ds_{filtered_flight_ds.attrs["flight_name"]}{file_suffix}.nc')
                else:
                    flight_output_dirpath = None

                plume_ds = find_plumes(ds=filtered_flight_ds, min_plume_length=end_of_plume_duration,
                                       flight_output_dirpath=flight_output_dirpath, print_debug=print_debug,
                                       write_plume_info_to_csv=save_output, filename_suffix=file_suffix)

                if save_output and plume_ds_to_netcdf:
                    plume_ds.to_netcdf(
                        f'{flight_output_dirpath}/plume-ds_{filtered_flight_ds.attrs["flight_name"]}{file_suffix}.nc')

                if plot_flight:
                    if not use_q3_ds: #TODO: use_q3_ds might not be used if we end up using same q3 value no matter the region
                        q3_ds = { 'NOx_q3': NOx_q3 if NOx_q3 is not None else cts.NOx_Q3,
                                  'CO_q3': CO_q3 if CO_q3 is not None else cts.CO_Q3 }
                    else:
                        q3_ds = q3_ds_complete

                    iagos_utils.plot_NOx_CO_PV_RHL_O3(ds=plume_ds, q3_ds=q3_ds,
                                                      NOx_plumes=True, NOx_tropo=True, NOx_tropo_filtered=True,
                                                      scatter_NOx_tropo=False, scatter_NOx_excess=False,
                                                      NOx_spike=True, NOx_spike_id=[],
                                                      show_region_names=show_region_names,
                                                      PV=True, RHL=True, CO=True, O3=True,
                                                      save_fig=save_fig, show_fig=show_fig,
                                                      fig_name=None, fig_name_prefix='', fig_name_suffix=file_suffix,
                                                      plot_dirpath=flight_output_dirpath,
                                                      x_axis='UTC_time', x_lim=None, title=None)

    if skipped_flights:
        print('XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX')
        print(f'<!> Skipped {len(skipped_flights)} flights: ')
        print(skipped_flights)
        print('XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX')


if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    # REQUIRED: flight id list
    flights_list_group = parser.add_mutually_exclusive_group(required=True)
    # all flights in output dir
    flights_list_group.add_argument('-a', '--all-flights', action='store_true',
                                    help='Indicates if all flights in output dir should be processed')
    # flight range
    flights_list_group.add_argument('--start-end-flight-ids', nargs=2, type=_none_or_variable,
                                    help='Start and end flight ids (in case we only want to retrieve NOx flights between two specific dates/ids)')
    # list of flights in a txt file
    flights_list_group.add_argument('--flight-ids-list-file',
                                    help='Path to a txt file containing a list of flight ids (1 flight id/line)')
    # list
    flights_list_group.add_argument('--flight-ids', nargs='+', default=[],
                                    help='List of flight ids/names given directly, not via txt file')
    # list of flights urls in a txt file
    flights_list_group.add_argument('--flight-urls-list-file',
                                    help='Path to a txt file containing a list of urls to flight .nc files (1 url/line)')
    # list
    flights_list_group.add_argument('--flight-urls', nargs='+', default=[],
                                    help='List of flight urls given directly, not via txt file')

    flights_group = parser.add_argument_group('flights info')
    flights_group.add_argument('--only-softioli', action='store_true', help='Indicates if only flights within the softioli regions of interests should be taken into account')

    #parser.add_argument('--end-of-plume', nargs='+', type=int, help='End of plume duration (default=100)', default=[100])

    output_group = parser.add_argument_group("output parameters")

    output_group.add_argument('--dont-save-output', action='store_true', help='Indicates if output should NOT be saved')

    output_group.add_argument('--root-output-dir', help=f'Path to root output directory (default=:{cts.OUTPUT_ROOT_DIR})', default=cts.OUTPUT_ROOT_DIR)

    output_group.add_argument('-o', '--output-dirname-suffix', help=f'Output dirname suffix (default=plume_detection_COq3-{cts.CO_Q3}_NOxq3-{cts.NOx_Q3})',
                        default=f'plume_detection_COq3-{cts.CO_Q3}_NOxq3-{cts.NOx_Q3}')
    output_group.add_argument('--filename-suffix', help='suffix to add to each file (default = "_COq3-<CO_q3>_NOxq3-<NOx_q3>"')
    output_group.add_argument('--flight-dirname-suffix', default='',
                              help='suffix to add to flight output directory name')

    output_group.add_argument('-d', '--print-debug', action='store_true', help='print debug (default=False)')

    output_group.add_argument('--save-filtered-ds', action='store_true', help='Indicates if filtered ds should be stored (default=False)')
    output_group.add_argument('--save-plume-ds', action='store_true',
                              help='Indicates if plume ds should be stored (default=False)')

    output_group.add_argument('--show-fig', action='store_true', help='Indicates if flight plot should be shown during execution (<!> stops program execution until plot is closed <!>) (default=False)')

    parser.add_argument('-c', '--CO-q3', type=int, help=f'CO q3, default = {cts.CO_Q3} (value stored in constant file)')
    parser.add_argument('--cruise-only', action='store_true', help='Indicates if only cruise values should be analysed (if False, all measurements made outside of the PBL will be analysed)')

    args = parser.parse_args()

    print(args)

    flight_ids_list, flight_urls_list = None, None
    start_flight_id, end_flight_id = None, None

    if args.flight_ids_list_file:
        flight_ids_list = list_from_file(args.flight_ids_list_file, header=0, ignore_blank_lines=True)
    elif args.flight_ids:
        flight_ids_list = args.flight_ids
    elif args.flight_urls_list_file:
        flight_urls_list = list_from_file(args.flight_urls_list_file, header=0, ignore_blank_lines=True)
    elif args.flight_urls:
        flight_urls_list = args.flight_urls
    elif args.start_end_flight_ids:
        start_flight_id, end_flight_id = args.start_end_flight_ids

    timenow = timestamp_now_formatted(cts.TIMESTAMP_FORMAT, tz='CET')

    get_LiNOX_plumes(
        flight_ids_list=flight_ids_list, flight_urls_list=flight_urls_list,
        start_flight_id=start_flight_id, end_flight_id=end_flight_id,
        only_softioli=args.only_softioli,

        cruise_only=args.cruise_only,
        CO_q3=args.CO_q3,

        print_debug=args.print_debug, save_output=not args.dont_save_output, timenow=timenow, show_region_names=False,

        root_output_dirpath=args.root_output_dir,
        output_dirname_suffix=args.output_dirname_suffix,
        flight_dirname_suffix=args.flight_dirname_suffix,
        file_suffix=args.filename_suffix if args.filename_suffix else f'_COq3-{cts.CO_Q3}_NOxq3-{cts.NOx_Q3}',

        filtered_ds_to_netcdf=args.save_filtered_ds, plume_ds_to_netcdf=args.save_plume_ds,
        plot_flight=True, save_fig=not args.dont_save_output, show_fig=args.show_fig)
