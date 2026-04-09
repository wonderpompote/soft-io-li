#!/home/patj/miniconda3/envs/softioli-src/bin/python

import matplotlib.pyplot as plt
from matplotlib.colors import LogNorm
import cartopy.crs as ccrs
import cartopy.feature as cfeature
import matplotlib.dates as mdates

import argparse
import pandas as pd
import pathlib
import numpy as np

from common.utils import list_from_file

from utils.fp_utils import get_fpout_nc_file_path_from_fp_dir, get_fp_out_ds_xdays


def plot_pcolor_da(
        x, y, c, xlabel, ylabel, clabel, title,
        xlim=None, ylim=None, clim=None,
        cmap='coolwarm', log_scale=False,
        display_coastlines=False, central_lon=None,
        save_plot=False, overwrite=False,
        res_dirpath=None, res_filename='plot.png',
        figsize=(12, 6), show_plot=True
):
    # set up directory to save plot if needed
    if save_plot:
        pathlib.Path(res_dirpath).mkdir(parents=True, exist_ok=True)
        if pathlib.Path(f'{res_dirpath}/{res_filename}').exists() and not overwrite:
            print(f'{res_dirpath}/{res_filename} already exists !')
            return

    # --- plot ---
    fig = plt.figure(figsize=figsize)

    # display coastlines, borders and --- lines for longitude, latitude
    if display_coastlines:
        projection = ccrs.PlateCarree() if central_lon is None else ccrs.PlateCarree(central_lon)
        ax = plt.axes(projection=projection)
        ax.gridlines(draw_labels=True, color='black', alpha=0.2, linestyle='--')
        ax.add_feature(cfeature.COASTLINE, lw=0.5)
        ax.add_feature(cfeature.BORDERS, linestyle=':', lw=0.6, alpha=0.7)
        transform = ccrs.PlateCarree()
    else:
        ax = fig.add_subplot()
        transform = None

    # set axes limits if given as arguments
    if xlim:
        ax.set_xlim(xlim)
    if ylim:
        ax.set_ylim(ylim)

    # handles x axis being timestamps
    x_plot = x
    if np.issubdtype(np.array(x).dtype, np.datetime64):
        x_plot = mdates.date2num(pd.DatetimeIndex(x))
        ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d %H:%M'))
        ax.xaxis.set_major_locator(mdates.AutoDateLocator())
        plt.gcf().autofmt_xdate()  # rotate labels to avoid overlap

    # pcolors parameters
    pcolor_kwargs = {'cmap': cmap}
    if transform is not None:
        pcolor_kwargs['transform'] = transform
    if log_scale:

        if not clim:
            cmin, cmax = 1, c.max()
        else:
            cmin, cmax = clim[0] if clim[0] != 0 else 1, clim[1]
        pcolor_kwargs['norm'] = LogNorm(vmin=cmin, vmax=cmax)

    # pcolor plot
    pcolor_plot = ax.pcolor(x_plot, y, c, **pcolor_kwargs)

    # colorbar and axis labels
    plt.colorbar(pcolor_plot, orientation='vertical').set_label(clabel)
    if clim:
        pcolor_plot.set_clim(clim)
    ax.set_xlabel(xlabel)
    ax.set_ylabel(ylabel)

    plt.title(title, fontsize=17)  # fontsize=17, weight='bold')
    plt.tight_layout()

    if save_plot:
        plt.savefig(f'{res_dirpath}/{res_filename}', bbox_inches='tight')
        print(f'Saved {res_dirpath}/{res_filename} !')

    if show_plot:
        plt.show()

    plt.close(fig)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    # REQUIRED plot type
    plot_type_group = parser.add_mutually_exclusive_group(required=True)
    plot_type_group.add_argument('--fp-summed-res-time', action='store_true',
                                 help="Plot summed residence time over longitude-latitude")
    plot_type_group.add_argument('--fp-hourly-res-time', action='store_true',
                                 help="Plot hourly residence time over longitude-latitude")
    # ... add plot types for satellite data

    # REQUIRED flight id(s)
    flight_group = parser.add_mutually_exclusive_group(required=True)
    # list of flights in a txt file
    flight_group.add_argument('--flight-id-list',
                              help='Path to a txt file containing a list of flight ids (1 flight id/line)')
    # list of flights directly given
    flight_group.add_argument('--flight-id', nargs='+', default=[],
                              help='List of flight ids/names given directly, not via txt file')

    # REQUIRED parameters to find correct dataset
    softioli_group = parser.add_argument_group()
    softioli_group.add_argument('--softioli-output-dir', type=pathlib.Path,
                                default=pathlib.Path('/o3p/patj/SOFT-IO-LI_output/2024-10-24_1543_all-NOx-flights_CO-110/'),
                                help='Path to SOFT-IO-Li output directory, containing all flight output directories (default=/o3p/patj/SOFT-IO-LI_output/2024-10-24_1543_all-NOx-flights_CO-110/)')
    softioli_group.add_argument('--fp-output-dirname', default='flexpart',
                              help='Name of the directory where the flexpart output is stored (default="flexpart")')

    # output directory
    output_group = parser.add_argument_group("Output parameters")
    output_group.add_argument('-o', '--output-dir', required=True,
                              help="Path to output directory (where the plots should be stored)")
    output_group.add_argument('--plot-filename',
                              help="Plot filename (default=<flight_id>_<plot_type>.png)")
    output_group.add_argument('--overwrite', action='store_true',
                              help='Indicates if existing plot should be overwritten')

    # plot parameters
    plot_group = parser.add_argument_group("Plot parameters")

plot_group.add_argument('-x', '--x-axis-var', default='longitude',
                        help='x-axis variable name (default=longitude)')
plot_group.add_argument('--xlabel', default='Longitude (°)',
                        help='x-axis label (default=Longitude (°))')
plot_group.add_argument('-y', '--y-axis-var', default='latitude',
                        help='y-axis  variable name (default=latitude)')
plot_group.add_argument('--ylabel', default='Latitude (°)',
                        help='y-axis label (default=Latitude (°))')
plot_group.add_argument('-c', '--c-var', default='spec001_mr',
                        help='c variable name (default=spec001_mr)')
plot_group.add_argument('--clabel', default='residence time (s)',
                        help='clabel (default=residence time (s))')
plot_group.add_argument('--title', help='Plot title')
plot_group.add_argument('--cmap', default='coolwarm',
                        help='color map (default=coolwarm)')
plot_group.add_argument('--log-scale', action='store_true',
                        help='Indicates if log scale should be used')

args = parser.parse_args()
print(args)

# retrieve list of flight ids
flight_id_list = []
if args.flight_id_list:  # txt file containing flight ids
    flight_id_list = list_from_file(args.flight_id_list, header=0, ignore_blank_lines=True)
else:  # flight ids passed directly in command line
    flight_id_list = args.flight_id

# create output dir if it doesn't exist
pathlib.Path(args.output_dir).mkdir(exist_ok=True, parents=True)

for flight_id in flight_id_list:
    # plot residence time over longitude - latitude (summed over height and time)
    if args.fp_summed_res_time:
        fpout_dirpath = f'{args.softioli_output_dir}/{flight_id}/{args.fp_output_dirname}'
        if not pathlib.Path(fpout_dirpath).exists():
            print(f'{fpout_dirpath} does not exist ! Skipping flight {flight_id}')
            continue
        else:
            fpout_nc_filepath = get_fpout_nc_file_path_from_fp_dir(fp_dirpath=fpout_dirpath)
            fp_da = get_fp_out_ds_xdays(fpout_nc_filepath, days=7, sum_height=True, chunks='auto',
                                        max_chunk_size=1e8, assign_releases_position_coords=False)['spec001_mr']
            for rel_index in fp_da.pointspec.data:
                # check if plot already exists (or should be overwritten)
                rel_index_str = str(rel_index)
                plot_filename = args.plot_filename if args.plot_filename else f'{flight_id}_rel{rel_index_str}_res-time-summed-over-7days.png'
                if not pathlib.Path(f'{args.output_dir}/{plot_filename}').exists() or args.overwrite:
                    fp_da_relx = fp_da.isel(pointspec=rel_index).sum('time')
                    plot_title = args.title if args.title else f'Flight {flight_id} rel{rel_index_str} - residence time summed over 7 days'

                    plot_pcolor_da(
                        x=fp_da_relx[args.x_axis_var], xlabel=args.xlabel,
                        y=fp_da_relx[args.y_axis_var], ylabel=args.ylabel,
                        c=fp_da_relx, clabel=args.clabel,
                        title=plot_title,
                        xlim=None, ylim=None, clim=None,
                        cmap=args.cmap, log_scale=args.log_scale,
                        display_coastlines=True, central_lon=None,
                        save_plot=True, overwrite=args.overwrite,
                        res_dirpath=args.output_dir, res_filename=plot_filename,
                        figsize=(12, 6), show_plot=False
                    )
                else:
                    print(
                        f'{args.output_dir}/{plot_filename} already exists! Use --overwrite option if you want to overwrite it. Skipping flight {flight_id}')

    elif args.fp_hourly_res_time:
        print('Hourly flexpart residence time plot not implemented yet!')

    else:
        print(f'No other plot type supported for now')

print('---\n')

