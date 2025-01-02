SOFT-IO-LI v1

Julie Patuel 2024 \
julie.patuel@cnrs.fr

SOFT-IO-LI is split into three seperate programmes. The result are stored in a folder with the following structure (the path to the result folder can be chosen by the user):
```
.
├── <flight-001_id> (example of a flight with potential NOx plume(s))
│   ├── <flight_id>_NOx_CO_PV_RH_O3_over_UTC_time_COq3-110_NOxq3-0.283.png
│   ├── <flight_id>_arrivaltime-<YYYYMMDD-HHmm>_COq3-110_NOxq3-0.283_plume-info.csv
│   ├── flexpart
│   │   ├── FLEXPART
│   │   ├── flexpart.log
│   │   ├── fp_omp.sh
│   │   ├── fp.sh
│   │   ├── input
│   │   │   ├── AGECLASSES
│   │   │   ├── AVAILABLE
│   │   │   ├── COMMAND
│   │   │   ├── IGBP_int1.dat
│   │   │   ├── OUTGRID
│   │   │   ├── RECEPTORS
│   │   │   ├── RELEASES
│   │   │   ├── SPECIES
│   │   │   │   └── SPECIES_024
│   │   │   ├── surfdata.t
│   │   │   └── surfdepo.t
│   │   ├── output
│   │   │   ├── AGECLASSES.namelist
│   │   │   ├── COMMAND.namelist
│   │   │   ├── grid_time_xxxxxxxxxxx.nc (flexpart netcdf output)
│   │   │   ├── header
│   │   │   ├── header_txt
│   │   │   ├── header_txt_releases
│   │   │   ├── OUTGRID.namelist
│   │   │   ├── README.md
│   │   │   ├── RELEASES.namelist
│   │   │   ├── SPECIES_024.namelist
│   │   │   └── trajectories.txt
│   │   └── pathnames
│   ├── flexpart_lightning_comparison
│   │   └── weighted_fp_sat_ds.nc
...
├── <flight-xxx_id> (example of a flight without potential NOx plume)
│   └── <flight-xxx_id>_NOx_CO_PV_RH_O3_over_UTC_time_COq3-110_NOxq3-0.283.png

```

## 1. plume_identification.py

This searches the IAGOS database for new flights measuring either NOx
(using the pack P2b IAGOS CORE) or NO and NO2 (using the pack PC2 CARIBIC).

The flight data is then analysed, only the cruise data is retained for
further analysis. NOx plumes in excess of the 75th percentile, that do not
contain statospheric influences and where the CO is NOT > 110 ppb are retained.

Plumes due to aircraft emissions are removed by ensuring that plumes with a
width of < 27.5 km  are excluded and by removing unusual and sudden spikes.

The final plumes are then analysed and saved into a seperate csv file for each
flight. For each plume identified the following information is saved:

- Plume ID \
- UTC time start \
- UTC time end \
- Longitude start \
- Longitude end \
- Latitude start \
- Latitude end \
- Pressure start \
- Pressure end \
- O3 mean \
- O3 excess std (for each geographical region) \
- O3 excess mean (for each geographical region) \
- CO mean \
- CO excess std (for each geographical region) \
- CO excess mean (for each geographical region) \
- NOx mean \
- NOx excess std \
- NOx excess mean \

If plumes are found and validated then the csv file containing the information for each plume is saved in **path/to/result/dir/<flight_id>/**.

For each flight with valid NOx measurements, a plot representing the flight variable is stored in **path/to/result/dir/<flight_id>/**. It is a good idea to check these plots to make sure the plumes look sensible, so far so good.

The plume_identification.py script can be launched using the following options:
```
python plume_identification.py --help
usage: plume_identification.py [-h] [--flight-list | --flight-range] [--only-softioli] [-s START_ID] [-e END_ID] [--flight-id-list FLIGHT_ID_LIST [FLIGHT_ID_LIST ...]] [--dont-save-output]
                               [-o OUTPUT_DIRNAME_SUFFIX] [--filename-suffix FILENAME_SUFFIX] [--flight-dirname-suffix FLIGHT_DIRNAME_SUFFIX] [-d] [--save-filtered-ds] [--save-plume-ds] [--show-fig]
                               [-c CO_Q3]

options:
  -h, --help            show this help message and exit
  --flight-list         Indicates if a list of flight ids/names will be passed
  --flight-range        Indicates if start and end flight ids/names will be passed
  -c, --CO-q3 CO_Q3     CO q3, default = 110 (value stored in constant file)

flights info:
  --only-softioli       Indicates if only flights within the softioli regions of interests should be taken into account
  -s, --start-id START_ID
                        Start flight name/id (in case we only want to retrieve NOx flights between two flight ids)
  -e, --end-id END_ID   End flight name/id (in case we only want to retrieve NOx flights between two flight ids)
  --flight-id-list FLIGHT_ID_LIST [FLIGHT_ID_LIST ...]
                        List of flight ids/names (default = None)

output parameters:
  --dont-save-output    Indicates if output should NOT be saved
  -o, --output-dirname-suffix OUTPUT_DIRNAME_SUFFIX
                        Output dirname suffix (default=plume_detection_COq3-110-115-120_NOxq3-0.283)
  --filename-suffix FILENAME_SUFFIX
                        suffix to add to each file (default = "_COq3-<CO_q3>_NOxq3-<NOx_q3>")
  --flight-dirname-suffix FLIGHT_DIRNAME_SUFFIX
                        suffix to add to flight output directory name
  -d, --print-debug     print debug (default=False)
  --save-filtered-ds    Indicates if filtered ds should be stored (default=False)
  --save-plume-ds       Indicates if plume ds should be stored (default=False)
  --show-fig            Indicates if flight plot should be shown during execution (<!> stops program execution until plot is closed <!>) (default=False)
```

## 2. flexpart.py

The flexpart.py script can be launched with the following options:
```
python flexpart.py --help
usage: flexpart.py [-h] -fo FLIGHTS_OUTPUT_DIR [--flight-dirname-suffix FLIGHT_DIRNAME_SUFFIX] [-o FP_OUTPUT_DIRNAME] [--overwrite] [-d] [-a] [--flight-list] [--flight-range] [-s START_ID] [-e END_ID]
                   [--flight-id-list FLIGHT_ID_LIST [FLIGHT_ID_LIST ...]] [-x EXCLUDE_FLIGHT_IDS [EXCLUDE_FLIGHT_IDS ...]] [-t TIMESTEP] [-sd SIMU_DURATION] [-gr GRID_RES] [--run-simu]
                   [--slurm-partition SLURM_PARTITION]

options:
  -h, --help            show this help message and exit
  -d, --print-debug     print debug (default=False)

Output directory:
  -fo FLIGHTS_OUTPUT_DIR, --flights-output-dir FLIGHTS_OUTPUT_DIR
                        Path to output directory (directory containing all flight output directories)
  --flight-dirname-suffix FLIGHT_DIRNAME_SUFFIX
                        suffix to add to flight output directory name
  -o FP_OUTPUT_DIRNAME, --fp-output-dirname FP_OUTPUT_DIRNAME
                        Name of the directory where the flexpart output will be stored (default="flexpart")
  --overwrite           Indicates if existing flexpart output directory should be overwritten (default=False)

Flights:
  -a, --all-flights     Indicates if all flights in output dir should be taken into account
  --flight-list         Indicates if a list of flight ids/names will be passed
  --flight-range        Indicates if start and end flight ids/names will be passed
  -s START_ID, --start-id START_ID
                        Start flight name/id (in case we only want to retrieve NOx flights between two flight ids)
  -e END_ID, --end-id END_ID
                        End flight name/id (in case we only want to retrieve NOx flights between two flight ids)
  --flight-id-list FLIGHT_ID_LIST [FLIGHT_ID_LIST ...]
                        List of flight ids/names (default = None)
  -x EXCLUDE_FLIGHT_IDS [EXCLUDE_FLIGHT_IDS ...], --exclude-flight-ids EXCLUDE_FLIGHT_IDS [EXCLUDE_FLIGHT_IDS ...]
                        Flight ids to exclude

Flexpart parameters:
  -t TIMESTEP, --timestep TIMESTEP
                        Timestep for the flexpart simulation (loutstep), (default="1h")
  -sd SIMU_DURATION, --simu-duration SIMU_DURATION
                        Flexpart simulation duration in days, (default=10)
  -gr GRID_RES, --grid-res GRID_RES
                        Flexpart output grid resolution (default=0.5)
  --run-simu            Indicates if flexpart simulation should be run <!> only use when running a few flexpart simulations, use job arrays if you need to run many simulations
  --slurm-partition SLURM_PARTITION
                        Slurm partition on which flexpart should be run (default="o3pwork")
```

FLEXPART_auto uses the contents of FLEXPART_templates to create a FLEXPART
directory for each flight.

This is located in
/o3p/user/SOFT-IO-LI/flexpart10.4/flexpart_v10.4_3d7eebf/src/exercises/soft-io-li/YYYY/MM/flight_id/

It then uses the outputs from the first programme to create the following
control files required by FLEXPART which are unique for each flight.

AVAILABLE

COMMAND

pathnames

RELEASES

Use submit_FLEXPART.sh and submit_FLEXPART.py to submit the job to Nuwa.
submit_FLEXPART.sh will load the modules required ro run corectly on Nuwa.

submit from the flight directory using:

       sbatch -p o3pwork submit_FLEXPART.sh

If the flight is long, for example Europe to West Coast USA, a longer queue
might be required. If so use:

      sbatch -p o3plong submit_FLEXPART.sh
 
2.b. check_FP_output_loc.ipynb

Before going any further a check needs to be made of the geographical location
of the maximum residence time from the FLEXPART output. If the output is not
covered by the extent of the GOES 16 and/or GOES 17 satellites further
analysis will not be possible.

To identify if the flight should be analysed further run the programme

check_FP_ouput_loc.ipynb

The final output of this is a yes or no for further analysis. In time the
coverage can be increased for GOES 18, EUMETSAT and other cloud/lightning data
as it becomes available.

Once it has beeen ascertained that further analysis is possible and before
running the third program, the user must download and prepare the
corresponding cloud and lightning data that will be used for the comparisons
with the FLEXPART maximum residence time outputs produced by program 2. 

2.c. GOES_ABI_2015.ipynb GOES_ABI.ipynb GLM_grouping_by_lon_lat.ipynb

********************************************************************************
For ABI cloud data:

ftp to collect ABI cloud data from ICARE Lille

For 2015:
#########
ftp -i ftp.icare.univ-lille1.fr
username
password
cd /SPACEBORNE/GEO/GOES-0750/GEO_L1B/2015/2015_07_01
lcd /o3p/usr/test/ABI/2015/182
mget GEO_L1B-GOES13_2015-07-01T*_G_IR107_V1-06.hdf

bye

For 2018:
#########
ftp -i ftp.icare.univ-lille1.fr
username
password
cd /SPACEBORNE/GEO/GOESNG-0750/GEO_L1B/2018/2018_05_30
lcd /o3p/usr/test/ABI/150
mget GEO_L1B-GOES16_2018-05-30T*_G_IR103_V1-06.hdf
bye

In jupyter, use GOES_ABI_2015.ipynb or GOES_ABI.ipynb to preprocess data, just
update the input data of day and date

********************************************************************************
For GLM lightning data:

ftp to collect GLM lightning data from ICARE Lille

ftp -i ftp.icare.univ-lille1.fr
username
password
cd /SPACEBORNE/GEO/GOESNG-0750/GLM/2018/2018_05_30
lcd /o3p/usr/glm/OR_GLM-L2-LCFA_G16_s2018150/temp_00
mget OR_GLM-L2-LCFA_G16_s201815000*

bye

On nuwa need to untar the GLM files and create a single netCDF file for each
hour using ncrcat

cd /o3p/usr/glm/OR_GLM-L2-LCFA_G16_s2018150/temp_00
conda activate GLM_test
tar -xavf OR_GLM-L2-LCFA_G16_s20181511600000_e20181511615000_c2018151162527.tgz 
ncrcat -h *.nc GLM_array_151_20-21.nc
ncrcat -h *.nc GLM_array_150_00-01.nc

When it finishes:

mv GLM_array_151_20-21.nc ../
rm *
cd ../
rmdir temp_00

In jupyter, use GLM_grouping_by_lon_lat.ipynb to preprocess data

********************************************************************************

3. flight_2018_001_1h_05deg.ipynb

This has not yet been automated, but contains the comparisons between the
FLEXPART output and the GLM lightning data over many of the 240 hours of data.

flight_2018_003_05deg.ipynb contains the comparisons with the cloud data as
well.

The flight path and departure and arrival airports are added to the plots.

The programmes also calculate the statistics of the NOX, O3 and CO in the
observed plume. flights from 2018 can be seen in /home/macc/test/soft-io-li/

There are also programmes for 2015 flights and the start of the automation for
these programmes.

********************************************************************************
User Guide

SOFT-IO-LI is written in Python 3 and can be run either as batch jobs on nuwa
using the .py files, or alternatively, interactively using JupyterLab on nuwa
using the .ipynb files. JupyterLab allows you to view plots immediately and is
quick even when running over many flights.

SOFT-IO-LI files and directories are stored on nuwa in the iagos space:
/o3p/iagos/SOFT-IO-LI/

Please do not edit the .ipynb or .py files here.
To test changes please edit copies in your user space.
A copy of this version is available from Github in case of problems:
https://github.com/ckmackay/SOFT-IO-LI.git

In order to run the SOFT-IO-LI programmes in JupyterLab follow the
instructions given for using conda and launching JupyterLab on nuwa
(http://nuwa.aero.obs-mip.fr/CondaNuwa).

On nuwa in your own space /home/user/ create a SOFT-IO-LI directory.

Make a copy of /o3p/iagos/SOFT-IO-LI/ locally, for example /o3p/user/SOFT-IO-LI/

The environment used for SOFT-IO-LI is saved in environment.yml

Create the environment where you will launch JupyterLab from, for example,
/home/user/SOFT-IO-LI using:

	conda env create -f environment.yml

Activate the environment, which is called hvenv, using:

	conda activate hvenv

Verify the environment is installed correctly using:

        conda env list

hvenv should now be in your list of conda environments.

At the start of each session to use SOFT-IO-LI, activate the hvenv environment
using:
	conda activate hvenv

(hvenv) will now appear at the start of each command line.

Copy the SOFT-IO-LI files to your local directory /home/user/SOFT-IO-LI/

If you are using Jupyter lab copy the *.ipynb files, otherwise copy the .py
and corresponding .sh files.

Each programme contains the imports first and if the hvenv environment is
correctly activated there will be no problems with the imports.

The paths are set next.

You will need the change the /user/ to your user id for the paths.

Once this is done you should be able to run the programmes and see the output.

Any problems send a mail to catherine.mackay@aero.obs_mip.fr

********************************************************************************
FLEXPART information.

SOFT-IO-LI uses FLEXPART release 10.4 and the following files in the source code
were modified:

1. par_mod.f was modified at line 143 to allow for the 0.5° ERA5 ECMWF data.

flexpart10.4/flexpart_v10.4_3d7eebf/src/ contains 3 FLEXPART compilations:

FLEXPART for 1°:
integer,parameter :: nxmax=361,nymax=181,nuvzmax=138,nwzmax=138,nzmax=138,nxshift=359 ! 1.0 degree 138 level

FLEXPART_05 for 0.5°:
integer,parameter :: nxmax=721,nymax=361,nuvzmax=138,nwzmax=138,nzmax=138,nxshift=359  ! 0.5 degree 138 level

FLEXPART 0.25 for 0.25°
integer,parameter :: nxmax=1441,nymax=721,nuvzmax=138,nwzmax=138,nzmax=138,nxshift=359  ! 0.25 degree 138 level

SOFT-IO-LI uses FLEXPART_05 as default.

2. makefile was modified to allow compilation on nuwa.

********************************************************************************


