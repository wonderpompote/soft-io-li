SOFT-IO-LI v1

Julie Patuel 2024 \
julie.patuel@cnrs.fr

SOFT-IO-LI is split into three seperate programs. The result are stored in a folder with the following structure (the path to the result folder can be chosen by the user):
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

- Plume ID
- UTC time start
- UTC time end
- Longitude start
- Longitude end
- Latitude start
- Latitude end
- Pressure start
- Pressure end
- O3 mean
- O3 excess std (for each geographical region)
- O3 excess mean (for each geographical region)
- CO mean
- CO excess std (for each geographical region)
- CO excess mean (for each geographical region)
- NOx mean
- NOx excess std
- NOx excess mean

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

## 3. fpout_sat_comparison.py

```
python softioli/fpout_sat_comparison.py --help
usage: fpout_sat_comparison.py [-h] -fo FLIGHTS_OUTPUT_DIR [--flight-dirname-suffix FLIGHT_DIRNAME_SUFFIX] [-o FP_OUTPUT_DIRNAME] [--flight-list] [--flight-range] [-a] [-s START_ID] [-e END_ID] [--flight-id-list FLIGHT_ID_LIST [FLIGHT_ID_LIST ...]] [--lightning-sat-name LIGHTNING_SAT_NAME]
                               [--cloud-sat-name CLOUD_SAT_NAME] [--grid-res GRID_RES] [--grid-res-str GRID_RES_STR] [--dont-sum-height] [--load-fpout] [--save-weighted-ds] [--ds-fname-suffix DS_FNAME_SUFFIX] [--overwrite-weighted-ds] [--dry-run] [-d] [--overwrite-sat-files]
                               [--rm-pre-regrid-abi-file] [--rm-pre-regrid-glm-file]

options:
  -h, --help            show this help message and exit
  --dry-run             dry run (fp_out and glm_out NOT loaded into memory and weighted flash count NOT calculated)
  -d, --print-debug     print debug (default=False)
  --overwrite-sat-files
                        Indicates if existing pre_regrid and regrid satellite files should be overwritten
  --rm-pre-regrid-abi-file
                        Indicates if pre_regrid hourly ABI file should be removed once the corresponding regrid file has been generated (to free up space)
  --rm-pre-regrid-glm-file
                        Indicates if pre_regrid hourly GLM file should be removed once the corresponding regrid file has been generated (to free up space)

Directories:
  -fo FLIGHTS_OUTPUT_DIR, --flights-output-dir FLIGHTS_OUTPUT_DIR
                        Path to output directory (directory containing all flight output directories)
  --flight-dirname-suffix FLIGHT_DIRNAME_SUFFIX
                        suffix to add to flight output directory name
  -o FP_OUTPUT_DIRNAME, --fp-output-dirname FP_OUTPUT_DIRNAME
                        Name of the directory where the flexpart output will be stored (default="flexpart")

Flights:
  --flight-list         Indicates if a list of flight ids/names will be passed
  --flight-range        Indicates if start and end flight ids/names will be passed
  -a, --all-flights     Indicates if all flights in output dir should be taken into account
  -s START_ID, --start-id START_ID
                        Start flight name/id (in case we only want to retrieve NOx flights between two flight ids)
  -e END_ID, --end-id END_ID
                        End flight name/id (in case we only want to retrieve NOx flights between two flight ids)
  --flight-id-list FLIGHT_ID_LIST [FLIGHT_ID_LIST ...]
                        List of flight ids/names (default = None)

Satellite parameters:
  --lightning-sat-name LIGHTNING_SAT_NAME
                        Lightning satellite name (default=GOES_GLM)
  --cloud-sat-name CLOUD_SAT_NAME
                        Cloud brightness temperature satellite name (default=GOES_ABI)
  --grid-res GRID_RES   Satellite grid resolution (default=0.5)
  --grid-res-str GRID_RES_STR
                        Satellite grid resolution string, format="<res>deg" (default=05deg)

Flexpart output parameters:
  --dont-sum-height     Indicates if flexpart output should NOT be summed over altitude (by default it is because satellite data does not have altitude information)
  --load-fpout          load fp_out dataArray into memory (default=False)

Weighted ds parameters:
  --save-weighted-ds    Indicates if weighted ds should be saved
  --ds-fname-suffix DS_FNAME_SUFFIX
                        Suffix to add to the weighted ds filename. The dataset will be stored in the flexpart_lightning_comparison directory in the flight output directory (default suffix="")
  --overwrite-weighted-ds
                        Indicates if weighted ds should be overwritten if it aleady exists

```

