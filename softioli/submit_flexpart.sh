#!/bin/bash

#####################################################################################################
# Script to run SOFT-IO-Li flexpart simulations with a slurm array
# 
# 2 main steps: 
#	1- create flexpart input files for each flight in the flights list (by running softioli/flexpart.py)
#	2- run flexpart simulation for each flight in the flights list
#
# usage: [-o SOFTIOLI_OUTPUT_DIRPATH] [-f FLEXPART_DIRNAME] [-l LOG_DIRNAME] FLIGHT_ID_LIST THROTTLE [EXTRA_SBATCH_ARGUMENTS]
#   required arguments:
#	FLIGHT_ID_LIST: path to txt file containing flight ids (1 flight id / line)
#	THROTTLE: maximum number of flexpart simulations that can be run at the same time (depends on the nodes and partition the flexpart simulations will be run on)
#		ex: if running flexpart on o3pwork with 3 nodes available, throttle = 12 (4 FP simus per node x 3 nodes)
#   optional arguments:
#	-o SOFTIOLI_OUTPUT_DIRPATH: optional, path to the directory containing the output directories for each flight (default = /o3p/patj/SOFT-IO-LI_output/2024-10-24_1543_all-NOx-flights_CO-110)
#	-f FLEXPART_DIRNAME: optional, flexpart directory name where the input and output files created will be stored (default = flexpart)
#	-l LOG_DIRNAME: optional, relative path to target log directory inside of ~/logs (default = flexpart_slurm_array)
#	EXTRA_SBATCH_ARGUMENTS: optional, extra sbatch arguments to run the array (for example -p o3pwork -x n201 ...)
#
#####################################################################################################

### default values
CONDA_ENV_NAME=softioli-src
SOFT_IO_LI_ROOT_PATH="/home/patj/soft-io-li_dev/src_github/softioli"
ERA5_DIR="/o3p/wolp/ECMWF/ERA5/050deg_1h_T319_eta1/"

### get directory paths and names (default or passed as params)
flight_output_dirpath="/o3p/patj/SOFT-IO-LI_output/2024-10-24_1543_all-NOx-flights_CO-110"
flexpart_output_dirname="flexpart"
log_dirname="flexpart_slurm_array"
current_date=$(date +'%Y-%m-%d_%H%M')

# read output dirnames and path if given as arguments
while getopts "o:f:l:" opt; do
	case $opt in
		o) flight_output_dirpath="$OPTARG" ;;
		f) flexpart_output_dirname="$OPTARG" ;;
		l) log_dirname="$OPTARG" ;;
	esac
done

shift $((OPTIND -1)) # removes named arguments from list so now $1=<flights_list.txt> etc.

# create log directory if it does not exist
log_directory=~/logs/$log_dirname
mkdir -p $log_directory

### check if flights list exists
FLIGHTS_LIST="$1"
if [ ! -f "$FLIGHTS_LIST" ]; then
echo "Error: $FLIGHTS_LIST not found"
exit 1
fi

### 1- create all FLEXPART input files
# run python script that creates flexpart input files for each flight
CREATE_FLEXPART_INPUT_FILES_JOB=$(sbatch --parsable \
--job-name=fp_create_inputs \
--partition=o3pwork,any \
--output="$log_directory/${current_date}_create_fp_inputs_%j.out" \
--wrap="conda run -n $CONDA_ENV_NAME python $SOFT_IO_LI_ROOT_PATH/flexpart.py --flight-list $FLIGHTS_LIST --flights-output-dir $flight_output_dirpath --fp-output-dirname $flexpart_output_dirname --era5-dir $ERA5_DIR --debug")

echo "Input creation job: $CREATE_FLEXPART_INPUT_FILES_JOB"



### slurm array parameters
THROTTLE="$2" # max number of jobs that can be run at the same time (depends on the partition/nodes used)
shift 2   # remaining args go to the array sbatch

nb_of_flights=$(wc -l < "$FLIGHTS_LIST")
echo "Flights: $nb_of_flights  |  Throttle: %${THROTTLE}"
echo "Extra sbatch args: $@"

# Step 2: run simulations once inputs are ready, forwarding your sbatch params
RUN_FP_SIM_JOB=$(sbatch --parsable \
--dependency=afterok:$CREATE_FLEXPART_INPUT_FILES_JOB \
--array=0-$((nb_of_flights-1))%${THROTTLE} \
--output="$log_directory/${current_date}_fp_%A-%a_%N.out" \
--error="$log_directory/${current_date}_fp_%A-%a_%N.err" \
"$@" \
flexpart_array.sh -o "$flight_output_dirpath" -f "$flexpart_output_dirname" "$FLIGHTS_LIST")

echo "Simulation array job: $RUN_FP_SIM_JOB (depends on $CREATE_FLEXPART_INPUT_FILES_JOB)"


