#!/bin/bash
#SBATCH --job-name=fp_array
#SBATCH --ntasks=1
#SBATCH --nodes=1

#####################################################################################################
# Script to run a SOFT-IO-Li flexpart simulation
#
# usage: [-o SOFTIOLI_OUTPUT_DIRPATH] [-f FLEXPART_DIRNAME] FLIGHT_ID_LIST
#   required arguments:
#	FLIGHT_ID_LIST: path to txt file containing flight ids (1 flight id / line)
#   optional arguments:
#	-o SOFTIOLI_OUTPUT_DIRPATH: optional, path to the directory containing the output directories for each flight (default = /o3p/patj/SOFT-IO-LI_output/2024-10-24_1543_all-NOx-flights_CO-110)
#	-f FLEXPART_DIRNAME: optional, flexpart directory name where the input and output files created will be stored (default = flexpart)#
#####################################################################################################

### get directory paths and names (default or passed as params)
flight_output_dirpath="/o3p/patj/SOFT-IO-LI_output/2024-10-24_1543_all-NOx-flights_CO-110"
flexpart_output_dirname="flexpart"

# read output dirnames and path if given as arguments
while getopts "o:f:" opt; do
	case $opt in
		o) flight_output_dirpath="$OPTARG" ;;
		f) flexpart_output_dirname="$OPTARG" ;;
	esac
done

shift $((OPTIND -1))


# read flight id from txt file corresponding to current task
mapfile -t FLIGHTS < "$1"
FLIGHT_ID=${FLIGHTS[${SLURM_ARRAY_TASK_ID}]}

if [ -z "$FLIGHT_ID" ]; then
echo "Error: no flight at index ${SLURM_ARRAY_TASK_ID}"
exit 1
fi

# get simulation directory
SIM_DIR="${flight_output_dirpath}/${FLIGHT_ID}/${flexpart_output_dirname}"
if [ ! -d "$SIM_DIR" ]; then
echo "Error: $SIM_DIR not found"
exit 1
fi

### run flexpart simulation
. /etc/profile.d/modules.sh
export MODULEPATH=/home/sila/modules/compilers:/home/sila/modules/libraries/generic
export MODULECONFIGFILE=/home/sila/modules/config/modulerc

module load gnu/7.5.0
module load gnu_netcdf/4.4.5
module load gnu_gribapi/1.13.1

cd "$SIM_DIR"
echo "Flight: $FLIGHT_ID"
date
echo "FLEXPART start"
/usr/local/slurm/bin/srun ./FLEXPART > flexpart.log 2>&1
STATUS=$?
echo "FLEXPART done (exit $STATUS)"
date
exit $STATUS

