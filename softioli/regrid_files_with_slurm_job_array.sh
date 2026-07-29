#!/bin/bash

#SBATCH --job-name=regrid_sat_data
#SBATCH --ntasks=1
#SBATCH -o /home/patj/logs/regrid_slurm/arr_job-%N-%j_%A-%a.out

#####################################################################################################
# Script to regrid hourly .nc files with a slurm array
#
# usage: sbatch --array=xx-xx%xxx $0 SAT_NAME FILE_WITH_ARGS [EXTRA_PYTHON_ARGUMENTS]
#   required arguments:
#	SAT_NAME: name of the satellite (supported values: GOES_GLM, GOES_ABI, MTG_LI)
#	FILE_WITH_ARGS: txt file with list of path pointing to hourly .nc files or directories containing hourly .nc files that need to be regridded
#   optional arguments:
#	EXTRA_PYTHON_ARGUMENTS: optional, extra arguments to be passed to the python regrid script (for example --overwrite --regrid-res 0.5 etc.)
#
# time needed to regrid a single .nc file: < 1min
# memory used to regrid a single .nc file: ~ 0.12GB
#####################################################################################################

if [ $# -lt 2 ]; then
        echo "Usage: sbatch --array=xx-xx%xxx $0 SAT_NAME FILE_WITH_ARGS [EXTRA_PYTHON_REGRID_ARGS]"
        exit 1
fi

# maps each line of file to an array element
mapfile -t ARGS_LIST < "$2"
# select args corresponding to current array task (for job number 5, select args element at index 5)
ARGS=${ARGS_LIST[${SLURM_ARRAY_TASK_ID}]}
SAT_NAME=$1

# shift so that remaining args (optional extra python args) can be accessible with $@
shift 2
EXTRA_PYTHON_ARGS=$@

# check if path passed points to a directory of a file
if [ -d "$ARGS" ]; then
	path_type_args="--dir-path"
elif [ -f "$ARGS" ]; then
	path_type_args="--file-path"
else
	echo "<!> ERROR: '$ARGS' is neither a directory nor a file, please check the paths and try again"
	exit 1
fi

date
echo "--- START regrid ---"
echo "Satellite name: $SAT_NAME"
echo "File to regrid: $ARGS"

/home/patj/miniconda3/envs/softioli-src/bin/python /home/patj/SOFT-IO-LI/src/softioli/regrid_daily_dir_script.py --sat-name $SAT_NAME $path_type_args $ARGS $EXTRA_PYTHON_ARGS --print-debug

echo "----------------------"
date
