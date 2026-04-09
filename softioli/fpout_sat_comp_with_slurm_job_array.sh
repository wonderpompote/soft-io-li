#!/bin/bash

#SBATCH --job-name=fp_sat_comp
#SBATCH --ntasks=1
#SBATCH -o /home/patj/logs/fp_sat_comp_slurm/arr_job-%N-%j_%A-%a.out

#####################################################################################################
# Script to compare flexpart output and satellite data with a slurm array
#
# usage: sbatch --array=xx-xx%xxx $0 FILE_WITH_ARGS SOFTIOLI_OUTPUT_DIRPATH [EXTRA_PYTHON_ARGS]
#   required arguments:
#       FILE_WITH_ARGS: txt file with list of flight ids (1 flight id/line)
#	SOFTIOLI_OUTPUT_DIRPATH: 
#   optional arguments:
#       EXTRA_PYTHON_ARGUMENTS: optional, extra arguments to be passed to the python fpout_sat_comparison script (for example --dry-run --grid-res 0.5 etc.)
#
# time needed for a single flight: xx min
# memory used for a single flight: ~ xx GB
#####################################################################################################


if [ $# -lt 2 ]; then
        echo "Usage: sbatch --array=xx-xx%xxx $0 FILE_WITH_ARGS SOFTIOLI_OUTPUT_DIRPATH [EXTRA_PYTHON_ARGS]"
        exit 1
fi

# maps each line of file to an array element
mapfile -t ARGS_LIST < "$1"
# select args corresponding to current array task (for job number 5, select args element at index 5)
ARGS=${ARGS_LIST[${SLURM_ARRAY_TASK_ID}]}
SOFTIOLI_OUTPUT_DIR=$2

if [ ! -d $SOFTIOLI_OUTPUT_DIR ]; then
	echo "<!> $SOFTIOLI_OUTPUT_DIR does not exist, please check the oath and try again"
	exit 1
fi

# shift so that remaining args (optional extra python args) can be accessible with $@
shift 2
EXTRA_PYTHON_ARGS=$@

date
echo "--- START fpout_sat_comparison (SLURM_ARRAY_TASK_ID=$SLURM_ARRAY_TASK_ID) ---"
echo "Flight: $ARGS"
echo "SOFT-IO-Li output directory: $SOFTIOLI_OUTPUT_DIR"
echo "extra python arguments: $EXTRA_PYTHON_ARGS"
echo "Running: /home/patj/miniconda3/envs/softioli-src/bin/python /home/patj/SOFT-IO-LI/src/softioli/fpout_sat_comparison.py --flight-id ${ARGS} --softioli-output-dir ${SOFTIOLI_OUTPUT_DIR} ${EXTRA_PYTHON_ARGS} --print-debug"
echo ""

/home/patj/miniconda3/envs/softioli-src/bin/python /home/patj/SOFT-IO-LI/src/softioli/fpout_sat_comparison.py --flight-id $ARGS --softioli-output-dir $SOFTIOLI_OUTPUT_DIR $EXTRA_PYTHON_ARGS --print-debug

echo ""
echo "----------------------"
date

