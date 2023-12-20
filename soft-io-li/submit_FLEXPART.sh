#!/bin/bash
#
#SBATCH --output="submit_FLEXPART-%j"

module load gnu/7.5.0
module load gnu_gribapi/1.13.1
module load gnu_netcdf/4.4.5
date
./submit_FLEXPART.py
date
exit 0
