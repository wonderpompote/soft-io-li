import pandas as pd
import pathlib

from fpsim import check_fp_status

from .constants import YYYY_pattern

def get_arrival_timestamp_from_plume_csv_filename(csv_file_path):
    """
    Returns timestamp corresponding to flight arrival time (as indicated in plume csv filename)
    <!> expecting filename with following format: <flight_id>_arrivaltime-YYYYMMDD-HHmm_<extra_information>.csv
    :param csv_file_path: <pathlib.Path> or <str>
    :return: <pandas.Timestamp> arrival time
    """
    if not isinstance(csv_file_path, pathlib.Path):
        csv_file_path = pathlib.Path(csv_file_path)
    return pd.Timestamp(csv_file_path.name.split('_')[1].replace('arrivaltime-',''))

def get_timestamp_next_hour(timestamp):
    """
    Returns timestamp with next hour
    ex: pd.Timestamp('2018-06-03 12:50:00') will return pd.Timestamp('2018-06-03 13:00:00')
        pd.Timestamp('2018-06-03 07:03:00') will return pd.Timestamp('2018-06-03 08:00:00')
    :param: <pandas.Timestamp> or <str>
    :return: <pandas.Timestamp> timestamp to next hour
    """
    return pd.Timestamp(timestamp).floor('h') + pd.Timedelta(1,'h')


#TODO: <!> le pattern fonctionne PAS
def get_fpout_nc_file_path_from_fp_dir(fp_dirpath, fp_output_dirname='output',
                                       nc_file_glob_pattern=f'grid_time_{YYYY_pattern}*.nc'):
    """
    Takes flexpart directory as argument and returns flexpart output netcdf file (if flexpart simulation was a success)
    @param fp_dirpath: <pathlib.Path> or <str> path to the flexpart directory
    @param fp_output_dirname: <str> output flexpart directory name (default='output')
    @param nc_file_glob_pattern: <str> pattern that should be used in the glob operation to find the .nc file
    @return: <pathlib.Path>
    """
    fp_dirpath = pathlib.Path(fp_dirpath)
    # check si fp success
    if check_fp_status(fp_dirpath):
        fp_output_dirpath = pathlib.Path(f'{fp_dirpath}/{fp_output_dirname}')
        # va chercher le fichier nc dans /output avec glob
        return sorted(fp_output_dirpath.glob(f'{nc_file_glob_pattern}'))[0]