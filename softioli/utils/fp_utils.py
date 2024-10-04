import pandas as pd
import pathlib


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