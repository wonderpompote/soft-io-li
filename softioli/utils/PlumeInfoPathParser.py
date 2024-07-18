import pandas as pd
import pathlib

from utils_functions import str_to_path, date_to_pd_timestamp

class PlumeInfoPathParser():

    def __init__(self, url, flight_name=None, arrival_timestamp=None):
        self.url = str_to_path(url)
        self.flight_name = flight_name
        self.arrival_timestamp = pd.Timestamp(arrival_timestamp)
        if any(self.flight_name, self.arrival_timestamp):
            self.extract_flight_name_and_arrival_timestamp()
        """
        est-ce que pathparser pour extract les infos du fichier pour ensuite flexpart
        + fonctions pour generate file path
        """

    def extract_flight_name_and_arrival_timestamp(self):
        split_url = self.url.stem.split('_') # <flight_name>_<arrival_timestamp>_plume-info.csv
        flight_name = split_url[0]
        arrival_timestamp = pd.Timestamp(split_url[1])

        if self.flight_name is None:
            self.flight_name = flight_name
        if self.arrival_timestamp is None:
            self.arrival_timestamp = arrival_timestamp