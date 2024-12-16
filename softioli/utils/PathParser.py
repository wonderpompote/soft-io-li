from abc import ABC, abstractmethod

class PathParser(ABC):

     @abstractmethod
     def extract_date(self):
         pass

     @abstractmethod
     def extract_regrid_res(self):
         pass

     @abstractmethod
     def extract_satellite(self):
         pass

     @abstractmethod
     def get_start_date_pdTimestamp(self):
         pass

     @abstractmethod
     def print(self):
         pass