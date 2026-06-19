import pathlib
from pandas import Timestamp

#----- output -----
OUTPUT_ROOT_DIR = pathlib.Path('/o3p/patj/SOFT-IO-LI_output')
TIMESTAMP_FORMAT = "%Y-%m-%d_%H%M"
ARRIVALTIME_FORMAT_CSV_FILENAME = '%Y%m%d-%H%M'

#----- part 1 -----
IAGOSv3_L2_CAT_PATH = pathlib.Path('/o3p/iagos/catalogues_v3/iagos_L2.parquet')
IAGOSv3_PV_PATH = pathlib.Path('/o3p/iagos/iagosv3/L4/')
IAGOSv3_PV_FILE_PREFIX = 'IAGOS_ECMWF'

# List of all airports in Europe or in America as found from IAGOS v3 catalogue in June 2026 --> <!> might need to be updated if new flights to new airports are added to the database
SOFTIOLI_AIRPORTS = ['AGP, Malaga, Spain',
 'ALG, Algiers, Algeria',
 'ALY, Alexandria, Egypt',
 'AMA, Amarillo, United States',
 'AMM, Amman, Jordan',
 'AMS, Amsterdam, Netherlands',
 'ANC, Anchorage, United States',
 'ANU, Antigua, Antigua and Barbuda',
 'ATH, Athens, Greece',
 'ATL, Atlanta, United States',
 'AUS, Austin, United States',
 'AYT, Antalya, Turkey',
 'BCN, Barcelona, Spain',
 'BER, Berlin, Germany',
 'BEY, Beirut, Lebanon',
 'BGI, Bridgetown, Barbados',
 'BOD, Bordeaux, France',
 'BOG, Bogota, Colombia',
 'BOS, Boston, United States',
 'BRE, Bremen, Germany',
 'BRU, Brussels, Belgium',
 'BTS, Bratislava, Slovakia',
 'BUD, Budapest, Hungary',
 'BWI, Baltimore, United States',
 'BYJ, Beja, Portugal',
 'CAI, Cairo, Egypt',
 'CAY, Cayenne, French Guiana',
 'CCS, Caracas, Venezuela',
 'CDG, Roissy, France',
 'CFE, Clermont-Ferrand, France',
 'CGN, Cologne, Germany',
 'CHR, Châteauroux, France',
 'CIA, Rome, Italy',
 'CLE, Cleveland, United States',
 'CLO, Cali, Colombia',
 'CLT, Charlotte, United States',
 'CMN, Casablanca, Morocco',
 'COS, Colorado Springs, United States',
 'CPH, Copenhagen, Denmark',
 'CRL, Charleroi, Belgium',
 'CUN, Cancun, Mexico',
 'CVG, Cincinnati, United States',
 'DBV, Dubrovnik, Croatia',
 'DEN, Denver, United States',
 'DFW, Dallas, United States',
 'DLM, Dalaman, Turkey',
 'DME, Moscow, Russia',
 'DRS, Dresden, Germany',
 'DTW, Detroit, United States',
 'DUB, Dublin, Ireland',
 'DUS, Dusseldorf, Germany',
 'EIL, Fairbanks, United States',
 'EIN, Eindhoven, Netherlands',
 'ESB, Ankara, Turkey',
 'EWR, Newark, United States',
 'EZE, Buenos Aires, Argentina',
 'FAI, Fairbanks, United States',
 'FAT, Fresno, United States',
 'FCO, Rome, Italy',
 'FKB, Karlsruhe/Baden-Baden, Germany',
 'FLL, Fort Lauderdale, United States',
 'FOR, Fortaleza, Brazil',
 'FRA, Frankfurt, Germany',
 'GHF, Giebelstadt, Germany',
 'GIG, Rio De Janeiro, Brazil',
 'GRU, Sao Paulo, Brazil',
 'GUA, Guatemala City, Guatemala',
 'GVA, Geneva, Switzerland',
 'GYE, Guayaquil, Ecuador',
 'HAJ, Hannover, Germany',
 'HAM, Hamburg, Germany',
 'HAV, Havana, Cuba',
 'HEL, Helsinki, Finland',
 'HER, Heraklion, Greece',
 'HNL, Honolulu, United States',
 'HOG, Holguin, Cuba',
 'HRG, Hurghada, Egypt',
 'IAD, Washington, United States',
 'IAH, Houston, United States',
 'IBZ, Ibiza, Spain',
 'IST, Istanbul, Turkey',
 'JED, Jeddah, Saudi Arabia',
 'JFK, New York, United States',
 'KEF, Keflavik, Iceland',
 'KOA, Kona, United States',
 'KUN, Kaunas, Lithuania',
 'LAS, Las Vegas, United States',
 'LAX, Los Angeles, United States',
 'LBG, Paris, France',
 'LCA, Larnaca, Cyprus',
 'LEI, Almeria, Spain',
 'LEJ, Leipzig, Germany',
 'LGG, Liège, Belgium',
 'LGW, London, United Kingdom',
 'LHR, London, United Kingdom',
 'LIL, Lille, France',
 'LIM, Lima, Peru',
 'LIS, Lisbon, Portugal',
 'LPA, Gran Canaria, Spain',
 'LRM, La Romana, Dominican Republic',
 'LXR, Luxor, Egypt',
 'LYS, Lyon, France',
 'MAD, Madrid, Spain',
 'MAN, Manchester, United Kingdom',
 'MBJ, Montego Bay, Jamaica',
 'MCI, Kansas City, United States',
 'MCO, Orlando, United States',
 'MEX, Mexico City, Mexico',
 'MGA, Managua, Nicaragua',
 'MIA, Miami, United States',
 'MKE, Milwaukee, United States',
 'MLA, Malta, Malta',
 'MRS, Marseille, France',
 'MSP, Minneapolis, United States',
 'MSY, New Orleans, United States',
 'MUC, Munich, Germany',
 'MVD, Montevideo, Uruguay',
 'MXP, Milano, Italy',
 'NCE, Nice, France',
 'NUE, Nuremberg, Germany',
 'OAK, Oakland, United States',
 'OGG, Kahului, United States',
 'ORD, Chicago, United States',
 'ORY, Orly, France',
 'OST, Ostend, Belgium',
 'OTP, Bucharest, Romania',
 'PBI, West Palm Beach, United States',
 'PDX, Portland, United States',
 'PFO, Paphos, Cyprus',
 'PHL, Philadelphia, United States',
 'PHX, Phoenix, United States',
 'PMI, Palma de Mallorca, Spain',
 'PMV, Porlamar, Venezuela',
 'POP, Puerto Plata, Dominican Republic',
 'PPG, Pago Pago, American Samoa',
 'PPT, Papeete, French Polynesia',
 'PRG, Prague, Czech Republic',
 'PSA, Pisa, Italy',
 'PTP, Pointe-a-pitre, Guadeloupe',
 'PTY, Panama City, Panama',
 'PUJ, Punta Cana, Dominican Republic',
 'RDU, Raleigh-durham, United States',
 'REC, Recife, Brazil',
 'RHO, Rhodes, Greece',
 'RIX, Riga, Latvia',
 'RLG, Laage, Germany',
 'RNS, Rennes, France',
 'RSW, Fort Myers, United States',
 'SAL, San Salvador, El Salvador',
 'SAN, San Diego, United States',
 'SCL, Santiago, Chile',
 'SCQ, Santiago, Spain',
 'SDQ, Santo Domingo, Dominican Republic',
 'SEA, Seattle, United States',
 'SFO, San Francisco, United States',
 'SJC, San Jose, United States',
 'SJO, San Jose, Costa Rica',
 'SJU, San Juan, Puerto Rico',
 'SKG, Thessaloniki, Greece',
 'SLC, Salt Lake City, United States',
 'SMF, Sacramento, United States',
 'SPM, Spangdahlem, Germany',
 'SSA, Salvador, Brazil',
 'STL, St. Louis, United States',
 'STN, London, United Kingdom',
 'SXB, Strasbourg, France',
 'SXM, Philipsburg, Netherlands Antilles',
 'SZG, Salzburg, Austria',
 'SZW, Parchim, Germany',
 'TFN, Tenerife, Spain',
 'TFS, Tenerife, Spain',
 'TLS, Toulouse, France',
 'TLV, Tel Aviv, Israel',
 'TOJ, Madrid, Spain',
 'TPA, Tampa, United States',
 'TXL, Berlin, Germany',
 'UIO, Quito, Ecuador',
 'VCE, Venice, Italy',
 'VCP, Campinas, Brazil',
 'VIE, Vienna, Austria',
 'VRA, Varadero, Cuba',
 'YHZ, Halifax, Canada',
 'YMX, Montreal, Canada',
 'YOW, Ottawa, Canada',
 'YQB, Quebec, Canada',
 'YQX, Gander, Canada',
 'YUL, Montreal, Canada',
 'YVR, Vancouver, Canada',
 'YYC, Calgary, Canada',
 'YYR, Goose Bay, Canada',
 "YYT, St. John's, Canada",
 'YYZ, Toronto, Canada',
 'ZRH, Zurich, Switzerland']

Q3_DS_PATH = '/home/patj/SOFT-IO-LI/q3_ds/CO_NOx_q3_ds_NONEreg_2024-07-02_1839.nc' #TODO: suppr if we don't use it
CO_O3_BACKGROUND_DS_PATH = '/home/patj/SOFT-IO-LI/q3_ds/CO_O3_bckg_q3_by_region_month_year_2024-09-20_1730.nc'

FLIGHT_NAME_ATTR = 'flight_name'
PROGRAM_ATTR = 'program'
DEPARTURE_UTC_TIME_ATTR = 'departure_UTC_time'
IAGOS = 'IAGOS'
CARIBIC = 'CARIBIC'
CORE = 'CORE'
MOZAIC ='MOZAIC'

PV_VARNAME = 'PV'
AIRPRESS_VARNAME = 'air_press_AC'
CO_VARNAME = 'CO_P1'
O3_VARNAME = 'O3_P1'
RHL_VARNAME = 'RHL_P1'
NOx_PLUME_ID_VARNAME = 'NOx_plume_id'

CORE_NO_VARNAME = 'NO_P2b'
CORE_NO2_VARNAME = 'NO2_P2b'
CORE_NOx_VARNAME = 'NOx_P2b'

CARIBIC_CO_VARNAME = 'CO_PC2' 
CARIBIC_O3_VARNAME = 'O3_PC2'
CARIBIC_NO_VARNAME = 'NO_PC2'
CARIBIC_NO2_VARNAME = 'NO2_PC2'
CARIBIC_NOx_VARNAME = 'NOx_PC'

NOx_SMOOTHED_VARNAME = 'NOx_smoothed'
NOx_SMOOTHED_TROPO_VARNAME = 'NOx_smoothed_tropo'
NOx_FILTERED_VARNAME = 'NOx_filtered'

CO_SMOOTHED_VARNAME = 'CO_smoothed'
CO_SMOOTHED_TROPO_VARNAME = 'CO_smoothed_tropo'
O3_TROPO_VARNAME = 'O3_tropo'

AIRCRAFT_SPIKE_VARNAME = 'aircraft_spike'

# window used to smooth NOx and CO values, window size = min plume length (100 seconds)
WINDOW_SIZE = {
    f'{IAGOS}-{CORE}': 25, # 25 * 4sec intervals
    f'{IAGOS}-{CARIBIC}': 10 # 10 * 10sec intervals
}
MIN_PLUME_LENGTH = 100 # in seconds, ~= 27.5 km

NOx_MEDIAN = 0.161 # calculated from all L2 IAGOS NOx cruise values in the troposphere to date (03 July 2024)
NOx_Q3 = 0.283 # calculated from all L2 IAGOS NOx cruise values in the troposphere to date (03 July 2024)
CO_Q3 = 110 # calculated from all L2 IAGOS CO cruise values in the troposphere to date (03 July 2024)

FLIGHT_PROGRAM_KEYERROR_MSG = f'flight program NOT supported yet, supported values so far: "{IAGOS}-{CORE}" or "{CORE}" or "{IAGOS}-{CARIBIC}" or "{CARIBIC}"'


#----- part 2 -----
METEO_FIELDS_DIR = '/o3p/wolp/ECMWF/ERA5/050deg_1h_T319_eta1/'

FP_LOUTSTEP = '1h' # flexpart timestep
FP_DURATION = 10 #days
FP_OUTHEIGHT_MIN = 500 #m
FP_OUTHEIGHT_STEP = 500
FP_OUTHEIGHT_MAX = 18000

MIN_NPARTS = 50000

#----- part 3 -----
DEFAULT_LOGDIR = pathlib.Path('/home/patj/logs/softioli/')
SAT_VERSION_ATTRS_NAME = 'satellite_version'

GOES_WEST_SAT_VERSION = ['GOES17', 'G17', 'GOES18', 'G18']
GOES_EAST_SAT_VERSION = ['GOES16', 'G16']

GOES_SATELLITE_GLM = 'GOES_GLM'
GLM_ROOT_DIR = pathlib.Path('/o3p/patj/glm')
REGRID_GLM_DIRNAME = 'regrid_hourly_glm'
PRE_REGRID_GLM_DIRNAME = 'pre_regrid_glm'
REGRID_GLM_ROOT_DIR = pathlib.Path(f'{GLM_ROOT_DIR}/{REGRID_GLM_DIRNAME}')
PRE_REGRID_GLM_ROOT_DIR = pathlib.Path(f'{GLM_ROOT_DIR}/{PRE_REGRID_GLM_DIRNAME}')
GLM_PATH_PREFIX = 'OR_GLM-L2-LCFA'
GLM_Gxx_PATTERN = 'G1[6-8]' # TODO: update if older/newer versions available
MIN_GOES_EAST_WEST_DATE_GLM = Timestamp('2019-10-15T0000')

GOES_SATELLITE_ABI = 'GOES_ABI'
ABI_ROOT_DIR = pathlib.Path('/o3p/patj/ABI')
REGRID_ABI_DIRNAME = 'regrid_hourly_ABI'
PRE_REGRID_ABI_DIRNAME = 'pre_regrid_ABI'
ABI_PATH_PREFIX = 'ABI_GEO_L1B'
ABI_GOESXX_PATTERN = 'GOES1[236-8]'
MIN_GOES_EAST_WEST_DATE_ABI = Timestamp('2019-02-12T0000')

ABI_COORDS_DIRPATH = '/o3p/patj/ABI/coords_files'
GOES_0750_C0_COORDS_FILE_HDF = 'GOES-0750.C0.4km.hdf'
GOES_0750_C1_COORDS_FILE_HDF = 'GOES-0750.C1.4km.hdf'
GOESNG_0750_COORDS_FILE_HDF = 'GOESNG-0750.2km.hdf'
GOESNG_1370_COORDS_FILE_HDF = 'GOESNG-1370.2km.hdf'
GOES_0750_C0_COORDS_FILE = 'GOES-0750.C0.4km.nc'
GOES_0750_C1_COORDS_FILE = 'GOES-0750.C1.4km.nc'
GOESNG_0750_COORDS_FILE = 'GOESNG-0750.2km.nc'
GOESNG_1370_COORDS_FILE = 'GOESNG-1370.2km.nc'

MTG_LI = 'MTG_LI'
MTG_LI_ROOT_DIR = pathlib.Path('/o3p/patj/MTG_LI')
REGRID_MTG_LI_DIRNAME = 'regrid_hourly_MTG_LI'
PRE_REGRID_MTG_LI_DIRNAME = 'pre_regrid_MTG_LI'
MTG_LI_PATH_PREFIX = 'MTG_I1_LI'

NLDN = 'NLDN'
NLDN_PATH_PREFIX = 'NLDN'
NLDN_ROOT_DIR = pathlib.Path('/o3p/patj/nldn')
NLDN_REGRID_DIRNAME = 'regrid_hourly_nldn'

SUPPORTED_LI_SATELLITES_LIST = [GOES_SATELLITE_GLM, MTG_LI]

GRID_RESOLUTION_STR = '05deg'
GRID_RESOLUTION = 0.5
FPOUT_LAT_MIN = -89.75
FPOUT_LAT_MAX = 89.75
FPOUT_LON_MIN = -179.25
FPOUT_LON_MAX = 180.25

YYYY_pattern = "[0-2][0-9][0-9][0-9]" # year
DDD_pattern = "[0-3][0-9][0-9]" # day of year
DD_pattern = "[0-3][0-9]" # day
MM_pattern = "[0-1][0-9]" # month
HH_pattern = "[0-2][0-9]" # hour
mm_pattern = "[0-5][0-9]" # minutes
sss_pattern = "[0-9][0-9][0-9]"

# TODO: update when other satellites OK
SAT_VALUE_ERROR = f'not supported yet. Supported satellites so far: "{GOES_SATELLITE_GLM}", "{GOES_SATELLITE_ABI}" and "{MTG_LI}" for specific dates'


# sat settings dict keys
flash_energy_varname = "flash_energy_varname"
flash_area_varname = "flash_area_varname"
raw_lat_cname = "pre_regrid_lat_coordname"
raw_lon_cname = "pre_regrid_lon_coordname"
attrs_to_keep = "attrs_to_keep"

# hist parameters
## flash energy unit before log operation: J
f_en_J_min_bin = -15 # log
f_en_J_max_bin = -10
f_en_J_hist_step = 0.1
## flash area unit before log operation: km2
f_ar_km2_min_bin = 1.5 # log
f_ar_km2_max_bin = 4.5
f_ar_km2_hist_step = 0.1

# TODO: complete with other satellite data + add dataset_name (mais là pas OK parce que nom fichier 20sec, PAS hourly)
SAT_SETTINGS = {
    GOES_SATELLITE_GLM: {
        flash_energy_varname: "flash_energy",
        flash_area_varname: "flash_area",
        raw_lat_cname: "flash_lat", # latitude coordinate name in pre regrid dataset
        raw_lon_cname: "flash_lon", # longitude coordinate name in pre regrid dataset
        attrs_to_keep: ['production_site', 'orbital_slot', 'platform_ID', 'instrument_type', 'instrument_ID',
                        'spatial_resolution', 'processing_level']
    },
    MTG_LI: {
        flash_energy_varname: "radiance", #TODO: deal with units = mW.m-2.sr-1
        flash_area_varname: "", #"flash_footprint", #TODO: post-processing to convert it into surface (km2)
        raw_lat_cname: "latitude", # latitude coordinate name in pre regrid dataset
        raw_lon_cname: "longitude", # longitude coordinate name in pre regrid dataset
        attrs_to_keep: ['product_id', 'type', 'institution', 'facility_or_tool', 'platform', 'processing_level']
    }
    # <OTHER_SATELLITE>: { ... }
}




