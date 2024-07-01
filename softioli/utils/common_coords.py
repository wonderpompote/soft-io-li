""" GEO_REGIONS: version avant 10 juin 2024 """
GEO_REGIONS = {
    0: {  # Northern North America
        "REGION_NAME": "NNA",
        "LON_MIN": -170,
        "LON_MAX": -25,
        "LAT_MIN": 58,
        "LAT_MAX": 80,
        "COLOR": 'teal'
    },
    1: {  # North America Left
        "REGION_NAME": "NAL",
        "LON_MIN": -128,
        "LON_MAX": -100,
        "LAT_MIN": 30,
        "LAT_MAX": 58,
        "COLOR": 'lime'
    },
    2: {  # North America Right
        "REGION_NAME": "NAR",
        "LON_MIN": -100,
        "LON_MAX": -60,
        "LAT_MIN": 30,
        "LAT_MAX": 58,
        "COLOR": 'red'
    },
    3: {  # Central America
        "REGION_NAME": "CAM",
        "LON_MIN": -115,
        "LON_MAX": -60,
        "LAT_MIN": 10,
        "LAT_MAX": 30,
        "COLOR": 'green'
    },
    4: {  # South America
        "REGION_NAME": "SAM",
        "LON_MIN": -80,
        "LON_MAX": -35,
        "LAT_MIN": -55,
        "LAT_MAX": 10,
        "COLOR": 'yellow'
    },
    5: {  # North Atlantic
        "REGION_NAME": "NAT",
        "LON_MIN": -60,
        "LON_MAX": -18,
        "LAT_MIN": 10,
        "LAT_MAX": 58,
        "COLOR": 'blue'
    },
    6: {  # North Atlantic 2 (petit trou sur la carte je pense)
        "REGION_NAME": "NAT2",
        "LON_MIN": -35,
        "LON_MAX": -18,
        "LAT_MIN": 5,
        "LAT_MAX": 10,
        "COLOR": 'red'
    },
    7: {  # South Atlantic
        "REGION_NAME": "SAT",
        "LON_MIN": -35,
        "LON_MAX": 10,
        "LAT_MIN": -70,
        "LAT_MAX": 5,
        "COLOR": 'magenta'
    },
    8: {  # Europe
        "REGION_NAME": "EU",
        "LON_MIN": -18,
        "LON_MAX": 40,
        "LAT_MIN": 35,
        "LAT_MAX": 70,
        "COLOR": 'orange'
    },
    9: {  # North Africa
        "REGION_NAME": "NAF",
        "LON_MIN": -18,
        "LON_MAX": 40,
        "LAT_MIN": 5,
        "LAT_MAX": 35,
        "COLOR": 'grey'
    },
    10: {  # South Africa
        "REGION_NAME": "SAF",
        "LON_MIN": 10,
        "LON_MAX": 50,
        "LAT_MIN": -35,
        "LAT_MAX": 5,
        "COLOR": 'indigo'
    },
    11: {  # Russia
        "REGION_NAME": "RUS",
        "LON_MIN": 40,
        "LON_MAX": 180,
        "LAT_MIN": 50,
        "LAT_MAX": 75,
        "COLOR": 'darkblue'
    },
    12: {  # Asia Right
        "REGION_NAME": "ASR",
        "LON_MIN": 90,
        "LON_MAX": 140,
        "LAT_MIN": -10,
        "LAT_MAX": 50,
        "COLOR": 'aqua'
    },
    13: {  # Asia Left
        "REGION_NAME": "ASL",
        "LON_MIN": 40,
        "LON_MAX": 90,
        "LAT_MIN": 5,
        "LAT_MAX": 50,
        "COLOR": 'brown'
    },
    14: {  # Australia
        "REGION_NAME": "AUS",
        "LON_MIN": 110,
        "LON_MAX": 180,
        "LAT_MIN": -50,
        "LAT_MAX": -10,
        "COLOR": 'darkgreen'
    },
    15: {  # Pacific
        "REGION_NAME": "PAC",
        "LON_MIN": 140,
        "LON_MAX": 180,
        "LAT_MIN": -10,
        "LAT_MAX": 50,
        "COLOR": 'orangered'
    },
    16: {  # Pacific US
        "REGION_NAME": "PAC_US",
        "LON_MIN": -180,
        "LON_MAX": -128,
        "LAT_MIN": -10,
        "LAT_MAX": 58,
        "COLOR": 'orangered'
    },
}





#TODO: est-ce que j'ai vraiment besoin de tous ces trucs ??
# -----------------------------------------------------------------------

# FP output lower-left corner (voir si je laisse ça là jsp trop)
OUT_LON_LEFT = -179.5
OUT_LAT_LOWER = -90.0
D_X_OUT_LON = 0.5
D_Y_OUT_LAT = 0.5

# Airport coordinates (--> ?? use airport ID instead of city name ?)
AIRPORT_COORDS= {
    "BOG": {
        "CITY_NAME": "BOGOTA", # <!> city names in upper case
        "LON": -74.1461,
        "LAT": 4.7010
    },
    "FRA": {
        "CITY_NAME": "FRANKFURT",
        "LON": 8.5622,
        "LAT": 50.0379
    },
    "CGN": {
        "CITY_NAME": "COLOGNE",
        "LON": 6.9603,
        "LAT": 50.9375
    },
    "SAN": {
        "CITY_NAME": "SAN_DIEGO",
        "LON": -117.1933,
        "LAT": 32.7338
    }  
}

# Satellite coverage
SATELLITES_COVERAGE = {
    "GOES_16": {
        "LON_MIN": -120, # 118 # -120°
        "LON_MAX": -30, # 298 # -30°
        "LAT_MIN": -52, # 75 # -52°
        "LAT_MAX": 52 # 284 # 52°
    },
    "GOES_17": {
        "LON_MIN": -180,
        "LON_MAX": -90,
        "LAT_MIN": -52,
        "LAT_MAX": 52
    }
}

# GEO REGIONS VERSION AVANT MODIF JUIN 2024
# --- Geographical region limits as shown in the article ---
"""
GEO_REGIONS = {
    0: {  # Northern North America
        "REGION_NAME": "NNA",
        "LON_MIN": -170,
        "LON_MAX": -25,
        "LAT_MIN": 60,
        "LAT_MAX": 80
    },
    1: {  # North America Right
        "REGION_NAME": "NAR",
        "LON_MIN": -100,
        "LON_MAX": -60,
        "LAT_MIN": 30,
        "LAT_MAX": 60
    },
    2: {  # North America Left
        "REGION_NAME": "NAL",
        "LON_MIN": -135,
        "LON_MAX": -100,
        "LAT_MIN": 30,
        "LAT_MAX": 60
    },
    3: {  # Central America
        "REGION_NAME": "CAM",
        "LON_MIN": -110,
        "LON_MAX": -60,
        "LAT_MIN": 10,
        "LAT_MAX": 30
    },
    4: {  # South America
        "REGION_NAME": "SAM",
        "LON_MIN": -80,
        "LON_MAX": -35,
        "LAT_MIN": -55,
        "LAT_MAX": 10
    },
    5: {  # North Atlantic
        "REGION_NAME": "NAT",
        "LON_MIN": -60,
        "LON_MAX": -25,
        "LAT_MIN": 10,
        "LAT_MAX": 60
    },
    6: {  # North Atlantic 2 (petit trou sur la carte je pense)
        "REGION_NAME": "NAT2",
        "LON_MIN": -35,
        "LON_MAX": -25,
        "LAT_MIN": 5,
        "LAT_MAX": 10
    },
    7: {  # South Atlantic
        "REGION_NAME": "SAT",
        "LON_MIN": -35,
        "LON_MAX": 10,
        "LAT_MIN": -70,
        "LAT_MAX": 5
    },
    8: {  # Europe
        "REGION_NAME": "EU",
        "LON_MIN": -25,
        "LON_MAX": 40,
        "LAT_MIN": 35,
        "LAT_MAX": 70
    },
    9: {  # North Africa
        "REGION_NAME": "NAF",
        "LON_MIN": -25,
        "LON_MAX": 40,
        "LAT_MIN": 5,
        "LAT_MAX": 35
    },
    10: {  # South Africa
        "REGION_NAME": "SAF",
        "LON_MIN": 10,
        "LON_MAX": 50,
        "LAT_MIN": -35,
        "LAT_MAX": 5
    },
    11: {  # Russia
        "REGION_NAME": "RUS",
        "LON_MIN": 40,
        "LON_MAX": 180,
        "LAT_MIN": 50,
        "LAT_MAX": 75
    },
    12: {  # Asia Right
        "REGION_NAME": "ASR",
        "LON_MIN": 90,
        "LON_MAX": 140,
        "LAT_MIN": -10,
        "LAT_MAX": 50
    },
    13: {  # Asia Left
        "REGION_NAME": "ASL",
        "LON_MIN": 40,
        "LON_MAX": 90,
        "LAT_MIN": 5,
        "LAT_MAX": 50
    },
    14: {  # Australia
        "REGION_NAME": "AUS",
        "LON_MIN": 110,
        "LON_MAX": 180,
        "LAT_MIN": -50,
        "LAT_MAX": -10
    },
    15: {  # Pacific
        "REGION_NAME": "PAC",
        "LON_MIN": 140,
        "LON_MAX": 180,
        "LAT_MIN": -10,
        "LAT_MAX": 50
    },
    16: {  # Pacific US
        "REGION_NAME": "PAC_US",
        "LON_MIN": -180,
        "LON_MAX": -130,
        "LAT_MIN": -10,
        "LAT_MAX": 60
    },
}
"""


"""
# --- North America, Atlantic and Europe dans le code de catherine des fois ---
NA_LON_MIN=-120
NA_LON_MAX=-60
NA_LAT_MIN=30
NA_LAT_MAX=60

AT_long_min=-60
AT_long_max=-25
AT_lat_min=0
AT_lat_max=60

EU_long_min=-25
EU_long_max=40
EU_lat_min=35
EU_lat_max=70
"""