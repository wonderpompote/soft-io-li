"""
Script partie 3
1- recup fp output (sur 7 jours)
2- recup lightning data for the 7-day period 
3- calcul ratio
4- recup infos à mettre dans bdd iagos
"""


from utils import constants as cts
from utils import glm_utils

if __name__ == '__main__':
    print('test imports:')
    print(cts.DDD_pattern)
    print(type(glm_utils.get_glm_daily_dir_date))