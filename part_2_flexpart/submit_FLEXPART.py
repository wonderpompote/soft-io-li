#!/usr/bin/env python
# coding: utf-8

# In[ ]:


##############################################################################################################################
# 4 avril 2022
# C. Mackay
# Check AVAILABLE, COMMAND, pathnames and RELEASES are in the FLEXPART directory
# Run the executable to submit the batch job


# In[5]:


import os
import subprocess


# In[6]:


### Check paths
flexpart_path = '/o3p/macc/flexpart10.4/flexpart_v10.4_3d7eebf/src/exercises/soft-io-li/'
flight_dir = 'temp2'


# In[7]:


# Change directory to FLEXPART directory for given flight and execute flexpart.sh
# Change back to current working directory

from os.path import exists
import os.path

cwd = os. getcwd()
os.chdir(flexpart_path+flight_dir)

if (os.path.exists(flexpart_path+flight_dir+'/options/RELEASES'))==True & (os.path.exists(flexpart_path+flight_dir+'/AVAILABLE'))==True & (os.path.exists(flexpart_path+flight_dir+'/pathnames'))==True & (os.path.exists(flexpart_path+flight_dir+'/options/COMMAND'))==True :  
    print("Files for running FLEXPART exist")

    output = subprocess.Popen(['sbatch -p o3pwork '+flexpart_path+flight_dir+"/flexpart.sh"],shell=True)
    print("FLEXPART process submitted to o3pwork queue on nuwa")


# In[ ]:


#for datetime
import datetime
from datetime import datetime as dt
#import matplotlib.dates as mdates
#from matplotlib.dates import date2num

