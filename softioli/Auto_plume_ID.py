#!/usr/bin/env python
# coding: utf-8

# In[ ]:


# Auto_plume_ID.ipynb

# This is the first programme of SOFT-IO-LI and it searches the IAGOS database for new NOx (IAGOS CORE) or NO and NO2 (CARIBIC)
# flights to analyse. A check is made to ensure the PV variable is included in the dataset, if not it is added.
# It searches for excess NOx (NO + NO2) above the 75th percentile of the NOx measured during the flight and identifies NOx plumes.
# It selects the plumes if they are measured during the cruise phase of the flight, if there are no stratospheric influences 
# (PV > 2 K m2 kg-1 s-1 and if the CO and O3 are below 100 ppb.
# Short plumes (<12km) are removed as most probably being from aviation emissions.
# The plume information, arrival date and times required for FLEXPART are extracted and saved and plots are made of the remaining plumes
# to enable checks to be easily made.

# C. Mackay September 2022 (Catherine.Mackay@aero.obs-mip.fr)
# https://github.com/ckmackay/SOFT-IO-LI.git

#Suggestions/improvements to be made:

# function find_untested_NOx_flights()
# Once NOx lightning data is added to the IAGOS db we need to include a check so as not to reanalyse any flights.

# function apply_cuts()
# CO and O3 cut at 100 ppb, this could be changed to 75th percentile of CO/O3 in that region, that month. Code to calculate these is in github.
# A quick check did not see any obvious improvment in doing this.


# In[1]:


import numpy as np
import pandas as pd
import xarray as xr
import json

import os

#for plotting
import matplotlib.pyplot as plt

#for datetime
import datetime
from datetime import datetime as dt
import matplotlib.dates as mdates
from matplotlib.dates import date2num


# In[2]:


### Check the following paths and settings: these are set for SOFT-IO-LI default on nuwa in /o3p/iagos/SOFT-IO-LI
### For testing, think of creating /o3p/user/SOFT-IO-LI/ and changing the paths below. 
### you will need to create the following directories:
### /o3p/user/SOFT-IO-LI/FLEXPART_templates
### /o3p/iagos/SOFT-IO-LI/Plume_info_all_cuts/
### /o3p/iagos/SOFT-IO-LI/flight_time_txt/

def set_paths():
    
    global iagos_cat, PV_path, plume_info_path, plume_plots_path, flight_time_path
    
    iagos_cat = '/o3p/iagos/catalogues/'
    PV_path='/o3p/iagos/L4/L2-ECMWF-PV-CO_contributions/'
    
    ### change for your user name!!!
    
    plume_info_path = '/o3p/patj/SOFT-IO-LI/test1/Plume_info_all_cuts/'
    plume_plots_path = '/o3p/patj/SOFT-IO-LI/test1/plots/'
    flight_time_path = '/o3p/patj/SOFT-IO-LI/test1/flight_time_txt/'
    


# In[3]:


# Search for NOx flights
# Using the catalogues search for any flights with NOx measurements
# Make a list of their locations
# Return the number of flights found

### Nb. Once NOx lightning data is added to the db we need to include a check so as not to reanalyse any flights

def find_untested_NOx_flights():
    global flights_with_NOx, df
    #df = pd.read_hdf('/o3p/wolp/catalogues/iagos.h5', key='sources')
    df = pd.read_hdf(iagos_cat+'iagos_L2.h5', key='sources')
    #novars = [c for c in df if c.startswith("data_vars_") and ("NOx") in c] #selects only IAGOS-CORE flights (210 in total)
    novars = [c for c in df if c.startswith("data_vars_") and ("NO2") in c] #selects IAGOS-CORE & IAGOS-CARIBIC flights (669 in total)

    flights_with_NOx = df.loc[(df[novars] > 0).any(axis="columns")]
    #print(novars)
    print(len(flights_with_NOx), 'flights containing NOx measurements')
    
    return


# In[4]:


# Set paths & fetch the NOx flight information

set_paths()
find_untested_NOx_flights()


# In[5]:


#Check the list, optional
#flights_with_NOx


# In[6]:


# Main programme, takes each flight and analyses, saving data necessary for FLEXPART if plumes are identified.

def provide_flight_info(flights_with_NOx):

    global flight_no
    
    flight_no=0
    for i in range(len(flights_with_NOx)):
         
        #set global vairables
        global ds
        global NOx_anom
        
        flights_with_NOx.drivers_load_args.iloc[i]
        ds = xr.load_dataset(*json.loads(flights_with_NOx.drivers_load_args.iloc[i])["args"])
        
        # Only use cruise information
        ds['data_vars_cruise'] = xr.where(ds["air_press_AC"]<30000, True, False)
        ds
        
        
        
        flight_no=flight_no+1
    
    # check if flight is CORE or CARIBIC and then select O3 and CO accordingly
    
        global val_data_NOx
        global data_PV
        if ds.source=='IAGOS-CORE':
            IAGOS_CORE()
        elif ds.source=="IAGOS-CARIBIC": #need to complete this§§§
            IAGOS_CARIBIC()
        else:
            print("Warning - flight source is neither IAGOS-CORE nor IAGOS-CARIBIC - Error!")
            
        global t, lon, lat
        t=ds.UTC_time.where(ds.data_vars_cruise==True)
        lon=ds.lon.where(ds.data_vars_cruise==True)
        lat=ds.lat.where(ds.data_vars_cruise==True)
   
        
        #if 'PV' in list(ds.keys()) and np.all(np.isnan(val_data_NOx)):
        
        if np.all(np.isnan(val_data_NO)):
            #If all NOx values are nan, move to next flight after giving a warning
            print('WARNING: NO all nan values in flight: ',ds.attrs['flight_name'], ', moving to next flight')
            
        elif np.all(np.isnan(val_data_NO2)):
            #If all NOx values are nan, move to next flight after giving a warning
            print('WARNING: NO2 all nan values in flight: ',ds.attrs['flight_name'], ', moving to next flight')
        
        elif np.all(np.isnan(val_data_NOx)):
            #If all NOx values are nan, move to next flight after giving a warning
            print('WARNING: NOx all nan values in flight: ',ds.attrs['flight_name'], ', moving to next flight')
            
            
        #Check if the txt file already exisits...if so print warning and move to next flight
        #elif 'PV' in list(ds.keys()) and os.path.exists('Plume_info_'+ds.attrs['flight_name']+'.txt'):
        
        elif os.path.exists(plume_info_path+'Plume_info_'+ds.attrs['flight_name']+'.txt'):
            print('*******************************************')
            print('WARNING: Plume_info_',ds.attrs['flight_name'], ', already exisits, moving to next flight')
            print('*******************************************')
            
        #Otherwise analyse the flight data
        #elif 'PV' in list(ds.keys()):
        else:    
            print('ANALYSING flight: ',ds.attrs['flight_name'])
            print('*******************************************')
            
            print("***Applying cuts***")
            apply_cuts()
            
            print("***Calculating moving averages***")
            calculate_moving_average(NOx_anom)
            
            print("***Finding plumes***")
            find_plumes(NOx_anom)
            
            print("***Removing planes***")
            remove_plane()
            
            
            if len(list1)==len(list2):
                print(len(list1), "plume(s) identified")
                #print("list1&2",list1, list2)
                #Now obtain the info on each plume and write this to the approproate txt file in ../Plume_info
                #also check for num_list
                
            
            
                #if len(list1)>0:
                if len(plume_start)>0:
                    print("***Plot plumes***")
                    plot_plumes()
                    print("***Extract datetime info***")
                    extract_datetime()
                    print("***Writing plume info***")
                    write_plume_info()
                else:
                    print("No plumes identified, end of anaylsis for this flight")
          
                
                
                
                
            else:
                print("No plumes identified, end of anaylsis for this flight")
          
        #else:
            #print('WARNING: No PV data available in flight: ',ds.attrs['flight_name'], ', moving to next flight')
            
    return(ds)

    


# In[7]:


# Obtain IAGOS CORE flight variables, check if PV is included in the datasets, if not add it from L4 data

def IAGOS_CORE():
            
    print('*******************************************')
    print('IAGOS-CORE', flight_no)
    print('*******************************************')
    
    global val_data_NO, val_data_NO2, val_data_NOx
    global data_PV, val_data_CO, val_data_O3
    
    # Only select CO data where data quality flag is good (=0)
    ds['data_status_CO'] = xr.where(ds["CO_P1_val"]==0, True, False)
    # Only select O3 data where data quality flag is good (=0)
    ds['data_status_O3'] = xr.where(ds["O3_P1_val"]==0, True, False)
    # Only select NOx data where data quality flag is good (=0)
    ds['data_status_NO'] = xr.where(ds["NO_P2b_val"]==0, True, False)
    ds['data_status_NO2'] = xr.where(ds["NO2_P2b_val"]==0, True, False)
        
    ### If 2018 onwards
    ds['data_status_NOx'] = xr.where(ds["NOx_P2b_val"]==0, True, False)
    # NOx_P2b_val==7 points where no data was collected so may have been an excess
    ds['data_status_NOx_null'] = xr.where(ds["NOx_P2b_val"]==7, True, False) 
        
    data_O3 = ds.O3_P1.where(ds.data_vars_cruise==True)
    val_data_O3 = data_O3.where(ds.data_status_O3==True)
    #mean_O3 = np.mean(data_O3)
    #print(mean_O3)
    data_CO = ds.CO_P1.where(ds.data_vars_cruise==True)
    val_data_CO = data_CO.where(ds.data_status_CO==True)
            
    #check if PV is included in the db (it isn't for lots of flights in 2005)using check_PV
    #if not check_PV calls function add_PV
    #Probably not needed for CORE but if new entries in DB this might be missing initially
            
    check_PV()
            
    data_PV = ds.PV.where(ds.data_vars_cruise==True)
    data_PV = 100*data_PV #100*PV used so it can be plotted on the same plot
    data_NO = ds.NO_P2b.where(ds.data_vars_cruise==True)
    val_data_NO =  data_NO.where(ds.data_status_NO==True)
    data_NO2 = ds.NO2_P2b.where(ds.data_vars_cruise==True)
    val_data_NO2 = data_NO2.where(ds.data_status_NO2==True)
    data_NOx = ds.NOx_P2b.where(ds.data_vars_cruise==True)
    val_data_NOx = data_NOx.where(ds.data_status_NOx==True)
    no_val_data_NOx = data_NOx.where(ds.data_status_NOx_null==True)
    mean_NOx = np.nanmean(data_NOx)
            
    # cloud info
    data_cloud = ds.cloud_P1.where(ds.data_vars_cruise==True) # add cloud data
    data_cl = ds.cloud_presence_P1.where(ds.data_vars_cruise==True) # add cloud data
        


# In[8]:


# Obtain IAGOS Caribic variables, check if PV is included in the datasets, if not add it from L4 data

def IAGOS_CARIBIC():
    
    print('*******************************************')
    print('IAGOS-CARIBIC', flight_no)
    print('*******************************************')
     
    global val_data_NO, val_data_NO2, val_data_NOx
    global data_PV, val_data_CO, val_data_O3
    
    data_O3 = ds.O3_PC2.where(ds.data_vars_cruise==True)
    data_CO = ds.CO_PC2.where(ds.data_vars_cruise==True)
    data_NO = ds.NO_PC2.where(ds.data_vars_cruise==True)
    data_NO2 = ds.NO2_PC2.where(ds.data_vars_cruise==True)
    data_NOy = ds.NOy_PC2.where(ds.data_vars_cruise==True)
    #data_NOx = data_NO + data_NO2
            
    # Only select CO data where data quality flag is good (=0)
    ds['data_status_CO'] = xr.where(ds["CO_PC2_val"]==0, True, False)
    # Only select O3 data where data quality flag is good (=0)
    ds['data_status_O3'] = xr.where(ds["O3_PC2_val"]==0, True, False)
    # Only select NOx data where data quality flag is good (=0)
    ds['data_status_NO'] = xr.where(ds["NO_PC2_val"]==0, True, False)
    ds['data_status_NO2'] = xr.where(ds["NO2_PC2_val"]==0, True, False)
            
    data_O3 = ds.O3_PC2.where(ds.data_vars_cruise==True)
    val_data_O3 = data_O3.where(ds.data_status_O3==True)
    #mean_O3 = np.mean(data_O3)
    #print(mean_O3)
    data_CO = ds.CO_PC2.where(ds.data_vars_cruise==True)
    val_data_CO = data_CO.where(ds.data_status_CO==True)
            
    #check if PV is included in the db (it isn't for lots of flights in 2005)using check_PV
    #if not check_PV calls function add_PV
            
    check_PV()
            
    #Some L2 files don't include PV either, so run a second check to see if they've been added, if not break
            
    if 'PV' in list(ds.keys()):
                
        data_PV = ds.PV.where(ds.data_vars_cruise==True)
        data_PV = 100*data_PV #100*PV used so it can be plotted on the same plot
        data_NO = ds.NO_PC2.where(ds.data_vars_cruise==True)
        val_data_NO =  data_NO.where(ds.data_status_NO==True)
        data_NO2 = ds.NO2_PC2.where(ds.data_vars_cruise==True)
        val_data_NO2 = data_NO2.where(ds.data_status_NO2==True)
        #data_NOx = ds.NOx_PC2.where(ds.data_vars_cruise==True)
        #val_data_NOx = data_NOx.where(ds.data_status_NOx==True)
                
        if np.all(np.isnan(val_data_NO)):
            print("***No NO recorded in this flight")
        elif np.all(np.isnan(val_data_NO2)):
            print("***No NO2 recorded in this flight")
        else:
            data_NOx = data_NO+data_NO2
            val_data_NOx = val_data_NO+val_data_NO2
                    
            #no_val_data_NOx = data_NOx.where(ds.data_status_NOx_null==True)
            mean_NO = np.nanmean(val_data_NO)
            mean_NO2 = np.nanmean(val_data_NO2)
            mean_NOx = np.nanmean(val_data_NOx)
            
            print("***mean_NO = ", mean_NO, "***")
            print("***mean_NO2 = ", mean_NO2, "***")
            print("***mean_NOx = ", mean_NOx, "***")
            
        # cloud info not available for CARIBIC flights
        
        #data_cloud = ds.cloud_P1.where(ds.data_vars_cruise==True) # add cloud data
        #data_cl = ds.cloud_presence_P1.where(ds.data_vars_cruise==True) # add cloud data
        #data_temp = ds.air_temp_AC.where(ds.data_vars_cruise==True) # add cloud data
    else:
        pass


# In[9]:


# Function to check if the PV is saved in the DB. Often this is not the case for CARIBIC flights
# If it's missing call function add_PV to add the PV to the dataset ds

def check_PV():
    if 'PV' in list(ds.keys()):
        pass
    else:
        add_PV()


# In[10]:


# Function to add PV to dataset if it's missing. PV is saved in /o3p/iagos/iagosv2/netcdf/L2/XXXXYY 
# where XXXX = year and YY month
# Code extracts correct year and month to find the correct L2 or L4 information.
# Once it's found the correct flight data file it compares the length of the flight to ensure it's the correct one.
# Then it adds the PV data to the dataset.
# Otherwise it prints and error message.

def add_PV():
    #find correct directory for flight
    start_datetime=(ds.departure_UTC_time)
    date_time = start_datetime.split("T")
    date = date_time[0]
    #print(start_datetime)
    #print(date)
    date_split = date.split("-")
    yr=date_split[0]
    #print(yr)
    mnth=date_split[1]
    #print(mnth)
    
    #find correct file for flight
    file_name = 'IAGOS_'+ds.flight_name+'_L4.nc'
    
    dr = xr.open_dataset(PV_path+yr+mnth+'/'+file_name)
    
    #Add PV to dataset ds
    
    #print(ds.UTC_time)
    time = ds.UTC_time
    last_time = (len(ds.UTC_time)-1)
    #print(last_time)
    #print("final time =", time[0])
    #print("final time =", time[int(last_time)])
    
    time_PV = dr.UTC_time
    last_time_PV = (len(dr.UTC_time)-1)
    
    #print(time_PV, last_time_PV)
    
    #check time lengths agree, if so add PV to dataset ds
    # ds is L1, dr = L4
    
    if len(ds.UTC_time)==len(dr.UTC_time):
        #print("No problems with time lengths")
        ds["PV"]=(['UTC_time'],  dr.PV)
    else:
        print("ERROR: PROBLEM WITH TIME LENGTHS!!!")
        #print(len(ds.UTC_time))
        #print(len(UTC_time_secs))
    
    
    if 'PV' in list(dr.keys()):
        #print("found PV")
        pass
    else:
        print("PV missing in L4 dataset")
        pass


# In[11]:


# Apply cuts: applies the following cuts on the flight data.
# NOx 75th percentile
# PV > 2 K m2 kg-1 s-1
# CO > 100 ppb
# O3 > 100 ppb
# returns the data that passes this data selection.

def apply_cuts():
    global NOx_anom
    global cl75
    # Find mean, 75th percentile and 95th percentile of good quality NOx data in cruise

        #data = data_NOx.where(ds.data_status_NOx==True)
        #clim = data.mean()

    clim = val_data_NOx.mean()
        #print(len(val_data_NOx))
        #print('1000 = ', val_data_NOx[100])
    #print('clim = ',clim)
        
            #find 75 percentile
    cl75 = np.nanpercentile(val_data_NOx, 75)
            #print('75th percentile', cl75)
        #find 95 percentile
    cl95 = np.nanpercentile(val_data_NOx, 95)
            #print('95th percentile', cl95)
            
    #print("Mean NOx", clim.values)
    print("Mean NOx", clim)
    
    print("75th percentile NOx", cl75)
    #print("95th percentile NOx", cl95)

    NOx_anom = val_data_NOx - cl75
            
            #ds['NOx_anom'] = val_data_NOx - cl75
            
    NOx_anom = NOx_anom.where(NOx_anom>=0) #Only keep positive values
            
    NOx_anom = NOx_anom.where(data_PV<200) #this value is fixed (NB PV is multiplied by 100, so actually cutting at 2!!!)
    
    NOx_anom = NOx_anom.where(val_data_O3<110) #this value is fixed
    NOx_anom = NOx_anom.where(val_data_CO<110) #this value is fixed
    


# In[12]:


# Write plume info to txt file
# This txt files contains the information that FLEXPART_auto.ipynb requires to produce the FLEXPART control files.

def write_plume_info():
    num_list=[]
    for i in range (len(plume_end)):
        num_list.append("plume_"+str(i+1))
    #rint(num_list)
    
    plume_info=[[len(plume_end)]]
    temp=[[]] * len(num_list)
    
    for i in range(len(num_list)):
        ts = pd.to_datetime(str(ds.UTC_time[plume_start[i]].values))
        date_start = ts.strftime('%Y%m%d')
        time_start = ts.strftime('%H%M%S')
        te = pd.to_datetime(str(ds.UTC_time[plume_end[i]].values))
        date_end = te.strftime('%Y%m%d')
        time_end = te.strftime('%H%M%S')
        #print(i, plume_start[i], plume_end[i], date_start, date_end, time_start, time_end,
        ds.lon[plume_start[i]].values, ds.lon[plume_end[i]].values 
        ds.lat[plume_start[i]].values, ds.lat[plume_end[i]].values
        ds.air_press_AC[plume_start[i]].values, ds.air_press_AC[plume_end[i]].values
        ds.baro_alt_AC[plume_start[i]].values, ds.baro_alt_AC[plume_end[i]].values
        
        temp[i]= plume_start[i], plume_end[i], int(date_start), int(date_end), int(time_start), int(time_end), float(ds.lon[plume_start[i]].values), float(ds.lon[plume_end[i]].values), float(ds.lat[plume_start[i]].values), float(ds.lat[plume_end[i]].values), float(ds.air_press_AC[plume_start[i]].values), float(ds.air_press_AC[plume_end[i]].values),float(ds.baro_alt_AC[plume_start[i]].values), float(ds.baro_alt_AC[plume_end[i]].values)
        
    np.savetxt(plume_info_path+'Plume_info_'+ds.attrs['flight_name']+'.txt',temp, fmt=' '.join(['%i']*4 + ['%06d']*2 + ['%1.4f']*8))
    


# In[13]:


# Finds new_start for each plume

def new_starts(new_start, NOx_anom):
    plume_start_id = []
    #print(new_start)
    count = new_start
    eof = len(NOx_anom)
    #for i in range(new_start,eof):
    for i in range(count,eof):
    
        count += 1
        if NOx_anom[i]>0:
            #plume_start_id = [count-1]
            plume_start_id.append(count-1)
            plume_start_lon = [lon[i].values]
            plume_start_NOx = [NOx_anom[i].values]
            break
    #print("plume_start_id =", plume_start_id)
    return(plume_start_id)   


# In[14]:


# Finds new_end for each plume - using the moving average caluculated by mov_ave_NOx_anom 

def new_end(new_end, total_NOx):
    #global total_NOx
    #print(new_end)
    count = new_end 
    count_b=0
    for i in range(new_end,len(mov_ave_NOx_anom)):
        count +=1
        plume_end_id = [count-51]
        plume_end_lon = [lon[i-51].values]
        plume_end_NOx = [mov_ave_NOx_anom[i-50]]
        old_total = total_NOx
        total_NOx = total_NOx + np.nansum(mov_ave_NOx_anom[i])
        if old_total == total_NOx:
            count_b +=1
        else: count_b =0
        if count_b ==50: #60 if unchanged for 4 mins assume end of plume....OK for first plume
            break
            
    return(plume_end_id, total_NOx)


# In[15]:


# Finds the plumes that meet the criteria

def find_plumes(NOx_anom):
    global start
    global plume_end_id
    global plume_start_id
    global list1, list2
    # Start search from 0
    n_start = 0
    counter = [0, 1]
    list1 = []
    list2 = []
    
    while counter[-1] > counter[-2]:
        #print(nstart)
        counts = counter[-1]
        #print("counts", counts)
        #print("nstart= ", n_start, "start= ", start)
        #list1.append(n_start)
    
        plume_start_id = new_starts(n_start, NOx_anom)
        #print("Value of start of first plume = ", plume_start_id)
        #got start of first plume
        #take new start from last value in plume_end_id
        plume_start_id_len = len(plume_start_id)-1
        #print(plume_start_id_len)
        
        if len(plume_start_id)==0:
            
            counter = [0, 0]
            
            
        else:
            for index, x in enumerate(plume_start_id):
                
                if index == plume_start_id_len:
            
                    start = plume_start_id[index]
                #print("Start value = ", start)
                    list1.append(start)
            plume_end_id = new_end(start, 0)[0]
        #print("Check", plume_end_id)
            total_NOx = new_end(start, 0)[1]
        #print("Value of end of first plume = ", plume_end_id)
            plume_end_id_len = len(plume_end_id)-1
        #print("plume_end_id", plume_end_id)
            for index, x in enumerate(plume_end_id):
                if index == plume_end_id_len:
                    n_start = plume_end_id[index]
        #print("what goes in to list2", n_start)
            list2.append(n_start)
            if len(list2)>=2:
                if list2[-1]==list2[-2]:
                    del list2[-1]
                    break
            n_start = n_start+1 
    
        #print("New starting point", n_start) 
        #print("nstart= ", n_start, "start= ", start)
            if n_start>start:
                counts+=1
            #print(counts)
                counter.append(counts)
            #print(counter)
        #print('Total NOx measured', total_NOx) 
        #print('list1 =', list1)
        #print('list2 =', list2)
        


# In[16]:


# Removes small plumes, usually high NOx values, that are associated with aviation emissions

def remove_plane():
    global plume_start, plume_end
    plume_start=[]
    plume_end=[]
    for i in range (len(list1)):
        #print("Diff = ", i, (list2[i])-(list1[i]))
        if (list2[i])-(list1[i])>= 15: #removes plumes of less than 12-15km extent as most likely to be related to aircraft emissions
            #keep pair
            plume_start.append(list1[i])
            plume_end.append(list2[i])
        
                        #del(list2[i])
                        #del(list1[i])
    
                #print(rem)       
    #del list2
    #del list1
    #print("plume st in remove plane=", plume_start)
    #print("plume en in remove plane=", plume_end)
        #else:
         #   counter[-1] == counter[-2]
            #pass


# In[17]:


# Calculate the moving average of NOx_anom

def calculate_moving_average(NOx_anom):
    global mov_ave_NOx_anom

    #Calculate moving average
    window_size = 50
    i = 0
    count = []
    mov_ave_NOx_anom = []
    while i < len(NOx_anom) - window_size + 1:
        #if NOx_anom[i]>=0:
        this_window = NOx_anom[i : i + window_size]
        #window_average = sum((this_window) / window_size, skipna=True)
        window_average = np.nansum(this_window) / window_size
        mov_ave_NOx_anom.append(window_average)
        #print(window_average)
        count.append(i)
        #print(i)
        i += 1
        j = 0    
        while j < window_size -1:
        #    #print(j)
        #    mov_ave_NOx_anom.append(None)
            j+=1

    #print(mov_ave_NOx_anom)


# In[18]:


# Plot the plume information for checking

def plot_plumes():
    params = {'legend.fontsize': 'x-large',
              'figure.figsize': (20, 5),
             'axes.labelsize': 'x-large',
             'axes.titlesize':'x-large',
             'xtick.labelsize':'x-large',
             'ytick.labelsize':'x-large'}
    plt.rcParams.update(params)
    
    lon_min = np.nanmin(lon)
    lon_min = lon_min-5
    print("lon_min",lon_min)
    lon_max = np.nanmax(lon)
    lon_max=lon_max+5
    print("lon_max",lon_max)
    fig, ax1 = plt.subplots()
    ax1.set_xlim([lon_min, lon_max])
    color = 'tab:red'
    ax1.set_ylabel('O3 & CO (ppb)')
    ax1.plot(lon, val_data_O3, color=color, label='O3')
    ax1.plot(lon, val_data_CO, color='tab:orange', label='CO')
    ax1.plot(lon, ds.PV*100, color='tab:grey', label='PV*100')
    ax1.tick_params(axis='y', labelcolor=color)
    

    ax1.set_ylim([0, 200]) #Only plot troposphere
    plt.axhline(y=100, linestyle='--', color='tab:orange') #assuming CO background of 100 ppbv

    ax1.set_xlabel('Longitude (°)')
    ax2 = ax1.twinx()  # instantiate a second axes that shares the same x-axis

    color = 'tab:blue'
    ax2.set_ylabel('NO, NO2 & NOx (ppb)')  # we already handled the x-label with ax1
    ax2.plot(lon, val_data_NO, color=color, label='NO')
    ax2.plot(lon, val_data_NO2, color='tab:green', label='NO2')
#if ds.source=='IAGOS-CORE':
    ax2.plot(lon, val_data_NOx, color= 'tab:purple', label='NOx')
    ax2.plot(lon, NOx_anom, color= 'tab:pink', label='NOx plumes')
    plt.axhline(y=cl75, linestyle='--', color='tab:orange')
  
    fig, ax1 = plt.subplots()
    ax1.set_xlim([lon_min, lon_max])
    ax1.set_xlabel('Longitude (°)')
    ax1.set_ylabel('NOx plumes (ppb)')
    ax2 = ax1.twinx()  # instantiate a second axes that shares the same x-axis

    color = 'tab:blue'
    ax2.set_ylabel('Not used')  # we already handled the x-label with ax1
    
    #ax1.set_xlim([lon_min, lon_max])
    
    ax1.plot(lon, NOx_anom, color= 'tab:pink', label='NOx plumes')
    
    for i in range(len(plume_start)):
        plt.axvline(x=plume_start[i])
        plt.axvline(x=plume_end[i])
    
    plt.savefig(plume_plots_path+ds.attrs['flight_name']+'.png')
    plt.show()
    plt.clf()


# In[19]:


# Extract date and time information of flight arrival for FLEXPART AVAILABLE file and save to flight_time_txt/ directory.

def extract_datetime():
    
    date = ds.arrival_UTC_time.split('T')[0]
    time = ds.arrival_UTC_time.split('T')[1]

    #print(date)
    #print(time)
    
    year = date.split('-')[0]
    month = date.split('-')[1]
    day = date.split('-')[2]
    hour = time.split(':')[0]
    #print("year = ", year, ", month = ",month, ", day = ",day, "hour = ", hour)
    
    dt=(str(year), str(month), str(day), str(hour))

    trial=[dt]
    #print('dt=', trial)
    np.savetxt(flight_time_path+'Plume_info_'+ds.attrs['flight_name']+'.txt',trial,fmt=' '.join(['%s']))


# In[ ]:


# Run the provide_flight_info to find and isolate plumes

provide_flight_info(flights_with_NOx)


# In[ ]:





# In[ ]:




