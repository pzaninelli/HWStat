#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon May 23 10:55:18 2022

@author: pzaninelli
"""

import xarray as xr
import numpy  as np
import pandas as pd
import glob
import os
import time
import datetime


start = time.time()


################## Define parameters ##################

################## Part 1: Compute daily T2max and T2min from subdaily temperatures ##################
################## Option_T2 determines if the identification of HWs is based on the 
################## daily maximum temperature (T2max), minumum temperature (T2min) or both (T2maxmin)
option_T2                = 'T2max' # options: T2max, T2min, T2maxmin
option_ocean             = 1       # if 1 ocean grid cells are converted to NaN 
################## Define the paths where the T2 data (path_in) and the land sea mask (path_in_mask) are located
path_in = '/home/pzaninelli/TRABAJO/IGEO/data/t2m/'
if option_ocean == 1:
    path_in_mask = '/home/pzaninelli/TRABAJO/IGEO/data/mask/'
################## Define and/or create the path where we want to save the outputs (path_out)
path_out     = '/home/pzaninelli/TRABAJO/IGEO/data/' + option_T2 + '/'
os.system ('mkdir -p ' + path_out)   

################## Part 2: Identify the days considered as HW ##################
################## Define the reference period (baseline) and the width of the time interval considered 
################## to compute the climatological percentile threshold for each calendar day 
################## (e.g. semiwindow = 7 considers 15 days (7 before, 7 after the calendar day))
start_climatology        = 1981
end_climatology          = 2010
semiwindow               = 7
################## A HW must fultill two conditions:
################## 1) T2 > percentile_threshold
################## 2) duration criterion: a sequence of at least "persistence_HW" consecutive days fulfilling condition 1 
percentile_threshold        = 90
persistence_HW              = 3
################## Optional choice to save intermediate arrays (T2max.nc, hw_exceedance.nc) needed to compute the HW metrics of part 3
option_save_temporary_files = 1 # if 1 save the intermediate files (otherwise, only the array with the HW metrics is saved)

################## Part 3: Compute metrics for all HW days in the considered interval ##################
################## The metrics are: frequency (HWF), duration of the longest spell (HWD), daily peak exceedance (HWA) and cumulative exceedance (HWM)
################## The intervals are: yearly, seasonal, monthly and running windows of the specified temporal witdth (the latter provides daily values)
option_month_season      = 1 # if 1 metrics are computed at monthly and seasonal time scales
option_rolling           = 1 # if 1 metrics are computed on a daily basis using windows of window_width days
window_width             = 30

################## Define parameters ##################

################## Part 1: Compute daily T2max and T2min from subdaily temperatures ##################
################## Read the data (hourly 2m temperature) from the netcdfs located in the folder "path_in"
list_raw_data  = sorted(glob.glob(path_in+'*.nc'))   
raw_data_array = xr.open_mfdataset(list_raw_data)
start_year = int(np.min(raw_data_array["time.year"].values))
end_year   = int(np.max(raw_data_array["time.year"].values))

if option_T2 == 'T2max' and os.path.isfile(path_out+'T2max_'+str(start_year)+'_'+str(end_year)+'.nc'): #In case it already exists, this step is avoided to save time
    pass
elif option_T2 == 'T2min' and os.path.isfile(path_out+'T2min_'+str(start_year)+'_'+str(end_year)+'.nc'): #In case it already exists, this step is avoided to save time
    pass
elif option_T2 == 'T2maxmin' and os.path.isfile(path_out+'T2max_'+str(start_year)+'_'+str(end_year)+'.nc') and os.path.isfile(path_out+'T2min_'+str(start_year)+'_'+str(end_year)+'.nc'): #In case it already exists, this step is avoided to save time
    pass
else:
    ################## Mask ocean
    if option_ocean == 1:
        list_raw_data_mask  = sorted(glob.glob(path_in_mask+'*.nc'))   
        raw_data_array_mask = xr.open_mfdataset(list_raw_data_mask)['lsm']
        raw_data_array      = raw_data_array.where(raw_data_array_mask[0] > 0.5, np.nan)
        del raw_data_array_mask
    ################## Change longitude coordinates from 360 to +-180
    raw_data_array = raw_data_array.assign_coords(longitude = (((raw_data_array.longitude + 180) % 360) - 180))
    raw_data_array = raw_data_array.sortby(raw_data_array.longitude)
    ################## Change temperature from K to Celsius degrees
    raw_data_array = raw_data_array - 273.15
    #raw_data_array.t2m.attrs['units'] = 'Celsius degrees'
    ################## Compute daily T2max and T2min
    if option_T2 in ['T2max','T2maxmin']:
        T2max = raw_data_array.resample(time='1D').max()
    if option_T2 in ['T2min','T2maxmin']:
        T2min = raw_data_array.resample(time='1D').min()
    del raw_data_array
    ################## Save to netcdf 
    # This is needed to speed up the computation time in the followings steps 
    # (see https://groups.google.com/g/xarray/c/11lDGSeza78?pli=1 for more details)
    if option_T2 in ['T2max','T2maxmin']:
        T2max.to_netcdf(path=path_out+'T2max_'+str(start_year)+'_'+str(end_year)+'.nc')
        del T2max
    if option_T2 in ['T2min','T2maxmin']:
        T2min.to_netcdf(path=path_out+'T2min_'+str(start_year)+'_'+str(end_year)+'.nc')
        del T2min
################## Part 1: Compute daily T2max and T2min from subdaily temperatures ##################