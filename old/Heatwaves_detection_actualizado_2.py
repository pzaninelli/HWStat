#!/usr/bin/env python
# coding: utf-8

# In[ ]:


################## Description ##################
#Input:
#1) Sub-daily (e.g. hourly, 6-hourly) data of Temperature and the path where they are (path_in)
#2) (optional) Land sea mask and the path (path_in_mask)

#Output:
#1) Daily data of Tmax and Tmin (saved as T2max.nc and T2min.nc)
#2) (optional) An array where 0 indicates no heatwave (HW) and positive values indicate the exceedance of the HW (hw_exceedance.nc)
#3) (optional) An array where 0 indicates no HW and positive values indicate the length of the HW spell
#   (all days of the same HW have the same value) (hw_persistence.nc)
#4) HW metrics aggregated over the specified time interval
#   Four metrics: frequency (HWF), longest spell (HWD), daily peak exceedance (HWA) and cumulative exceedance (HWM)
#   Four time intervals: yearly, seasonal, monthly and running windows of the specified temporal witdth (in days)
#   Seasons are defined as DJF (winter, MAM (spring), JJA (summer) and SON (autumn)


# In[ ]:


import xarray            as xr
import numpy             as np
import pandas            as pd
import glob
import os
import time
import datetime


# In[ ]:


start = time.time()


# In[ ]:


################## Define parameters ##################

################## Part 1: Compute daily T2max and T2min from subdaily temperatures ##################
################## Option_T2 determines if the identification of HWs is based on the 
################## daily maximum temperature (T2max), minumum temperature (T2min) or both (T2maxmin)
option_T2                = 'T2maxmin' # options: T2max, T2min, T2maxmin
option_ocean             = 1       # if 1 ocean grid cells are converted to NaN 
################## Define the paths where the T2 data (path_in) and the land sea mask (path_in_mask) are located
path_in      = '/home/jose/Escritorio/Nuevo_vol/Datos/ERA5/hourly_global_25/raw/T2/'
if option_ocean == 1:
    path_in_mask = '/home/jose/Escritorio/Nuevo_vol/Datos/ERA5/hourly_global_25/raw/land_sea_mask/'
################## Define and/or create the path where we want to save the outputs (path_out)
path_out     = '/home/jose/Escritorio/Nuevo_vol/Output/CLINT/Detection_Heatwave/' + option_T2 + '/'
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


# In[ ]:


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


# In[ ]:


################## Part 2: Identify the days considered as HW ##################
print('Parte 2')
if os.path.isfile(path_out+'hw_exceedance.nc'): #In case it is already computed, we avoid this step to save time
    pass
else:
    ################## Load again the T2 data from the netcdf created before
    if option_T2 == 'T2max':
        T2work = xr.open_mfdataset(glob.glob(path_out+'T2max*.nc')[0])
    elif option_T2 == 'T2min':
        T2work = xr.open_mfdataset(glob.glob(path_out+'T2min*.nc')[0])
    elif option_T2 == 'T2maxmin':
        T2work  = xr.open_mfdataset(glob.glob(path_out+'T2max*.nc')[0])
        T2work2 = xr.open_mfdataset(glob.glob(path_out+'T2min*.nc')[0])
    ################## "array_above_percentile" identify the days that fulfills condition 1
    ################## "array_hw_days" identify the days that fulfills condition 1 and 2 simultaneously
    ################## "array_hw_exceedance" gives 0 for non HW days and the exceedance (defined as the differeance
    ################## between T2 and the percentile threshold of climatological T2) for HW days
    ################## "array_persistence" gives the length of each HW spell (i.e. if a HW lasts 5 days, these days are labelled as 5)
    array_above_percentile       = np.full(tuple(T2work.dims[d] for d in ['time', 'longitude', 'latitude']),False)
    array_hw_days                = np.full(np.shape(array_above_percentile),False)
    array_hw_exceedance          = np.full(np.shape(array_above_percentile),0.)
    array_persistence            = np.full(np.shape(array_above_percentile),0)
    max_dayofyear                = np.max(T2work["time.dayofyear"].values) #usually 366 depending on the calendar used
    array_climatology_percentile = np.full([max_dayofyear,np.shape(array_above_percentile)[1],np.shape(array_above_percentile)[2]],np.nan)
    if option_T2 == 'T2maxmin':
        array_climatology_percentile_maxmin = np.full([max_dayofyear,np.shape(array_above_percentile)[1],np.shape(array_above_percentile)[2]],np.nan)
    for xxx in range(0,np.shape(array_above_percentile)[1]):
        for yyy in range(0,np.shape(array_above_percentile)[2]):
            print(str(xxx) + '/' + str(np.shape(array_above_percentile)[1]) + ' - ' + str(yyy) + '/' + str(np.shape(array_above_percentile)[2]))
            if xxx == 0 and yyy == 0:
                serie_dayofyear = T2work["time.dayofyear"].values
                serie_year      = T2work["time.year"].values
                list_longitude  = T2work["longitude"].values
                list_latitude   = T2work["latitude"].values
            serie_work = T2work.sel(longitude = list_longitude[xxx], latitude = list_latitude[yyy])['t2m'].values 
            if option_ocean == 1 and np.isnan(serie_work[0]) == True:
                continue
            if option_T2 == 'T2maxmin':
                serie_work2 = T2work2.sel(longitude = list_longitude[xxx], latitude = list_latitude[yyy])['t2m'].values 
            for zzz in range(0,np.shape(array_above_percentile)[0]):
                #This condition ensures that the local thresholds are computed only one time 
                if np.isnan(array_climatology_percentile[serie_dayofyear[zzz]-1,xxx,yyy]) == True:
                    #Select the days within the window considered. Different conditions when 
                    #the window is at the beginning, end or middle of the natural year
                    if serie_dayofyear[zzz]-semiwindow < 1:
                        boolean_dayofyear = (serie_dayofyear <= serie_dayofyear[zzz]+semiwindow) + (serie_dayofyear >= max_dayofyear + (serie_dayofyear[zzz]-semiwindow) )
                    elif serie_dayofyear[zzz]+semiwindow > max_dayofyear:
                        boolean_dayofyear = (serie_dayofyear >= serie_dayofyear[zzz]-semiwindow) + (serie_dayofyear <= serie_dayofyear[zzz] + semiwindow - max_dayofyear  )
                    else:
                        boolean_dayofyear = (serie_dayofyear >= serie_dayofyear[zzz]-semiwindow) * (serie_dayofyear <= serie_dayofyear[zzz]+semiwindow)
                    array_climatology_percentile[serie_dayofyear[zzz]-1,xxx,yyy]  = np.percentile(serie_work[(serie_year >= start_climatology) * (serie_year <= end_climatology) * boolean_dayofyear],percentile_threshold)
                    if option_T2 == 'T2maxmin':
                        array_climatology_percentile_maxmin[serie_dayofyear[zzz]-1,xxx,yyy] = np.percentile(serie_work2[(serie_year >= start_climatology) * (serie_year <= end_climatology) * boolean_dayofyear],percentile_threshold)
                if option_T2 in ['T2max','T2min']:
                    array_above_percentile[zzz,xxx,yyy] = serie_work[zzz] > array_climatology_percentile[serie_dayofyear[zzz]-1,xxx,yyy]
                    array_hw_exceedance[zzz,xxx,yyy]     = np.subtract(serie_work[zzz],array_climatology_percentile[serie_dayofyear[zzz]-1,xxx,yyy])
                elif option_T2 == 'T2maxmin':
                    array_above_percentile[zzz,xxx,yyy] = (serie_work[zzz] > array_climatology_percentile[serie_dayofyear[zzz]-1,xxx,yyy]) * (serie_work2[zzz] > array_climatology_percentile_maxmin[serie_dayofyear[zzz]-1,xxx,yyy])
                    array_hw_exceedance[zzz,xxx,yyy]     = np.mean([np.subtract(serie_work[zzz],array_climatology_percentile[serie_dayofyear[zzz]-1,xxx,yyy]),np.subtract(serie_work2[zzz],array_climatology_percentile_maxmin[serie_dayofyear[zzz]-1,xxx,yyy])])
            ################## Identify groups with "persistence_HW" or more days fulfilling the T2m > percentile_threshold condition
            ### method described in https://stackoverflow.com/questions/54997393/identify-groups-consecutive-true-values-in-pd-serie        
            aux1 = pd.Series(array_above_percentile[:,xxx,yyy])
            aux2 = aux1.ne(aux1.shift()).cumsum()
            array_hw_days[:,xxx,yyy]     = (aux2.map(aux2.value_counts()) >= persistence_HW)  * (array_above_percentile[:,xxx,yyy] == True)
            array_persistence[:,xxx,yyy] = aux2.map(aux2.value_counts())
    ################## Convert to 0 the exceedance and persistence values for days that are not a HW
    array_hw_exceedance[array_hw_days == False] = 0.
    array_persistence [array_hw_days == False] = 0.
    ################## Save the hw exceedance and persistence data as netcdfs (i.e. these files are required in part 3, but they can be deleted if option_save_temporary_files condition is set)
    T2work_coords = T2work.coords
    T2work_dims   = T2work.dims
    T2work_lon    = T2work.longitude.values
    T2work_lat    = T2work.latitude.values
    del T2work
    xarray_hw_exceedance = xr.DataArray(array_hw_exceedance, coords=T2work_coords, dims=T2work_dims, name='hw_exceedance').astype('float32')
    xarray_hw_exceedance.to_netcdf(path=path_out+'hw_exceedance.nc')
    xarray_persistence = xr.DataArray(array_persistence, coords=T2work_coords, dims=T2work_dims, name='hw_persistence').astype('int')
    xarray_persistence.to_netcdf(path=path_out+'hw_persistence.nc')
    del array_above_percentile, array_hw_days, array_climatology_percentile, xarray_hw_exceedance, xarray_persistence, array_hw_exceedance, array_persistence
    if option_T2 == 'T2maxmin':
        del T2work2, array_climatology_percentile_maxmin, 
################## Part 2: Identify the days considered as heatwave   ##################


# In[ ]:


################## Part 3: Compute metrics for all HW days in the considered interval ##################
if option_month_season == 1 or option_rolling == 1:
    ################## Define the path to save the outputs (path_out_metrics)
    path_out_metrics    = path_out + 'metrics/'
    ################## Load the netcdfs created in Part 2
    xarray_hw_exceedance = xr.open_mfdataset(path_out+'hw_exceedance.nc') 
    xarray_persistence  = xr.open_mfdataset(path_out+'hw_persistence.nc')
    serie_year          = xarray_hw_exceedance["time.year"].values
    serie_month         = xarray_hw_exceedance["time.month"].values
    serie_season        = xarray_hw_exceedance["time.season"].values
    array_hw_exceedance  = xarray_hw_exceedance['hw_exceedance'].values
    array_persistence   = xarray_persistence['hw_persistence'].values
    xarray_coords       = xarray_hw_exceedance.coords
    xarray_dims         = xarray_hw_exceedance.dims
    lon_values          = xarray_hw_exceedance.longitude.values
    lat_values          = xarray_hw_exceedance.latitude.values
    del xarray_hw_exceedance, xarray_persistence

################## Part 3.1: Years, seasons and months ##################
################## The output arrays give a value per year/season/month
if option_month_season == 1:
    ################## Compute the final size and define the output arrays
    no_years   = (np.max(serie_year)-np.min(serie_year))+1
    no_months  = no_years * 12
    no_seasons = no_years * 4
    array_HWF_monthly  = np.full([no_months,np.shape(array_hw_exceedance)[1],np.shape(array_hw_exceedance)[2]],0.)
    array_HWA_monthly  = np.full(np.shape(array_HWF_monthly),0.)
    array_HWM_monthly  = np.full(np.shape(array_HWF_monthly),0.)
    array_HWD_monthly  = np.full(np.shape(array_HWF_monthly),0.)
    array_HWF_seasonal = np.full([no_seasons,np.shape(array_hw_exceedance)[1],np.shape(array_hw_exceedance)[2]],0.)
    array_HWA_seasonal = np.full(np.shape(array_HWF_seasonal),0.)
    array_HWM_seasonal = np.full(np.shape(array_HWF_seasonal),0.)
    array_HWD_seasonal = np.full(np.shape(array_HWF_seasonal),0.)
    array_HWF_yearly   = np.full([no_years,np.shape(array_hw_exceedance)[1],np.shape(array_hw_exceedance)[2]],0.)
    array_HWA_yearly   = np.full(np.shape(array_HWF_yearly),0.)
    array_HWM_yearly   = np.full(np.shape(array_HWF_yearly),0.)
    array_HWD_yearly   = np.full(np.shape(array_HWF_yearly),0.)
    coordinate_time_monthly  = np.full([no_months],'', dtype="U10")
    coordinate_time_seasonal = np.full([no_seasons],'', dtype="U10")
    coordinate_time_yearly   = np.full([no_years],'', dtype="U10")
    ################## Computation of the metrics
    aux_monthly  = 0
    aux_seasonal = 0
    aux_yearly   = 0
    for zzz in np.arange(np.min(serie_year),np.max(serie_year)+1):
        #### Years
        boolean_array_yearly = np.full(np.shape(array_hw_exceedance)[0],False)
        boolean_array_yearly[(serie_year == zzz)] = True
        array_HWF_yearly[aux_yearly,:,:] = np.divide(np.sum(array_hw_exceedance[boolean_array_yearly,:,:] > 0,axis=0),np.sum(boolean_array_yearly))
        array_HWA_yearly[aux_yearly,:,:] = np.max(array_hw_exceedance[boolean_array_yearly,:,:],axis=0)        
        array_HWM_yearly[aux_yearly,:,:] = np.sum(array_hw_exceedance[boolean_array_yearly,:,:],axis=0)        
        array_HWD_yearly[aux_yearly,:,:] = np.max(array_persistence[boolean_array_yearly,:,:],axis=0)
        coordinate_time_yearly[aux_yearly] = str(zzz) + '-07-01'
        aux_yearly = aux_yearly + 1
        #### Months
        for aaa in range(1,13):
            boolean_array_monthly = np.full(np.shape(array_hw_exceedance)[0],False)
            boolean_array_monthly[(serie_year == zzz) * (serie_month == aaa)] = True
            array_HWF_monthly[aux_monthly,:,:] = np.divide(np.sum(array_hw_exceedance[boolean_array_monthly,:,:] > 0,axis=0),np.sum(boolean_array_monthly))
            array_HWA_monthly[aux_monthly,:,:] = np.max(array_hw_exceedance[boolean_array_monthly,:,:],axis=0)        
            array_HWM_monthly[aux_monthly,:,:] = np.sum(array_hw_exceedance[boolean_array_monthly,:,:],axis=0)        
            array_HWD_monthly[aux_monthly,:,:] = np.max(array_persistence[boolean_array_monthly,:,:],axis=0)
            coordinate_time_monthly[aux_monthly] = str(zzz) + '-' + str(aaa).zfill(2) + '-15'
            aux_monthly = aux_monthly + 1
        #### Seasons
        aux_months  = ['12','4','7','10']
        aux_seasons = ['DJF','MAM','JJA','SON']
        for bbb in aux_seasons:
            boolean_array_seasonal = np.full(np.shape(array_hw_exceedance)[0],False)
            if bbb in ['MAM','JJA','SON']:
                boolean_array_seasonal[(serie_year == zzz) * (serie_season == bbb)] = True  
            elif bbb == 'DJF':
                boolean_array_seasonal[((serie_year == zzz) * (serie_month == 12)) + ((serie_year == zzz+1) * (np.isin(serie_month,[1,2])))] = True
            if np.sum(boolean_array_seasonal) < 60:
                array_HWF_seasonal[aux_seasonal,:,:], array_HWA_seasonal[aux_seasonal,:,:], array_HWM_seasonal[aux_seasonal,:,:], array_HWD_seasonal[aux_seasonal,:,:] = np.nan, np.nan, np.nan, np.nan
            else:
                array_HWF_seasonal[aux_seasonal,:,:] = np.divide(np.sum(array_hw_exceedance[boolean_array_seasonal,:,:] > 0,axis=0),np.sum(boolean_array_seasonal))
                array_HWA_seasonal[aux_seasonal,:,:] = np.max(array_hw_exceedance[boolean_array_seasonal,:,:],axis=0)        
                array_HWM_seasonal[aux_seasonal,:,:] = np.sum(array_hw_exceedance[boolean_array_seasonal,:,:],axis=0)        
                array_HWD_seasonal[aux_seasonal,:,:] = np.max(array_persistence[boolean_array_seasonal,:,:],axis=0) 
            coordinate_time_seasonal[aux_seasonal] = str(zzz) + '-' + aux_months[aux_seasons.index(bbb)].zfill(2) + '-15'
            aux_seasonal = aux_seasonal + 1            
    del boolean_array_monthly, boolean_array_seasonal,boolean_array_yearly
    ################## Save the output arrays of each metric as netcdfs
    ################## (after converting the output array to xarray)
    #### Years
    path_out_metrics_yearly = path_out_metrics + 'yearly/'
    os.system ('mkdir -p ' + path_out_metrics_yearly)   
    xarray_HWF_yearly = xr.DataArray(array_HWF_yearly, 
            coords={'time': np.array(coordinate_time_yearly, dtype='datetime64'), 'longitude': lon_values,'latitude': lat_values}, 
            dims=['time', 'longitude', 'latitude'], name = 'HWF').astype('float32')
    xarray_HWF_yearly.to_netcdf(path=path_out_metrics_yearly+'HWF_yearly.nc')
    xarray_coords_yearly = xarray_HWF_yearly.coords
    xarray_dims_yearly   = xarray_HWF_yearly.dims
    del xarray_HWF_yearly, array_HWF_yearly
    xarray_HWA_yearly = xr.DataArray(array_HWA_yearly, coords=xarray_coords_yearly, dims=xarray_dims_yearly, name = 'HWA').astype('float32')
    xarray_HWA_yearly.to_netcdf(path=path_out_metrics_yearly+'HWA_yearly.nc')
    del xarray_HWA_yearly, array_HWA_yearly
    xarray_HWM_yearly = xr.DataArray(array_HWM_yearly, coords=xarray_coords_yearly, dims=xarray_dims_yearly, name = 'HWM').astype('float32')
    xarray_HWM_yearly.to_netcdf(path=path_out_metrics_yearly+'HWM_yearly.nc')
    del xarray_HWM_yearly, array_HWM_yearly
    xarray_HWD_yearly = xr.DataArray(array_HWD_yearly, coords=xarray_coords_yearly, dims=xarray_dims_yearly, name = 'HWD').astype('float32')
    xarray_HWD_yearly.to_netcdf(path=path_out_metrics_yearly+'HWD_yearly.nc')
    del xarray_HWD_yearly, array_HWD_yearly
    #### Months
    path_out_metrics_monthly = path_out_metrics + 'monthly/'
    os.system ('mkdir -p ' + path_out_metrics_monthly)   
    xarray_HWF_monthly = xr.DataArray(array_HWF_monthly, 
            coords={'time': np.array(coordinate_time_monthly, dtype='datetime64'), 'longitude': lon_values,'latitude': lat_values}, 
            dims=['time', 'longitude', 'latitude'], name = 'HWF').astype('float32')
    xarray_HWF_monthly.to_netcdf(path=path_out_metrics_monthly+'HWF_monthly.nc')
    xarray_coords_monthly = xarray_HWF_monthly.coords
    xarray_dims_monthly   = xarray_HWF_monthly.dims
    del xarray_HWF_monthly, array_HWF_monthly
    xarray_HWA_monthly = xr.DataArray(array_HWA_monthly, coords=xarray_coords_monthly, dims=xarray_dims_monthly, name = 'HWA').astype('float32')
    xarray_HWA_monthly.to_netcdf(path=path_out_metrics_monthly+'HWA_monthly.nc')
    del xarray_HWA_monthly, array_HWA_monthly
    xarray_HWM_monthly = xr.DataArray(array_HWM_monthly, coords=xarray_coords_monthly, dims=xarray_dims_monthly, name = 'HWM').astype('float32')
    xarray_HWM_monthly.to_netcdf(path=path_out_metrics_monthly+'HWM_monthly.nc')
    del xarray_HWM_monthly, array_HWM_monthly
    xarray_HWD_monthly = xr.DataArray(array_HWD_monthly, coords=xarray_coords_monthly, dims=xarray_dims_monthly, name = 'HWD').astype('float32')
    xarray_HWD_monthly.to_netcdf(path=path_out_metrics_monthly+'HWD_monthly.nc')
    del xarray_HWD_monthly, array_HWD_monthly
    #### Seasons
    path_out_metrics_seasonal = path_out_metrics + 'seasonal/'
    os.system ('mkdir -p ' + path_out_metrics_seasonal)   
    xarray_HWF_seasonal = xr.DataArray(array_HWF_seasonal, 
            coords={'time': np.array(coordinate_time_seasonal, dtype='datetime64'), 'longitude': lon_values,'latitude': lat_values}, 
            dims=['time', 'longitude', 'latitude'], name = 'HWF').astype('float32')
    xarray_HWF_seasonal.to_netcdf(path=path_out_metrics_seasonal+'HWF_seasonal.nc')
    xarray_coords_seasonal = xarray_HWF_seasonal.coords
    xarray_dims_seasonal   = xarray_HWF_seasonal.dims
    del xarray_HWF_seasonal, array_HWF_seasonal
    xarray_HWA_seasonal = xr.DataArray(array_HWA_seasonal, coords=xarray_coords_seasonal, dims=xarray_dims_seasonal, name = 'HWA').astype('float32')
    xarray_HWA_seasonal.to_netcdf(path=path_out_metrics_seasonal+'HWA_seasonal.nc')
    del xarray_HWA_seasonal, array_HWA_seasonal
    xarray_HWM_seasonal = xr.DataArray(array_HWM_seasonal, coords=xarray_coords_seasonal, dims=xarray_dims_seasonal, name = 'HWM').astype('float32')
    xarray_HWM_seasonal.to_netcdf(path=path_out_metrics_seasonal+'HWM_seasonal.nc')
    del xarray_HWM_seasonal, array_HWM_seasonal
    xarray_HWD_seasonal = xr.DataArray(array_HWD_seasonal, coords=xarray_coords_seasonal, dims=xarray_dims_seasonal, name = 'HWD').astype('float32')
    xarray_HWD_seasonal.to_netcdf(path=path_out_metrics_seasonal+'HWD_seasonal.nc')
    del xarray_HWD_seasonal, array_HWD_seasonal


# In[ ]:


################## Part 3.2: Running windows of the specified temporal witdth ##################
################## The output arrays of each metric gives a value per day
if option_rolling == 1:
    ################## Define the output arrays
    array_HWF = np.full(np.shape(array_hw_exceedance),0.)
    array_HWA = np.full(np.shape(array_hw_exceedance),0.)
    array_HWM = np.full(np.shape(array_hw_exceedance),0.)
    array_HWD = np.full(np.shape(array_hw_exceedance),0.)
    ################## Computation of the metrics
    for zzz in range(0,np.shape(array_hw_exceedance)[0]):
        boolean_array = np.full(np.shape(array_hw_exceedance)[0],False)
        boolean_array[zzz:zzz+window_width] = True
        if np.sum(boolean_array) == window_width:
            array_HWF[zzz,:,:] = np.divide(np.sum(array_hw_exceedance[boolean_array,:,:] > 0,axis=0),np.sum(boolean_array))
            array_HWA[zzz,:,:] = np.max(array_hw_exceedance[boolean_array,:,:],axis=0)        
            array_HWM[zzz,:,:] = np.sum(array_hw_exceedance[boolean_array,:,:],axis=0)        
            array_HWD[zzz,:,:] = np.max(array_persistence[boolean_array,:,:],axis=0)   
        else:
            array_HWF[zzz,:,:],array_HWA[zzz,:,:],array_HWM[zzz,:,:],array_HWD[zzz,:,:] = np.nan, np.nan, np.nan, np.nan
    del boolean_array, array_hw_exceedance, array_persistence

    ################## Save the output arrays of each metric as netcdfs
    path_out_metrics_rolling = path_out_metrics + 'rolling_' + str(window_width) + 'days/'
    os.system ('mkdir -p ' + path_out_metrics_rolling)   
    xarray_HWF = xr.DataArray(array_HWF, coords=xarray_coords, dims=xarray_dims, name = 'HWF').astype('float32')
    xarray_HWF.to_netcdf(path=path_out_metrics_rolling+'HWF_'+str(window_width)+'_days.nc')
    del xarray_HWF, array_HWF
    xarray_HWA = xr.DataArray(array_HWA, coords=xarray_coords, dims=xarray_dims, name = 'HWA').astype('float32')
    xarray_HWA.to_netcdf(path=path_out_metrics_rolling+'HWA_'+str(window_width)+'_days.nc')
    del xarray_HWA, array_HWA
    xarray_HWM = xr.DataArray(array_HWM, coords=xarray_coords, dims=xarray_dims, name = 'HWM').astype('float32')
    xarray_HWM.to_netcdf(path=path_out_metrics_rolling+'HWM_'+str(window_width)+'_days.nc')
    del xarray_HWM, array_HWM
    xarray_HWD = xr.DataArray(array_HWD, coords=xarray_coords, dims=xarray_dims, name = 'HWD').astype('float32')
    xarray_HWD.to_netcdf(path=path_out_metrics_rolling+'HWD_'+str(window_width)+'_days.nc')
    del xarray_HWD, array_HWD
    ################## Part 3: Compute metrics for all HW days in the considered interval ##################


# In[ ]:


################## Remove intermediate arrays needed to compute the HW metrics
if option_save_temporary_files != 1:
    os.remove(path_out+'hw_exceedance.nc')
    os.remove(path_out+'hw_persistence.nc')


# In[ ]:


end = time.time()
str(datetime.timedelta(seconds=end - start))

