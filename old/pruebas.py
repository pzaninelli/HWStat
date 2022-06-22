#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed May 25 09:48:01 2022
main script
@author: pzaninelli
"""
import numpy as np
from src.ParamsInit import *
from src.DailyAccesor import *
import timeit

def apply_mask(t2m, mask):
    mask = (mask > 0.5)
    return t2m.where(mask, np.nan)

def dask_percentile(arr, axis=0, q=95):
    if len(arr.chunks[axis]) > 1:
        msg = ('Input array cannot be chunked along the percentile '
               'dimension.')
        raise ValueError(msg)
    return xr.map_blocks(np.nanpercentile, arr, axis=axis, q=q,
                                 drop_axis=axis)

def anom_thres(da,win=30, groupby_type = "time.dayofyear"):
    r = da.rolling(time = win,center = True)
    rolling_da = r.construct(time = "t_win")
    perc = rolling_da.groupby(groupby_type).quantile(q = 0.9,dim=("time","t_win"),skipna = True)
    return da.groupby(groupby_type) - perc.drop("quantile")

def Sign_trans(x):
    return np.where(np.sign(x)==-1, 0, np.sign(x))

def island_cumsum_vectorized(x):
    """
    Parameters
    ----------
    x : TimeSeries
        vector of 0 and 1.

    Returns
    -------
    Array with the outcome of cumulative values.
    from https://stackoverflow.com/questions/42129021/counting-consecutive-1s-in-numpy-array
    """
    a_ext = np.concatenate(( [0], x, [0] ), dtype = object)
    idx = np.flatnonzero(a_ext[1:] != a_ext[:-1])
    a_ext[1:][idx[1::2]] = idx[:-1:2] - idx[1::2]
    return a_ext.cumsum()[1:-1]

def island_cumsum_vectorized2(x):
    return np.concatenate([np.cumsum(c) if c[0] == 1 else c for c in np.split(x, 1 + 
                                                    np.where(np.diff(x))[0])])

def compute_hd_len(x):
    return island_cumsum_vectorized(Sign_trans(x))

if __name__ == "__main__":
    # main()
    params = ParamsInit.from_file("parameters/params.ini")
    t2m = xr.open_dataarray(params.dir_in) 
                             # chunks={'lat': 10, 'lon': 10, 'time': -1})
    # t2m = apply_mask(t2m, xr.open_dataarray(params.file_mask)[0])
    
    tmax = t2m.Daily.dailymax()
    tmax = tmax.Daily.toCelsius()
    tmax = tmax.Daily.applyMask( xr.open_dataarray(params.file_mask)[0])
    # tmax.Daily.base_period = {"start":1981,"end":1982}
    # perc_tx = tmax.Daily.getThreshold()
    # anom_tx = tmax.Daily.getAnomaly()
    ex = tmax.Daily.excedence(base_period = {'start': 1981, 'end': 1982}, 
                                perc = 90, 
                                window = 30)
    adhw_seas = tmax.Daily.ADHW(group_by="season")
    adhw_seas = tmax.Daily.ADHW(group_by="season",base_period = {'start': 1981, 'end': 1982}, 
                                perc = 90, 
                                window = 30)
    
    hwf_seas = tmax.Daily.hwf(group_by="season",base_period = {'start': 1981, 'end': 1982}, 
                                perc = 90, 
                                window = 30)
    
    wM = tmax.Daily.WatchMat(base_period = {'start': 1981, 'end': 1982}, 
                                perc = 90, 
                                window = 30)
    chd = tmax.Daily.getConsHDN(base_period = {'start': 1981, 'end': 1982}, 
                                perc = 90, 
                                window = 30)
    hwf = tmax.Daily.groupby("time.season").hwf(base_period = {'start': 1981, 'end': 1982}, 
                                perc = 90, 
                                window = 30)
    tmin = t2m.daily.dailymin()
    tmin = tmin.daily.toCelsius()
    tmin = tmin.assign_coords({"dayofyear":tmin.time.dt.dayofyear})
    tmin_anom_ch = tmin_anom.chunk({"latitude": 10, "longitude" : 10})
    
    # calcular las fechas en las que se da ola de calor
    a =np.argwhere(np.array(chd==3))
    b = np.argwhere(np.array(chd>3))
    chd2[a[0][0],a[0][1],a[0][2]].time.value
    for time,ilo,ila in zip(b[:,0],b[:,2],b[:,1]): hw_arr[time,ila,ilo]=1
    
    tmax_hw = xr.apply_ufunc(island_cumsum_vectorized, sign_tx, 
                              input_core_dims=[["time"]],
                              output_core_dims=[["time"]],
                              vectorize=True).transpose("time","latitude","longitude")
    tmin_hw2 = xr.apply_ufunc(island_cumsum_vectorized, tmin_anom_ch, 
                              input_core_dims=[["time"]],
                              output_core_dims=[["time"]],
                              dask = "parallelized",
                              vectorize=True).transpose("time","latitude","longitude")
    
    # https://stackoverflow.com/questions/54938180/get-95-percentile-of-the-variables-for-son-djf-mam-over-multiple-years-data
#    tmin = tmin.sel(latitude = np.arange(-60,90),method = "nearest") # elegir latitud entre -60 y 90
# r = Tmax.rolling(time = 30)
# rolling_tmax = r.construct(time = "t_win")
# aux2 = rolling_tmax.groupby("monthday").reduce(np.percentile,q=90)