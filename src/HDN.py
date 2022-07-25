#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Percentile and Heat Day-Night Classes
@author: José Manuel Garrido Perez, Pablo G. Zaninelli
@year: 2022
"""

import xarray as xr
import numpy as np
import warnings
from os.path import exists as path_exists
from src.cmodules.funcx import *

class Percentile:
    
    def __init__(self, 
                 xarray_obj,
                 base_period = {}, 
                 window = None,
                 perc = None
                 ):
        self._obj = xarray_obj
        self._percentile = None
        self._base_period = base_period
        self._window = window
        self._p = perc
        self._time, self._lat, self._lon = "time", "latitude", "longitude"
        
    def __repr__(self):
        return f"""
                <PercentileClass>:
                Percentile(
                    arr,
                    base_period = {self.base_period},
                    window = {self.window}
                    perc = {self.perc}
                    )
                """
    # comparison methods
    def __eq__(self,o):
        isequal = dict(eq_base_period = self.base_period == o.base_period,
                       eq_window = self.window == o.window,
                       eq_perc = self.perc == o.perc)
        return all(isequal.values())
        
    
    def setDims(self,**kwargs):
        for dim,name in kwargs.items():
            if dim.lower() in "longitude":
                self._lon = name
            elif dim.lower() in "latitude":
                self._lat = name
            elif dim.lower() in "time":
                self._time = name
    @property
    def base_period(self):
        return self._base_period
    
    @property
    def window(self):
        return self._window
    
    @property 
    def perc(self):
        return self._p
    
    @property
    def percentile(self):
        return self._percentile
    
    @base_period.setter
    def base_period(self, base_period):
        assert isinstance(base_period, dict), "ERROR::'base_period' must be a dict type!"
        assert len(base_period) == 2, "ERROR::lenght of dictionary is larger than 2"
        if not base_period:
            # raise ValueError("base_period must be provided!")
            self._base_period["start"] = int(self._obj[self._time].dt.year.min())
            self._base_period["end"] = int(self._obj[self._time].dt.year.max())
            print("base_period not provided!\n")
            print("It was considered as base_period:\n")
            print(*base_period.items(), sep = "\n")
        if not "start" in base_period or not "end" in base_period:
            raise AttributeError("'start' and 'end' must be provided in base_period dict!")
        assert base_period["start"] < base_period["end"], "ERROR:: start is after end"
        self._base_period["start"] = base_period["start"]
        self._base_period["end"] = base_period["end"]
    
    @window.setter    
    def window(self,win):
        self._window = win
    
    @perc.setter
    def perc(self,p):
        assert p>=0 & p<=100, "ERROR:: percentile must be between 0 and 100"
        self._p = p
   
    @percentile.setter
    def percentile(self, percentile):
        assert percentile is None or isinstance(percentile, xr.DataArray), "'percentile' must be a DataArray Class or None"
        if percentile is not None:
            assert len(percentile[self._time]) in (365,366), "Time coordinate must have a length of 365 or 366"         
        self._percentile = percentile
        
        
    def computePerc(self):
         print("Computing percentile...")
         if not self._daily_check(self._obj,self._time):
             warnings.warn("Variable must be daily min or max, please apply 'dailymin/max' method")
         if not self.base_period:
             raise ValueError("'base_period' must be defined before compute percentile!")
         aux = (self._obj.HWCNT._addDOY()).loc[{self._time : self._obj[self._time].dt.year.isin(range(self.base_period["start"],self.base_period["end"]))}]
         if aux.chunksizes:
             self._undoParallel(aux,return_chunk=False)   
         r = aux.rolling(time = self.window,center = True)
         rolling_da = r.construct({self._time : "t_win"})
         self._percentile = (rolling_da.groupby(self._time + ".dayofyear").quantile(q = self.perc/100,
                                             dim=(self._time,"t_win"), skipna = True)).drop("quantile")
         self._percentile.name = "Percentile of " + self._obj.name
         self._percentile.attrs["percentile"] = self.perc
         self._percentile.attrs["window"] = self.window
         self._percentile.attrs["base_period"] = str(self.base_period["start"]) + '-' + str(self.base_period["end"])
         
         print("Computed percentile!!")
         
    
    def savePerc(self, filename=None):
        if filename is None:
            filename = self._set_filename(self.base_period, self.perc, self.window)
        self._check_nc_ends(filename)
        self._check_compute_percentile()
        self.percentile.to_netcdf(filename)
        
        
    def _check_compute_percentile(self):
        if self.percentile is None:
            self.computePerc()
        
    @classmethod
    def load(cls, xarray_obj, filename):
        if not path_exists(filename):
            raise FileExistsError(f"{filename} does not exist!")
        perc = xr.load_dataarray(filename)
        attributes = perc.attrs
        if not "base_period" in attributes or not "percentile" in attributes or not "window" in attributes:
            raise AttributeError("Percentile xarray must have 'base_period', 'percentile' and 'window' defined!")
        base_period = {"start": int(attributes["base_period"].split("-")[0]),
                       "end": int(attributes["base_period"].split("-")[1])}
        cls.percentile = perc
        return cls(xarray_obj,
                   base_period,
                   attributes["window"],
                   attributes["percentile"])
            
    @staticmethod
    def _check_nc_ends(filename):
        if not filename.endswith('.nc'):
            return filename + '.nc'
        else:
            pass
        
    @staticmethod
    def _set_filename(base_period,perc, window):
        return f"Percentile_P-{perc}_Win-{window}_days_{base_period['start']}_{base_period['end']}.nc"
    
    @staticmethod
    def _daily_check(arr,tname):
        return xr.infer_freq(arr[tname])=='D'
    
    @staticmethod
    def _undoParallel(arr,return_chunk = True):
        assert arr.chunksizes, "the array is not parallelized!"
        if return_chunk:
            Chunks = {}
            for key,value in arr.chunksizes.items(): Chunks[key] = value[0]
            arr.load()
            return Chunks
        else:
            arr.load()

        
    @staticmethod
    def _doParallel(arr, Chunks):
        assert not arr.chunksizes, f"{arr} was already parallelized!"
        return arr.chunk(Chunks)
        


class HDN(Percentile):
    
    
    def __init__(self, xarray_obj,
                 base_period = {}, 
                 window = None,
                 perc = None):
        super().__init__(xarray_obj,base_period,window,perc)
        self._thres = None
        self._hdn = None
        self._wm = None
    
    
    @property
    def hdn(self):
        return self._hdn
    
    @property
    def threshold(self):
        return self._thres
    @property
    def WatchMat(self):
        return self._wm

    @hdn.setter
    def hdn(self,hdn):
        self._hdn = hdn
        
    @threshold.setter
    def threshold(self,thres):
        assert thres is None or isinstance(thres,int) and thres >0, "threshold must be greater than 0 or None"
        self._thres = thres  
   
    @WatchMat.setter
    def WatchMat(self,wm):
        self._wm = wm
   
    def getAnom(self):
        if self.percentile is None:
            self.computePerc()
        return self._obj.groupby(".".join((self._time,"dayofyear"))) - self.percentile   

    def computeHDN(self):
         print("Compute count HDN")
         anom = self.getAnom()
         Sign = xr.where(np.sign(anom)==-1,0,np.sign(anom))    
         self.hdn = xr.apply_ufunc(self._island_cumsum_vectorized, Sign.load(), 
                                   input_core_dims=[[self._time]],
                                   output_core_dims=[[self._time]],
                                   vectorize=True).transpose(self._time,self._lat,
                                                 self._lon).astype(np.float32)
         
         print("HDN computed!")
    
    def computeWatchM(self):
        assert self.threshold is not None, "threshold was not defined!"
        if self.hdn is None:
            self.computeHDN()
        if self._obj.chunksizes:
            Chunks = self._undoParallel(self._obj)
            print("Computing WatchMat...")
            WM = self._countHDN(self.hdn, self.threshold)
            print("WatchMat computed")
            self._obj = self._doParallel(self._obj, Chunks)
        else:
            print("Computing WatchMat...")
            WM = self._countHDN(self.hdn, self.threshold)
            print("WatchMat computed")
        self.WatchMat = WM
                                                         
    def computeWatchM_C(self): # cython version
        assert self.threshold is not None, "threshold was not defined!"
        if self.hdn is None:
            self.computeHDN()
        if self._obj.chunksizes:
            Chunks = self._undoParallel(self._obj)
            print("Computing WatchMat...")
            WM = self._countHDN_C(self.hdn, self.threshold)
            print("WatchMat computed")
            self._obj = self._doParallel(self._obj, Chunks)
        else:
            print("Computing WatchMat...")
            WM = self._countHDN_C(self.hdn, self.threshold)
            print("WatchMat computed")
        self.WatchMat = WM
    
    
    @staticmethod
    def _island_cumsum_vectorized(x):
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
  
    @staticmethod
    def _countHDN(arr, thres):
       arr2 = xr.zeros_like(arr)
       id3 = np.argwhere(np.array(arr==thres))
       id3_time = id3[:,0]
       id3_time2 = id3_time-1
       id3_time1 = id3_time2 -1
       id3_lat = id3[:,1]
       id3_lon = id3[:,2]
       idp3 = np.argwhere(np.array(arr>thres))
       idp3_time = idp3[:,0]
       idp3_lat = idp3[:,1]
       idp3_lon = idp3[:,2]
       for time,lat,lon in zip(id3_time2,id3_lat, id3_lon): arr2[time,lat,lon] = 1
       for time,lat,lon in zip(id3_time1,id3_lat, id3_lon): arr2[time,lat,lon] = 1
       for time,lat,lon in zip(id3_time,id3_lat, id3_lon):  arr2[time,lat,lon] = 1
       for time,lat,lon in zip(idp3_time,idp3_lat, idp3_lon): arr2[time,lat,lon] = 1
       return arr2
   
    @staticmethod
    def _countHDN_C(arr,thres):
        arr2 = xr.zeros_like(arr)
        arr2 = arr2.to_numpy()
        id3 = np.argwhere(np.array(arr==thres))
        id3_time = id3[:,0]
        id3_time2 = id3_time-1
        id3_time1 = id3_time2 -1
        id3_lat = id3[:,1]
        id3_lon = id3[:,2]
        idp3 = np.argwhere(np.array(arr>thres))
        idp3_time = idp3[:,0]
        idp3_lat = idp3[:,1]
        idp3_lon = idp3[:,2]
        arr2 = fillones(arr2,id3_time2,id3_lat, id3_lon)
        arr2 = fillones(arr2,id3_time1,id3_lat, id3_lon)
        arr2 = fillones(arr2,id3_time,id3_lat, id3_lon)
        arr2 = fillones(arr2,idp3_time,idp3_lat, idp3_lon)
        arr2 = arr.copy(data = arr2)
        return arr2