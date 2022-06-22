#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
HeatWave Accesor for Xarray
@author: José Manuel Garrido Perez, Pablo G. Zaninelli
@year: 2022
"""

from src.HDN import *
from copy import deepcopy
                                                           
@xr.register_dataarray_accessor("HWCNT")
class HWCNTAccesor:
    
    def __init__(self, xarray_obj):
        self._obj = xarray_obj
        self._time, self._lat, self._lon = "time", "latitude", "longitude"
        self._percObj = HDN(xarray_obj)
        # self._percentile=None
        # self._base_period = {}
        # self._p = 90
        # self._window = 30
        
        
    def convertLon(self):
        """
        Convert longitude

        Returns
        -------
        DataArray object
        """
        return self._obj.assign_coords({self._lon : 
                            (((self._obj[self._lon] + 180) % 360) - 180)})

    def setDims(self,**kwargs):
        for dim,name in kwargs.items():
            if dim.lower() in "longitude":
                self._lon = name
            elif dim.lower() in "latitude":
                self._lat = name
            elif dim.lower() in "time":
                self._time = name
                
    def dailymin(self):
        """
        Daily minimum temperature

        Returns
        -------
        DataArray object
            From houly to daily values.

        """
        tn = self._obj.resample({self._time:'1D'}).min()
        tn.name = "Minimum temperature"
        return tn
    
    def dailymax(self):
        """
        Daily maximum temperature

        Returns
        -------
        DataArray object
            From houly to daily values.

        """
        tx = self._obj.resample({self._time:'1D'}).max()
        tx.name = "Maximum temperature"
        return tx
    
    def toCelsius(self):
        """
        Convert temperature to Celsius degrees

        Returns
        -------
        DataArray object
        
        """
        temp = self._obj - 273.15    
        temp.attrs["Units"] = "ºC"
        return temp
    
    def applyMask(self, mask = None, IndMask = 0.5):
        """
        Apply ocean mask

        Returns
        -------
        DataArray object
            If a mask is provided, ocean is masked.

        """
        if mask is not None:
            return self._obj.where(mask > IndMask, np.nan)
        else:
            warnings.warn("Mask file was not provided so it was not applied!!")
            return self._obj
        
    def _addDOY(self):
        return self._obj.assign_coords({"dayofyear":self._obj[self._time].dt.dayofyear})
    
    @property
    def percObj(self):
        return self._percObj

    
    @percObj.setter
    def percObj(self,percobj):
        assert isinstance(percobj, HDN), "'perc_o' must be HDN Class"
        self._percObj = percobj
        
        
    def setThres(self,threshold):
        self._percObj.threshold = threshold
        
    def setParameters(self, base_period = {}, window = None, perc = None, 
                      filename = None):
        if not filename is None:
            self._percObj = HDN.load(self._obj, filename)
        else:
            self._percObj = HDN(self._obj, base_period, window, perc)
            
    def getParameters(self):
        print(self._percObj)
        
    def computePercentile(self):
        self._percObj.computePerc()        
            
    def computeHDN(self):
        self._percObj.computeHDN()
    
    def getPercentile(self ):
        if self._percObj.percentile is None:
            self.computePercentile()
        return self._percObj.percentile
    
    def info(self):
        if self.percObj is None:
         print("""
        Parameters:
            base_period = ''
            percentile = ''
            window = ''
            persistence threshold = ''
            percentile computed = False
            HDN computed = False""")
        else:
            if self._percObj.percentile is None and self._percObj.hdn is None: 
                print(f"""
                      Parameters:
                          base_period = {self.percObj.base_period}
                          percentile = {self.percObj.perc}
                          window = {self.percObj.window}
                          persistence threshold = {self.percObj.threshold}
                          percentile computed = False
                          HDN computed = False""")
            elif  self._percObj.percentile is not None and self._percObj.hdn is None:
                print(f"""
                      Parameters:
                          base_period = {self.percObj.base_period}
                          percentile = {self.percObj.perc}
                          window = {self.percObj.window}
                          persistence threshold = {self.percObj.threshold}
                          percentile computed = True
                          HDN computed = False""")
            elif  self._percObj.percentile is None and self._percObj.hdn is not None:
                print(f"""
                      Parameters:
                          base_period = {self.percObj.base_period}
                          percentile = {self.percObj.perc}
                          window = {self.percObj.window}
                          persistence threshold = {self.percObj.threshold}
                          percentile computed = False
                          HDN computed = True""")
            else:
                print(f"""
                      Parameters:
                          base_period = {self.percObj.base_period}
                          percentile = {self.percObj.perc}
                          window = {self.percObj.window}
                          persistence threshold = {self.percObj.threshold}
                          percentile computed = True
                          HDN computed = True""")
                          
    def getAnomaly(self,**kwargs):
        # self._check_compute_percentile()
        if kwargs:
            base_period, window, perc = self._split_kwargs(kwargs)
            self.setParameters(base_period, window, perc)
            # self.computePercentile()
        return self._percObj.getAnom()
        
    # def groupby(self,group):
    #     return self._obj.groupby(group)

    def getConsHDN(self,**kwargs):
        # self._check_compute_percentile()
        if kwargs:
            base_period, window, perc = self._split_kwargs(kwargs)
            self.setParameters(base_period, window, perc)
        # if self._perObj.hdn is None:
        #     self._percObj.computeHDN()
        return self._percObj.hdn
    
    def copy(self):
        obj2 = HWCNTAccesor(self._obj)
        obj2.setParameters(base_period = self._percObj.base_period,
                          window = self._percObj.window,
                          perc = self._percObj.perc)
        obj2.setThres(self._percObj.threshold)
        obj2._percObj.percentile = self._percObj.percentile
        obj2._percObj.hdn = self._percObj.hdn
        return obj2
    
    def WatchMat(self,**kwargs):
        if kwargs:
            base_period, window, perc = self._split_kwargs(kwargs)
            self.setParameters(base_period, window, perc)
            self._percObj.computeWatchM()
        if self._percObj.WatchMat is None:
            print("**WatchMat is None**")
            self._percObj.computeWatchM()
        return self.percObj.WatchMat

    def WatchMat_C(self,**kwargs):
        if kwargs:
            base_period, window, perc = self._split_kwargs(kwargs)
            self.setParameters(base_period, window, perc)
            self._percObj.computeWatchM_C()
        if self._percObj.WatchMat is None:
            print("**WatchMat is None**")
            self._percObj.computeWatchM_C()
        return self.percObj.WatchMat

    def HWF(self, group_by = None,**kwargs):
        print("Computing HWF...")
        if kwargs:
            # base_period, window, perc = self._split_kwargs(kwargs)
            WM = self.WatchMat(**kwargs)
        else:
            WM = self.WatchMat()
        WM_diff = WM.diff(self._time)
        id_HW = xr.where(WM_diff==-1,0,WM_diff)
        HWF_tot = id_HW.sum(dim=self._time)
        if group_by is None:
            HWF = HWF_tot
        elif isinstance(group_by,str):
            assert group_by in ("month","season","year"), "'group_by' must be 'month','season' or 'year'"
            if group_by == "month":
                HWF = id_HW.resample({self._time:"MS"}).sum(dim=self._time)/HWF_tot
            elif group_by == "season":
                HWF = id_HW.resample({self._time:"QS-DEC"}).sum(dim=self._time)/HWF_tot
            else: # year
                HWF = id_HW.resample({self._time:"AS"}).sum(dim=self._time)/HWF_tot    
        elif isinstance(group_by,int):
            HWF = id_HW.resample({self._time:str(group_by)+'D'}).sum(dim=self._time)
        else:
            raise AttributeError("'group_by' must be 'month','season' or 'year' or an integer of days to group by")
        HWF.name = "HWF"
        print("HWF computed")
        return HWF

    def HWF_C(self, group_by = None,**kwargs):
        print("Computing HWF...")
        if kwargs:
            # base_period, window, perc = self._split_kwargs(kwargs)
            WM = self.WatchMat_C(**kwargs)
        else:
            WM = self.WatchMat_C()
        WM_diff = WM.diff(self._time)
        id_HW = xr.where(WM_diff==-1,0,WM_diff)
        HWF_tot = id_HW.sum(dim=self._time)
        if group_by is None:
            HWF = HWF_tot
        elif isinstance(group_by,str):
            assert group_by in ("month","season","year"), "'group_by' must be 'month','season' or 'year'"
            if group_by == "month":
                HWF = id_HW.resample({self._time:"MS"}).sum(dim=self._time)/HWF_tot
            elif group_by == "season":
                HWF = id_HW.resample({self._time:"QS-DEC"}).sum(dim=self._time)/HWF_tot
            else: # year
                HWF = id_HW.resample({self._time:"AS"}).sum(dim=self._time)/HWF_tot    
        elif isinstance(group_by,int):
            HWF = id_HW.resample({self._time:str(group_by)+'D'}).sum(dim=self._time)
        else:
            raise AttributeError("'group_by' must be 'month','season' or 'year' or an integer of days to group by")
        HWF.name = "HWF"
        print("HWF computed")
        return HWF

    def exceedance(self,**kwargs):
       print("Computing exceedance...")
       if kwargs:
           wm = self.WatchMat(**kwargs)
       else:
           wm = self.WatchMat()
       print("exceedance computed!")
       ex = xr.where(wm == 1, self._obj, 0)
       ex.name = "Exceedance"
       return ex
    
    def exceedance_C(self,**kwargs):
       print("Computing exceedance...")
       if kwargs:
           wm = self.WatchMat_C(**kwargs)
       else:
           wm = self.WatchMat_C()
       print("exceedance computed!")
       ex = xr.where(wm == 1, self._obj, 0)
       ex.name = "Exceedance"
       return ex
    
    def HWD(self,group_by = None, **kwargs):
        print("Computing HWD...")
        if kwargs:
            CHDN = self.getConsHDN(**kwargs)
        else:
            CHDN = self.getConsHDN()
        HWD_tot = CHDN.max(dim=self._time)
        if group_by is None:
            HWD = HWD_tot
        elif isinstance(group_by,str):
            assert group_by in ("month","season","year"), "'group_by' must be 'month','season' or 'year'"    
            if group_by == "month":
                HWD = CHDN.resample({self._time:"MS"}).max(dim=self._time)
            elif group_by == "season":
                HWD = CHDN.resample({self._time:"QS-DEC"}).max(dim=self._time)
            else:
                HWD = CHDN.resample({self._time:"AS"}).max(dim=self._time)
        elif isinstance(group_by,int):
            HWD = CHDN.resample({self._time:str(group_by)+'D'}).max(dim=self._time)
        else:
            raise AttributeError("'group_by' must be 'month','season' or 'year' or an integer of days to group by")
        HWD.name = "HWD"
        print("HWD computed")
        return HWD
    
    def HWA(self, group_by = None, **kwargs):
        print("Computing HWA...")
        if kwargs:
            exceed = self.exceedance(**kwargs)
        else:
            exceed = self.exceedance()
        if group_by is None:
            HWA = exceed.max(dim=self._time)
        elif isinstance(group_by,str):
            assert group_by in ("month","season","year"), "'group_by' must be 'month','season' or 'year'"
            if group_by == "month":
                HWA = exceed.resample({self._time:"MS"}).max(dim=self._time)
            elif group_by == "season":
                HWA = exceed.resample({self._time:"QS-DEC"}).max(dim=self._time)
            else:
                HWA = exceed.resample({self._time:"AS"}).max(dim=self._time)
        elif isinstance(group_by,int):
            HWA = exceed.resample({self._time:str(group_by)+'D'}).max(dim=self._time)
        else:
            raise AttributeError("'group_by' must be 'month','season' or 'year' or an integer of days to group by")
        HWA.name = "HWA"
        print("HWA computed")
        return HWA
    
    def HWA_C(self, group_by = None, **kwargs):
        print("Computing HWA...")
        if kwargs:
            exceed = self.exceedance_C(**kwargs)
        else:
            exceed = self.exceedance_C() 
        if group_by is None:
            HWA = exceed.max(dim=self._time)
        elif isinstance(group_by,str):
            assert group_by in ("month","season","year"), "'group_by' must be 'month','season' or 'year'"
            if group_by == "month":
                HWA = exceed.resample({self._time:"MS"}).max(dim=self._time)
            elif group_by == "season":
                HWA = exceed.resample({self._time:"QS-DEC"}).max(dim=self._time)
            else:
                HWA = exceed.resample({self._time:"AS"}).max(dim=self._time)
        elif isinstance(group_by,int):
            HWA = exceed.resample({self._time:str(group_by)+'D'}).max(dim=self._time)
        else:
            raise AttributeError("'group_by' must be 'month','season' or 'year' or an integer of days to group by")
        HWA.name = "HWA"
        print("HWA computed")
        return HWA
    
    def HWM(self, group_by = None, **kwargs):
        print("Computing HWM...")
        if kwargs:
            exceed = self.exceedance(**kwargs)
        else:
            exceed = self.exceedance()
            
        if group_by is None:
            HWM = exceed.sum(dim=self._time)
        elif isinstance(group_by, str):
            assert group_by in ("month","season","year"), "'group_by' must be 'month','season' or 'year'"
            if group_by == "month":
                HWM = exceed.resample({self._time:"MS"}).sum(dim=self._time)
            elif group_by == "season":
                HWM = exceed.resample({self._time:"QS-DEC"}).sum(dim=self._time)
            else:
                HWM = exceed.resample({self._time:"AS"}).sum(dim=self._time)
        elif isinstance(group_by,int):
            HWM = exceed.resample({self._time:str(group_by)+'D'}).sum(dim=self._time)
        else:
            raise AttributeError("'group_by' must be 'month','season' or 'year' or an integer of days to group by")
        HWM.name = "HWM"
        print("HWM computed")
        return HWM
    
    def HWM_C(self, group_by = None, **kwargs):
        print("Computing HWM...")
        if kwargs:
            exceed = self.exceedance_C(**kwargs)
        else:
            exceed = self.exceedance_C()
            
        if group_by is None:
            HWM = exceed.sum(dim=self._time)
        elif isinstance(group_by, str):
            assert group_by in ("month","season","year"), "'group_by' must be 'month','season' or 'year'"
            if group_by == "month":
                HWM = exceed.resample({self._time:"MS"}).sum(dim=self._time)
            elif group_by == "season":
                HWM = exceed.resample({self._time:"QS-DEC"}).sum(dim=self._time)
            else:
                HWM = exceed.resample({self._time:"AS"}).sum(dim=self._time)
        elif isinstance(group_by,int):
            HWM = exceed.resample({self._time:str(group_by)+'D'}).sum(dim=self._time)
        else:
            raise AttributeError("'group_by' must be 'month','season' or 'year' or an integer of days to group by")
        HWM.name = "HWM"
        print("HWM computed")
        return HWM
        
    def ADHW(self,group_by=None,**kwargs): # average duration of heatwave
        print("Computing ADHW...")
        if kwargs:
            # base_period, window, perc = self._split_kwargs(kwargs)
            hwf = self.HWF(group_by,**kwargs)
            WM1 = self.WatchMat(**kargs)
            if group_by is None:
                WM = WM1.sum(dim=self._time)
            elif isinstance(group_by, str):
                assert group_by in ("month","season","year"), "'group_by' must be 'month','season' or 'year'"
                if group_by == "month":
                    WM = WM1.resample({self._time:"MS"}).sum(dim=self._time)
                elif group_by == "season":
                    WM = WM1.resample({self._time:"QS-DEC"}).sum(dim=self._time)
                else:
                    WM = WM1.resample({self._time:"AS"}).sum(dim=self._time)
            elif isinstance(group_by,int):
                WM = WM1.resample({self._time:str(group_by)+'D'}).sum(dim=self._time)
            else:
                raise AttributeError("'group_by' must be 'month','season' or 'year' or an integer of days to group by")
        else:
            hwf = self.HWF(group_by)
            WM1 = self.WatchMat()
            if group_by is None:
                WM = WM1.sum(dim=self._time)
            elif isinstance(group_by, str):
                assert group_by in ("month","season","year"), "'group_by' must be 'month','season' or 'year'"
                if group_by == "month":
                    WM = WM1.resample({self._time:"MS"}).sum(dim=self._time)
                elif group_by == "season":
                    WM = WM1.resample({self._time:"QS-DEC"}).sum(dim=self._time)
                else:
                    WM = WM1.resample({self._time:"AS"}).sum(dim=self._time)
            elif isinstance(group_by,int):
                WM = WM1.resample({self._time:str(group_by)+'D'}).sum(dim=self._time)
            else:
                raise AttributeError("'group_by' must be 'month','season' or 'year' or an integer of days to group by")
        print("ADHW computed")
        ADHW = np.divide(WM,hwf)
        ADHW.name = "ADHW"
        return ADHW
    
    def ADHW_C(self,group_by=None,**kwargs): # average duration of heatwave
        print("Computing ADHW...")
        if kwargs:
            # base_period, window, perc = self._split_kwargs(kwargs)
            hwf = self.HWF_C(group_by,**kwargs)
            WM1 = self.WatchMat_C(**kargs)
            if group_by is None:
                WM = WM1.sum(dim=self._time)
            elif isinstance(group_by, str):
                assert group_by in ("month","season","year"), "'group_by' must be 'month','season' or 'year'"
                if group_by == "month":
                    WM = WM1.resample({self._time:"MS"}).sum(dim=self._time)
                elif group_by == "season":
                    WM = WM1.resample({self._time:"QS-DEC"}).sum(dim=self._time)
                else:
                    WM = WM1.resample({self._time:"AS"}).sum(dim=self._time)
            elif isinstance(group_by,int):
                WM = WM1.resample({self._time:str(group_by)+'D'}).sum(dim=self._time)
            else:
                raise AttributeError("'group_by' must be 'month','season' or 'year' or an integer of days to group by")
        else:
            hwf = self.HWF_C(group_by)
            WM1 = self.WatchMat_C()
            if group_by is None:
                WM = WM1.sum(dim=self._time)
            elif isinstance(group_by, str):
                assert group_by in ("month","season","year"), "'group_by' must be 'month','season' or 'year'"
                if group_by == "month":
                    WM = WM1.resample({self._time:"MS"}).sum(dim=self._time)
                elif group_by == "season":
                    WM = WM1.resample({self._time:"QS-DEC"}).sum(dim=self._time)
                else:
                    WM = WM1.resample({self._time:"AS"}).sum(dim=self._time)
            elif isinstance(group_by,int):
                WM = WM1.resample({self._time:str(group_by)+'D'}).sum(dim=self._time)
            else:
                raise AttributeError("'group_by' must be 'month','season' or 'year' or an integer of days to group by")
        print("ADHW computed")
        ADHW = np.divide(WM,hwf)
        ADHW.name = "ADHW"
        return ADHW
        
    @staticmethod
    def _split_kwargs(x):
        for opt, value in x.items():
            if opt == "base_period":
                base_period = value
            if opt == "window":
                window = value
            if opt == "perc":
                perc = value
        return base_period, window, perc
    
    
    # @staticmethod
    # def 