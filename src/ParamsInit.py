#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Script to load initial parameters
@author: José Manuel Garrido Perez, Pablo G. Zaninelli
@year: 2022
"""
from configparser import ConfigParser
import os
from os.path import exists as path_exists
import os.path
import xarray as xr







class ParamsInit:
    """
    Object to donwload ERA5 Reanalysis
    
    Attributes
    ---------
        __OPTION_T2: list
            Allowed Variable options 
    
    Methods
    -------
        from_file: statistic method
            To get parameters from 'ini' file
        
    """
    
    __OPTION_T2 = ("T2max", "T2min", "T2maxmin")
    
    def __init__(self,
                 file_var = None,
                 var = ["T2max"],
                 main_file_var_name = [],
                 lon_names = "longitude",
                 lat_names = "latitude",
                 time_names = "time",
                 start_year_clim = None,
                 end_year_clim = None,
                 ocean_mask = False,
                 file_mask = None,
                 dir_out = None,
                 file_out = None,
                 percentile_thres = 90,
                 persistence_hw = 3,
                 save_tmp_files = True,
                 op_mon_seas = True,
                 op_rolling = True,
                 window_width = 30):
        
        if not isinstance(file_var, str):        
            raise AttributeError(f"{file_var} should be a 'list' of strings!")
        elif not path_exists(file_var):
            raise FileNotFoundError(f"{file_var} was not found!")
        else:
            self._file_var = file_var 
        
        if len(main_file_var_name)==0:
            self._main_file_var_name = self._get_main_var(self._file_var)
        else:
            self._main_file_var_name = main_file_var_name
        self._dims_names = {} # init _dims_names
        if len(lon_names)==0 or len(lat_names)==0 or len(time_names)==0:
            raise AttributeError("'longitude', 'latitude' and 'time' dimension names should be defined!")
        else:
            self._dims_names["longitude"] = lon_names
            self._dims_names["latitude"] = lat_names
            self._dims_names["time"] = time_names
        
        self._dims_len = {} # init _dims_len
        
        self._dims_len["longitude"] = self._get_dims_len(self._file_var, self._dims_names)["longitude"]
        
        self._dims_len["latitude"] = self._get_dims_len(self._file_var, self._dims_names)["latitude"]
        
        self._dims_len["time"] = self._get_dims_len(self._file_var, self._dims_names)["time"]
        
        if not all(item in self.__OPTION_T2 for item in var) or not all(isinstance(item, str) for item in var):
            raise AttributeError(f"ERROR:: 'var' must be one of {self.__OPTION_T2} as a string")
        else:
            self._var = var
        
        if start_year_clim is None:
            self._start_year_clim = self._get_StartEnd_dates(self._dir_in)["startYear"]
        elif not isinstance(start_year_clim, int):
            raise AttributeError(f"{start_year_clim} should be an 'integer'!")
        else:
            self._start_year_clim = start_year_clim
        
        if end_year_clim is None:
            self._end_year_clim = self._get_StartEnd_dates(self._dir_in)["endYear"]
        elif not isinstance(end_year_clim, int):
            raise AttributeError(f"{end_year_clim} should be an 'integer'!")
        else:
            self._end_year_clim = end_year_clim

        if not isinstance(ocean_mask, bool):
            raise AttributeError("'ocean_mask' should be an 'boolean'!")
        else:
            self._ocean_mask = ocean_mask

        if self._ocean_mask and file_mask is None:
            raise AttributeError("Not valid file to mask the ocean!")
        elif self._ocean_mask and not path_exists(file_mask):
            raise AttributeError(f"'{file_mask}' does not exist!")
        elif not self._ocean_mask: 
            self._file_mask = None 
        else:
            self._file_mask = file_mask 

        if dir_out is None or dir_out == "":
            self._dir_out = os.getcwd()
        elif not path_exists(dir_out):
            raise AttributeError(f"'{dir_out}' does not exists!")
        else:
            self._dir_out = dir_out
        self._file_out = self._set_file_out(file_out,self._var,
                                            self._start_year_clim,
                                            self._end_year_clim)
        self._percentile_thres = percentile_thres
        self._persistence_hw = persistence_hw
        self._save_tmp_files = save_tmp_files 
        self._op_mon_seas = op_mon_seas 
        self._op_rolling = op_rolling 
        self._window_width = window_width
        
    def __str__(self) -> str:
            return f"""
            Parameters of HW computing object:
                FILE IN = {self._file_var}
                Variable = {self._var}
                Start Year = {self._start_year_clim}
                End Year = {self._end_year_clim}
                Load ocean mask? = {self._ocean_mask}
                Mask file = {self._file_mask if self._ocean_mask else ' '}
                DIR OUT = {self._dir_out}
                Percentile threshold = {self._percentile_thres}
                Persistence for HW = {self._persistence_hw}
                Window width = {self._window_width}
            """
    
    @property
    def dir_in(self):
        return self._file_var
    
    @property
    def variable(self):
        return self._var
    
    @property
    def start_year(self):
        return self._start_year_clim

    @property
    def end_year(self):
        return self._end_year_clim    
        
    @property
    def ocean_mask(self):
        return self._ocean_mask
    
    @property
    def file_mask(self):
        return self._file_mask
    
    @property
    def dir_out(self):
        return self._dir_out
    
    @property
    def file_out(self):
        return self._file_out
    
    @property
    def percentile_threshold(self):
        return self._percentile_thres 
    
    @property 
    def persistence_hw(self):
        return self._persistence_hw
    
    @property
    def save_temporal_files(self):
        return self._save_tmp_files
    
    @property
    def monthy_seas_stats(self):
        return self._op_mon_seas
    
    @property
    def rolling_stats(self):
        return self._op_rolling 
    
    @property
    def window_width(self):
        return self._window_width
    
    @property
    def dim_names(self):
        return self._dims_names

    @property
    def dim_len(self):
        return self._dims_len    
        
    @staticmethod 
    def _set_file_out(file_out, variable,start_year,end_year):
        """
        Parameters
        ----------
        file_out : NC File
            File In.
        variable : str
            Variable option.
        start_year : int
        end_year : int

        Returns
        -------
        str
            Fileout name.

        """
        def check_nc(Filename):
            if not Filename.endswith(".nc"):
                return Filename + ".nc"
            else:
                return Filename
            
        if file_out is None or file_out == "":
            return f"{variable[0]}_HW_metrics_{start_year}_{end_year}.nc"
        else:
            return check_nc(os.path.basename(file_out))            
        
    @staticmethod 
    def _get_StartEnd_dates(filename, time_var_name = "time"):
        """
        Get date, day, month and year. 
        Inspired by 
        https://stackoverflow.com/questions/13648774/get-year-month-or-day-from-numpy-datetime64
        """
        data = xr.open_dataset(filename)
        date_min = (data[time_var_name].min()).values
        date_max = (data[time_var_name].max()).values
        startYear = date_min.astype('datetime64[Y]').astype(int) + 1970
        endYear = date_max.astype('datetime64[Y]').astype(int) + 1970
        startMonth = date_min.astype('datetime64[M]').astype(int) % 12 + 1
        endMonth = date_max.astype('datetime64[M]').astype(int) % 12 + 1
        startDay = int(date_min.astype('datetime64[D]').astype(str).split('-')[2])
        endDay = int(date_max.astype('datetime64[D]').astype(str).split('-')[2])
        return {"startDate" : date_min,
                "endDate" : date_max,
                "startYear" : startYear, 
                "endYear": endYear,
                "startMonth" : startMonth,
                "endMonth" : endMonth,
                "startDay" : startDay,
                "endDay" : endDay}
    
    @staticmethod
    def _get_main_var(filename):
        """
        Function to get the main variable names

        Parameters
        ----------
        filename : NC File
            File In.

        Returns
        -------
        list
            Main Variables' names.

        """
        data = xr.open_dataset(filename)
        ndim_variables = {}
        for key, value in data.items():
            ndim_variables[key] = len(data[key].dims)
        
        return [key for key, value in ndim_variables.items() \
                if value == max(ndim_variables.values())]
    
    @staticmethod
    def _get_dims_len(filename, dim_names):
        """
        Function to get the lenght of dimensions

        Parameters
        ----------
        filename : NC File
            File In.
        dim_names : dict
            Dictionary with names 'longitude', 'latitude' and 'time'.

        Returns
        -------
        dict
            lenght of each dimension.

        """
        data = xr.open_dataset(filename)
        return {
            "longitude": len(data[dim_names["longitude"]]),
            "latitude" : len(data[dim_names["latitude"]]),
            "time" : len(data[dim_names["time"]])
            }

    @classmethod
    def from_file(cls, filepath):
        """
        Function to load parameters from externar '.ini' file

        Parameters
        ----------
        filepath : '.ini' File
            File with the parameters information. 
            See https://docs.python.org/3/library/configparser.html

        Returns
        -------
        ParamsInit object
            
        """
        if not path_exists(filepath):
            raise AttributeError(f"the initial parameters file '{filepath}' does not exist!")
        config = ConfigParser()
        config.read(filepath)
        params = config["PARAMETERS"]
        return cls(params["FileIN"],
                   params["Variable"].split(","),
                   params["NameVarNCFile"].split(","),
                   params["LonName"],
                   params["LatName"],
                   params["TimeName"],
                   int(params["StartYearClim"]),
                   int(params["EndYearClim"]),
                   params.getboolean("OceanMask?"),
                   params["OceanMaskFile"],
                   params["DirOUT"],
                   params["FileOut"],
                   int(params["PercentileThreshold"]),
                   int(params["PersistenceThreshold"]),
                   params.getboolean("SaveTempFiles?"),
                   params.getboolean("ComputeMonthlySeasStat?"),
                   params.getboolean("ComputeRollingStats?"),
                   int(params["WindowWidth"])
                   )
    


if __name__ == "__main__":
    parameters = ParamsInit.from_file("/home/pzaninelli/TRABAJO/IGEO/HWDetectionAlgorithm/parameters/params.ini")
