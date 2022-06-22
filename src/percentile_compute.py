#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu May 26 16:34:55 2022
Percentile computation
@author: pzaninelli
"""
import numpy as np
import xarray as xr

def percentile_computation(darray, win = 15, **kargs):
    if not isinstance(darray, (xr.DataArray, xr.Dataset)):
        raise AttributeError("the array should be a 'xarray.DataArray' or 'xarray.Dataset'!")
    
    