#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Functions for calculating heat wave indices
@author: José Manuel Garrido Perez, Pablo G. Zaninelli
@year: 2022
"""

from src.HWAccesor import *

__OPT_TEMP = ("T2max","T2min")

def preproc(arr, params, minmax = "T2max", maskfile = None):
    """
    Function to preprocess the input hourly variable

    Parameters
    ----------
    arr : DataArray
        Input Variable.
    minmax : string, optional
        For tmin or tmax, options are 'T2max' or 'T2min'. The default is "T2max".
    maskfile : string, optional
        Filename with Ocean mask. The default is None for no mask.

    Returns
    -------
    arr : DataArray
        Preprocessed DataArray.

    """
    
    assert minmax in __OPT_TEMP, f"'minmax' must be {__OPT_TEMP}"
    arr = arr.HWCNT.toCelsius() # transform to Celsius degree
    if minmax == "T2max": # compute daily maximum or minimum
        arr = arr.HWCNT.dailymax()
    else:
        arr = arr.HWCNT.dailymin()
    arr = arr.HWCNT.convertLon() # convert longitude
    if maskfile is not None: # apply ocean mask?
        mask = xr.open_dataarray(maskfile)
        mask = mask.HWCNT.convertLon()
        arr = arr.HWCNT.applyMask(mask[0])
    arr = arr.sortby(params.dim_names["longitude"])
    return arr

def HWF(arr, base_period = {}, window = None, prec = None, thres = None):
    """
    Compute HeatWave Frequency

    Parameters
    ----------
    arr : DataArray
        Input Array.
    base_period : dict, optional
        Period to compute the percentile. The default is {}.
    window : int, optional
        Window to compute percentile. The default is None.
    prec : int, optional
        prec(th) percentile. The default is None.
    thres : int, optional
        Threshold of persistence in hot days. The default is None.

    Returns
    -------
    hwf_y : DataArray
        Yearly HWF.
    hwf_seas : DataArray
        Seasonal HWF.
    hwf_mon : DataArray
        Monthly HWF.

    """
    if base_period and window is not None and prec is not None and thres is not None:
        arr.HWCNT.setParameters(base_period = base_period,
                             window= window,
                             prec = prec)
        arr.HWCNT.threshold = thres
        arr.HWCNT.computeHDN
    hwf_y = arr.HWCNT.HWF("year")
    hwf_seas = arr.HWCNT.HWF("season")
    hwf_mon = arr.HWCNT.HWF("month")
    return (hwf_y, hwf_seas, hwf_mon)

def HWF_C(arr, base_period = {}, window = None, prec = None, thres = None):
    """
    Compute HeatWave Frequency. C version.

    Parameters
    ----------
    arr : DataArray
        Input Array.
    base_period : dict, optional
        Period to compute the percentile. The default is {}.
    window : int, optional
        Window to compute percentile. The default is None.
    prec : int, optional
        prec(th) percentile. The default is None.
    thres : int, optional
        Threshold of persistence in hot days. The default is None.

    Returns
    -------
    hwf_y : DataArray
        Yearly HWF.
    hwf_seas : DataArray
        Seasonal HWF.
    hwf_mon : DataArray
        Monthly HWF.

    """
    if base_period and window is not None and prec is not None and thres is not None:
        arr.HWCNT.setParameters(base_period = base_period,
                             window= window,
                             prec = prec)
        arr.HWCNT.threshold = thres
        arr.HWCNT.computeHDN
    hwf_y = arr.HWCNT.HWF_C("year")
    hwf_seas = arr.HWCNT.HWF_C("season")
    hwf_mon = arr.HWCNT.HWF_C("month")
    return (hwf_y, hwf_seas, hwf_mon)

def Percentile(arr, base_period = {}, window = None, prec = None, thres = None):
    """
    Compute HeatWave Frequency

    Parameters
    ----------
    arr : DataArray
        Input Array.
    base_period : dict, optional
        Period to compute the percentile. The default is {}.
    window : int, optional
        Window to compute percentile. The default is None.
    prec : int, optional
        prec(th) percentile. The default is None.
    thres : int, optional
        Threshold of persistence in hot days. The default is None.

    Returns
    -------
    percentile : DataArray
        Percentile array

    """
    if base_period and window is not None and prec is not None and thres is not None:
        arr.HWCNT.setParameters(base_period = base_period,
                             window= window,
                             prec = prec)
        arr.HWCNT.threshold = thres
        arr.HWCNT.computeHDN
    percentile = arr.HWCNT.getPercentile()
    return percentile

def Exceedance(arr, base_period = {}, window = None, prec = None, thres = None):
    if base_period and window is not None and prec is not None and thres is not None:
        arr.HWCNT.setParameters(base_period = base_period,
                             window= window,
                             prec = prec)
        arr.HWCNT.threshold = thres
        arr.HWCNT.computeHDN
    ex = arr.HWCNT.exceedance()
    return ex

def Persistence(arr, base_period = {}, window = None, prec = None, thres = None):
    if base_period and window is not None and prec is not None and thres is not None:
        arr.HWCNT.setParameters(base_period = base_period,
                             window= window,
                             prec = prec)
        arr.HWCNT.threshold = thres
        arr.HWCNT.get
    pr = arr.HWCNT.getConsHDN()
    return pr

def Exceedance_C(arr, base_period = {}, window = None, prec = None, thres = None):
    if base_period and window is not None and prec is not None and thres is not None:
        arr.HWCNT.setParameters(base_period = base_period,
                             window= window,
                             prec = prec)
        arr.HWCNT.threshold = thres
        arr.HWCNT.computeHDN
    ex = arr.HWCNT.exceedance_C()
    return ex

def HWA(arr, base_period = {}, window = None, prec = None, thres = None):
    """
    Daily Peak Exceedence

    Parameters
    ----------
    arr : DataArray
        Input Array.
    base_period : dict, optional
        Period to compute the percentile. The default is {}.
    window : int, optional
        Window to compute percentile. The default is None.
    prec : int, optional
        prec(th) percentile. The default is None.
    thres : int, optional
        Threshold of persistence in hot days. The default is None.

    Returns
    -------
    hwa_y : DataArray
        Yearly HWA.
    hwa_seas : DataArray
        Seasonal HWA.
    hwa_mon : DataArray
        Monthly HWA.

    """
    if base_period and window is not None and prec is not None and thres is not None:
        arr.HWCNT.setParameters(base_period = base_period,
                             window= window,
                             prec = prec)
        arr.HWCNT.threshold = thres
        arr.HWCNT.computeHDN
    hwa_y = arr.HWCNT.HWA("year")
    hwa_seas = arr.HWCNT.HWA("season")
    hwa_mon = arr.HWCNT.HWA("month")
    return (hwa_y, hwa_seas, hwa_mon)

def HWA_C(arr, base_period = {}, window = None, prec = None, thres = None):
    """
    Daily Peak Exceedence. C version.

    Parameters
    ----------
    arr : DataArray
        Input Array.
    base_period : dict, optional
        Period to compute the percentile. The default is {}.
    window : int, optional
        Window to compute percentile. The default is None.
    prec : int, optional
        prec(th) percentile. The default is None.
    thres : int, optional
        Threshold of persistence in hot days. The default is None.

    Returns
    -------
    hwa_y : DataArray
        Yearly HWA.
    hwa_seas : DataArray
        Seasonal HWA.
    hwa_mon : DataArray
        Monthly HWA.

    """
    if base_period and window is not None and prec is not None and thres is not None:
        arr.HWCNT.setParameters(base_period = base_period,
                             window= window,
                             prec = prec)
        arr.HWCNT.threshold = thres
        arr.HWCNT.computeHDN
    hwa_y = arr.HWCNT.HWA_C("year")
    hwa_seas = arr.HWCNT.HWA_C("season")
    hwa_mon = arr.HWCNT.HWA_C("month")
    return (hwa_y, hwa_seas, hwa_mon)

def HWM(arr, base_period = {}, window = None, prec = None, thres = None):
    if base_period and window is not None and prec is not None and thres is not None:
        arr.HWCNT.setParameters(base_period = base_period,
                             window= window,
                             prec = prec)
        arr.HWCNT.threshold = thres
        arr.HWCNT.computeHDN
    hwm_y = arr.HWCNT.HWM("year")
    hwm_seas = arr.HWCNT.HWM("season")
    hwm_mon = arr.HWCNT.HWM("month")
    return (hwm_y, hwm_seas, hwm_mon)

def HWM_C(arr, base_period = {}, window = None, prec = None, thres = None):
    if base_period and window is not None and prec is not None and thres is not None:
        arr.HWCNT.setParameters(base_period = base_period,
                             window= window,
                             prec = prec)
        arr.HWCNT.threshold = thres
        arr.HWCNT.computeHDN
    hwm_y = arr.HWCNT.HWM_C("year")
    hwm_seas = arr.HWCNT.HWM_C("season")
    hwm_mon = arr.HWCNT.HWM_C("month")
    return (hwm_y, hwm_seas, hwm_mon)

def ADHW(arr, base_period = {}, window = None, prec = None, thres = None):
    if base_period and window is not None and prec is not None and thres is not None:
        arr.HWCNT.setParameters(base_period = base_period,
                             window= window,
                             prec = prec)
        arr.HWCNT.threshold = thres
        arr.HWCNT.computeHDN
    adhw_y = arr.HWCNT.ADHW("year")
    adhw_seas = arr.HWCNT.ADHW("season")
    adhw_mon = arr.HWCNT.ADHW("month")
    return (adhw_y, adhw_seas, adhw_mon)

def ADHW_C(arr, base_period = {}, window = None, prec = None, thres = None):
    if base_period and window is not None and prec is not None and thres is not None:
        arr.HWCNT.setParameters(base_period = base_period,
                             window= window,
                             prec = prec)
        arr.HWCNT.threshold = thres
        arr.HWCNT.computeHDN
    adhw_y = arr.HWCNT.ADHW_C("year")
    adhw_seas = arr.HWCNT.ADHW_C("season")
    adhw_mon = arr.HWCNT.ADHW_C("month")
    return (adhw_y, adhw_seas, adhw_mon)