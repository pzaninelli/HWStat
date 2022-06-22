#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Jun 14 15:55:43 2022

@author: pzaninelli
"""

import cython
import numpy as np
cimport numpy as np
from libc.stdlib cimport malloc, free
from cython.parallel import prange, parallel

ctypedef np.int64_t DTYPE_i_t
ctypedef np.float32_t DTYPE_f_t    

@cython.boundscheck(False)
def fillones(np.ndarray[DTYPE_f_t, ndim=3] arr, 
              np.ndarray[DTYPE_i_t, ndim = 1] time, 
              np.ndarray[DTYPE_i_t, ndim = 1] lat, 
              np.ndarray[DTYPE_i_t, ndim = 1] lon):
    cdef int n = len(time)
    cdef int i, t, la, lo 
            
    for i in range(n):
        t = time[i]
        la = lat[i]
        lo = lon[i]
        arr[t,la,lo] = 1    
        
    return arr



# @cython.boundscheck(False)
# def fillones_p(np.ndarray[DTYPE_f_t, ndim=1] arr, 
#               np.ndarray[DTYPE_i_t, ndim = 1] time):
#     cdef int n = arr.shape[0]
#     cdef int nt = time.shape[0]
#     cdef DTYPE_f_t *buffer = <DTYPE_f_t *>malloc(n * sizeof(DTYPE_f_t))
#     cdef DTYPE_i_t *tbuff = <DTYPE_i_t *>malloc(nt * sizeof(DTYPE_i_t))
#     cdef int i, j, k, it
    
#     # copy arr into buffer
#     for i in range(n):
#         buffer[i] = arr[i]
#     # copy time into tbuff
#     for it in range(nt):
#         tbuff[it] = time[it]
 
#     with nogil, parallel():
#         for j in prange(nt,schedule='dynamic'):            
#             buffer[tbuff[j]] = 1    
    
#     # copy buffer into arr again
#     for k in range(n):
#         arr[i] = buffer[i]
        
#     free(buffer)
#     free(tbuff)    
#     return arr
