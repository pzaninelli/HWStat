#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
main script
@author: José Manuel Garrido Perez, Pablo G. Zaninelli
@year: 2022
"""
from src.ParamsInit import *
from src.indices import *
from optparse import OptionParser,OptionGroup
from multiprocessing import Pool, cpu_count
from functools import partial
import os
import sys
import time

parser = OptionParser(usage="usage: %prog  [options] ",\
                      version='%prog v0.0.0')
    
# general options
parser.add_option("-q", "--quiet",
                  action="store_false", dest="verbose", default=True,
                  help="don't print status messages to stdout")
# groupal options
query_opts=OptionGroup(parser,'Query Options',"These options control the query mode")

# file in to take the parameters
query_opts.add_option("-p", "--params", dest="file", action="store",
    default="parameters/params.ini", help=".ini file to take the parameters")

# query_opts.add_option("-o", "--output", dest="output", action="store",
#     default = os.getcwd(), help="path of the output files, e.g. /home/CLINT/")

# use C versions of the functions
parser.add_option("-C", "--cmods",
                  action="store_true", dest="cmods", default=False,
                  help="Activate the C version of the functions to increase performance.\
                      The .so file must be compiled to use this option.")

parser.add_option_group(query_opts)
(options, args) = parser.parse_args()


os.chdir(os.path.dirname(__file__)) # change dir to the current file

__TMIN, __TMAX = "T2min", "T2max"

def set_parameters_from_ini(filename = options.file):
    return ParamsInit.from_file(filename)

def save_files(variable = str(),path = options.output,**args):
    assert args, "'args' must be defined!"
    assert path_exists(path), f"{path} does not exist!"
    if not path.endswith('/'): path = path + '/'
    for name, file in args.items():
        file.to_netcdf(path + name + '_' + variable + ".nc")
    
def set_HW_parameters(arr,params):
    arr.HWCNT.setParameters(base_period = {"start":params.start_year,
                                           "end":params.end_year},
                            window = params.window_width,
                            perc = params.percentile_threshold)
    arr.HWCNT.setThres(params.persistence_hw)

def confirmation():
    should_continue = False
    count = 1
    while True:
        if count == 10:
            print("Run the script again!\n")
            break
        option = str(input("Is your request OK: type yes[Y/y] or cancel[C/c]: "))
        if option.upper()=='Y':
            print("Starting process...\n")
            should_continue = True
            break
        elif option.upper()=='C':
            print("Process stopped!")
            break
        else:
            print("Incorrect option\n")
            continue
        count += 1
    return should_continue

def computeIndices(Array,Params, minmaxvar):
    arr = preproc(Array,Params, minmax=minmaxvar, maskfile=Params.file_mask)
    set_HW_parameters(arr, Params)
    if not options.cmods: # C modules not activated
        HWF_yearly, HWF_seasonal, HWF_monthly = HWF(arr)
        HWA_yearly, HWA_seasonal, HWA_monthly = HWA(arr)
        HWM_yearly, HWM_seasonal, HWM_monthly = HWM(arr)
        exceedance = Exceedance(arr)
        persistence = Persistence(arr)
    else: # C modules activated!
        HWF_yearly, HWF_seasonal, HWF_monthly = HWF_C(arr)
        HWA_yearly, HWA_seasonal, HWA_monthly = HWA_C(arr)
        HWM_yearly, HWM_seasonal, HWM_monthly = HWM_C(arr)
        exceedance = Exceedance_C(arr)
        persistence = Persistence(arr)
    save_files_var = partial(save_files, variable = minmaxvar,
                             path = Params.dir_out)
    save_files_var(HWF_YEARLY=HWF_yearly, 
               HWF_SEASONAL=HWF_seasonal, 
               HWF_MONTHLY=HWF_monthly)
    save_files_var(HWA_YEARLY=HWA_yearly, 
               HWA_SEASONAL=HWA_seasonal, 
               HWA_MONTHLY=HWA_monthly)
    save_files_var(HWM_YEARLY=HWM_yearly, 
               HWM_SEASONAL=HWM_seasonal, 
               HWM_MONTHLY=HWM_monthly)
    save_files_var(EXCEEDANCE=exceedance)
    save_files_var(PERSISTENCE=persistence)


def main():
    # reading prameters
    params = set_parameters_from_ini()
    print(params)
    should_continue = confirmation()
    if not should_continue:
        sys.exit(0) 
    arr = xr.open_dataarray(params.dir_in,chunks={params.dim_names["longitude"]:25,
                                                  params.dim_names["latitude"]:25})
    if params.variable[0] in (__TMAX,__TMIN):
        computeIndices(arr, params, params.variable[0])
    else:
        print("There are {} CPUs on this machine ".format(cpu_count()))
        variables = [__TMIN, __TMAX]
        computeIndices_part=partial(computeIndices,arr, params)
        pool = Pool(cpu_count())
        results = pool.map(computeIndices_part, variables)
        pool.close()
        pool.join()
        
        
if __name__ == "__main__":
    start_time = time.time()
    main()
    print("--- %s seconds ---" % (time.time() - start_time))
