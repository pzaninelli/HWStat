from src.ParamsInit import *
from src.indices import *
import os

os.chdir("HWDetectionAlgorithm/")

def set_parameters_from_ini(filename):
    return ParamsInit.from_file(filename)

def save_files(variable = str(),path = "/pool/pool4/pablo/",**args):
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

params = set_parameters_from_ini(filename="../param_HW.ini")
arr = xr.open_dataarray(params.dir_in,chunks={params.dim_names["longitude"]:25,
                                                  params.dim_names["latitude"]:25})
tmax = preproc(arr,params, minmax=params.variable[0], maskfile=params.file_mask)
tmax2 = tmax.HWCNT.copy()
set_HW_parameters(tmax,params)
hwa_seas = tmax.HWCNT.HWA_C(group_by="season")
hwd_seas = tmax.HWCNT.HWD(group_by="season")
hwm_seas = tmax.HWCNT.HWM_C(group_by="season")
adhw_seas = tmax.HWCNT.ADHW_C(group_by="season")
