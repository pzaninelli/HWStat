# HWStat
This program computes several statistics of heatwaves.

A heatwave is defined as an uninterrupted period with extreme temperatures above a given (percentile-based) threshold. This percentile, which is calculated locally with a running window centered on each day of year, and the minimum duration (in days) required to define a heatwave are user-defined. The definition can be applied to maximum temperature (hot days), minimum temperature (hot nights), or both (hot days and nights).

The metrics are calculated (accumulated) over a specific time interval (month, season, year or a certain number of days defined by the user) and provided for the entire period. The output (spatial maps for each time interval of the considered period) includes the following parameters: the heatwave frequency (**HWF**, number of days with HW conditions), the longest spell (**HWD**, the longest HW event, in days), the heatwave amplitude (**HWA**, the largest daily temperature exceedance with respect to the considered percentile, in temperature units) and the heatwave magnitude (**HWM**, the cumulative exceedance over the considered time interval, i.e. an aggregated measure of HW frequency, duration and intensity based on the sum of daily temperature exceedances for all HW days, in temperature units).

The input temperature file must be in netcdf format and in hourly frequency.

This program saves the output in netcdf format for the above-mentioned indices and with the selected frequency (monthly, seasonal, annual,user-defined). The spatial domain and period are provided as in the input temperature data.


To pass the arguments to the program, you have to fill in a *.ini* located at

```Bash
HWStat/parameters/params.ini
```
To run

```Bash
python HWStat
```
To see the possible options, use the "-h" option to see the help:

```Bash
python HWStat -h
```

It also allows you to run some of the modules in the Cython version to get the 
best performance. To do this, you have to compile the pyx file which belongs to 
*cmodules* folder via *setup.py*:

```Bash
python setup.py build_ext --inplace
```
and the ".so" file must be generated. A ".so" file (library) for x86_64 and Linux
is located in *cmodules* folder.

To activate the C modules, you have to add the "-C" option:

```Bash
python HWStat -C
```

## The HWCNT Xarray Accesor

This program also included a Xarray accesor (**HWCNT**) that is used by the main
program to compute the indexes. So this accesor could be called independently of 
the main program importing the HWAccesor. For example, to compute the daily maximum
temperature:

```Python
tmax = t2m.HWCNT.dailymax()
```
To transform to Celsius degrees

```Python
t2m = t2m.HWCNT.toCelsius()
```
or to compute the exceedance, the base period to compute the percentile, the window,
the threshold percentile and the threshold of consecutive days defining a heatwave
must be established. In this case we use the period 1950-1980 to compute the 90th 
percentile with a window of 30 days and at least 3 days of consecutive hot days/nights
to define a heatwave: 

```Python
t2m.HWCNT.setParameters(base_period = {"start":1950, "end":1980},
                             window= 30,
                             prec = 90)
t2m.HWCNT.threshold = 3
```

and then we get the exceedance

```Python
ex = t2m.HWCNT.exceedance()
```

