# HWStat

This program compute statistics of heatwaves. These statistics are: the persistence 
(**PERSISTENCE**) of hot days/nights (depending on whether the computation is on maximum 
or minimum temperature), the exceedance (**EXCEEDANCE**) that contains the 0 for time steps
that do not belong to a heatwave event and the daily temperature for those that do.
the heatwave frequency (**HWF**), the heatwave longest spell (**HWD**), 
daily peak exceedance (**HWA**) and the cumulative exceedence (**HWM**).

The output of this program are netCDF files with each of the above-mentioned indexes
computed calculated at different frequencies: monthly, seasonal and annual.


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

However, the last computation is only 