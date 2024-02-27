# ESR_Weather

The **ESR_Weather** Package is developed to help retrieving both historical weather data and future forecasts.

We can get two types of data from the package:

- ``gfs``: This data includes both final analysis (usually can be used as observations, obtained from GDAS) and forecasts. Unlike traditional observations, such as station-based data, the analysis data from GFS is structured in a regular grid with a resolution of approximately 20+ km. The analysis data are not just from station based data, but many of them are from satellites, ships, buoys and aircrafts. This comprehensive coverage spans the entire globe, encompassing both land and ocean. Note that this requires large amount of data to be downloaded therefore it may take quite a while to finish the job.

- ``obs``: This data originates from the Global Telecommunication System (GTS), which includes real-time observations from various sources, including NIWA/MetService. However, the resolution of OBS data can vary significantly and may not be available over oceans or sparsely populated areas.

The difference between ``gfs`` and ``obs`` is shown below:

|                   | GFS                                                                                       | OBS                                                                 |
|-------------------|-------------------------------------------------------------------------------------------|---------------------------------------------------------------------|
| Coverage          | Global                                                                                    | Global                                                              |
| Spatial Resolution| ~20 km everywhere around the world including ocean(may be upgraded to 9km later with EC data) | Varies depending on the station locations, very low spatial coverage in population sparse area |
| Temporal Resolution| 3 hours                                                                                  | 1-3 hours                                                           |
| Upgrade frequency | 6 hours                                                                                   | 6 hours                                                             |
| Data latency      | 3-4 hours                                                                                 | 3-4 hours                                                           |
| Fields            | 100+ different fields including regular meteorological fields                            | Regular meteorological fields (e.g., winds, temperature, humidity and rain) |
| Station types     | All types of observations from satellite, ship, buoy, aircraft to all surface observations. | Mostly surface observations                                         |
| Data type         | Historical observations from 1970s<br>Forecasts for the next 2 weeks                     | Only historical observations                                        |


## Installation:
The package can be installed via:
```
pip install esr_weather
```

In order to get ``obs``, we also need to install ``bufr`` library which is written in _Fortran_. The library can be installed via:

```
make install_bufr
```


## Example:

### Obtain gfs
The package can be simply run as:
```
    from esr_weather.esr_weather import get_data

    analysis_start_datetime = "20231001T00"
    analysis_end_datetime = "20231001T00"
    output = get_data(analysis_start_datetime = analysis_start_datetime, analysis_end_datetime = analysis_end_datetime, latlon_list=[(-40.0, 175.0), (-42.0, 175.0)])
```

The above exmaple quires observations of temperature, winds, humidity and rainfall for the period between 20231001T00 and 20231002T00, at the locations of ``(-40.0, 175.0)`` and ``(-42.0, 175.0)``, where the location is formatted as ```[(lat1, lon1), (lat2, lon2), ...]```. The output looks like:

```
{
    (-40.0, 175.0): {
        datetime(2023, 10, 1, 0, 0): {'Temperature': 179.668, 'Relative humidity': 0.1, 'U component of wind': 19.478, 'V component of wind': -4.301, 'Precipitation rate': 0.0}, 
        datetime(2023, 10, 1, 6, 0): {'Temperature': 183.668, 'Relative humidity': 0.0, 'U component of wind': 17.805, 'V component of wind': -5.401, 'Precipitation rate': 0.0}, 
        datetime(2023, 10, 1, 12, 0): {'Temperature': 179.997, 'Relative humidity': 0.1, 'U component of wind': 12.929, 'V component of wind': -7.814, 'Precipitation rate': 0.0}, 
        ...}
    (-42.0, 175.0): 
        {datetime(2023, 10, 1, 0, 0): {'Temperature': 178.628, 'Relative humidity': 0.1, 'U component of wind': 6.078, 'V component of wind': -7.601, 'Precipitation rate': 0.0}, 
        datetime(2023, 10, 1, 6, 0): {'Temperature': 182.408, 'Relative humidity': 0.0, 'U component of wind': 7.805, 'V component of wind': -12.001, 'Precipitation rate': 0.0}, 
        ...}
}
```

### Obtain obs
The package can be simply run as:
```
    from esr_weather.esr_weather import get_data

    analysis_start_datetime = "20231001T00"
    analysis_end_datetime = "20231001T00"
    output = get_data(analysis_start_datetime = analysis_start_datetime, analysis_end_datetime = analysis_end_datetime, latlon_list=[(-40.0, 175.0), (-42.0, 175.0)], data_type="obs", bufrsurface_exe_path="rda-bufr-decode-ADPsfc/exe/bufrsurface.x")
```
where ``bufrsurface_exe_path`` refers to the ``bufr library`` that is installed by ``make install_bufr``. The output (as the format of ``Dataframe``) looks like:

```
              datetime station_id latitude longitude temperature dewpoint wind_dir wind_spd     rain   distance
    2023-10-01 09:00:00      93339   -39.44    175.65         6.4      5.0    270.0      6.2  -9999.9  83.497754
    2023-09-30 22:00:00       ZMMC    -42.0     175.0        11.9      3.8    340.0     11.8  -9999.9        0.0
```

