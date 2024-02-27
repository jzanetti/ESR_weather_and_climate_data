from esr_weather.esr_weather import get_data

analysis_start_datetime = "20231001T00"
analysis_end_datetime = "20231001T00"
output = get_data(
    analysis_start_datetime=analysis_start_datetime,
    analysis_end_datetime=analysis_end_datetime,
    latlon_list=[(-40.0, 175.0), (-42.0, 175.0)],
    data_type="obs",
    bufrsurface_exe_path="rda-bufr-decode-ADPsfc/exe/bufrsurface.x",
)

x = 3
