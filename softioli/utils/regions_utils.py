import numpy as np
import xarray as xr

from .common_coords import GEO_REGIONS
from .utils_functions import get_lon_lat_varnames

def assign_geo_region_to_ds(ds, geo_regions_dict=GEO_REGIONS):
    """
    Assigns region name to each data point in ds
    :param ds: <xarray.Dataset>
    :param geo_regions_dict: <dict> { <reg_id>: { "REGION_NAME"
    :return:
    """
    lon_varname, lat_varname = get_lon_lat_varnames(ds=ds)
    # get da with same dimension as flight ds filled with default region_name (NONE)
    region_names = xr.full_like(ds[lon_varname], "NONE", dtype=object)
    # assign region_name to each region_names data point
    #TODO: décider si on garde region names ou region ids
    for region in geo_regions_dict.values():
        condition = (
                (ds[lon_varname] >= region["LON_MIN"]) &
                (ds[lon_varname] < region["LON_MAX"]) &
                (ds[lat_varname] > region["LAT_MIN"]) &
                (ds[lat_varname] <= region["LAT_MAX"])
        )
        region_names = region_names.where(~condition, other=region["REGION_NAME"])
    ds = ds.assign_coords(geo_region=region_names)
    return ds


