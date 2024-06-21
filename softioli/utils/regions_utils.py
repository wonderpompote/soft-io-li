import xarray as xr

from .common_coords import GEO_REGIONS

def assign_geo_region_to_ds(ds, geo_regions_dict):
    #TODO: recup lon lat avec fonctions de pawel pour savoir si longitude ou latitude

    # get da with same dimension as flight ds filled with default region_name (NONE)
    region_names = xr.full_like(ds["lon"], "NONE", dtype=object)
    # assign region_name to each region_names data point
    #TODO: décider si on garde region names ou region ids
    for region in geo_regions_dict.values():
        condition = (
                (ds["lon"] >= region["LON_MIN"]) &
                (ds["lon"] < region["LON_MAX"]) &
                (ds["lat"] > region["LAT_MIN"]) &
                (ds["lat"] <= region["LAT_MAX"])
        )
        # region_ids = region_ids.where(~condition, other=region["id"])
        region_names = region_names.where(~condition, other=region["REGION_NAME"])
    ds = ds.assign_coords(geo_regions=region_names)
    return ds
