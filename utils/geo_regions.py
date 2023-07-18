import xarray as xr
import numpy as np

def generate_dummy_lon_lat_da(lon_min=-179.75, lon_max=180, lon_step=0.5, lat_min=-89.75, lat_max=90, lat_step=0.5, dummy_value=99.):
    """

    :param lon_min:
    :param lon_max:
    :param lon_step:
    :param lat_min:
    :param lat_max:
    :param lat_step:
    :param dummy_value:
    :return:
    """
    lons = np.arange(lon_min, lon_max, lon_step)
    lats = np.arange(lat_min, lat_max, lat_step)
    dummy_data = np.full(shape=[len(lons),len(lats)], fill_value=dummy_value)
    return xr.DataArray(
        data=dummy_data,
        coords={'longitude': lons, 'latitude': lats},
        dims=['longitude', 'latitude']
    )

def generate_geo_regions_mask_da(GEO_REGIONS, dummy_lon_lat_da=generate_dummy_lon_lat_da(), dummy_value=99.):
    """
    Function to generate a dataArray used as a mask for geograhical regions
    Each lon-lat couple is assigned a geographical region ID (0 to 16 OR 99 if not part of a defined geographical region)
    :param GEO_REGIONS: <dict> containing each geographical region min and max longitude and latitude
    :param dummy_lon_lat_da: dataArray filled
    :param dummy_value:
    :return:
    """
    for r_id in GEO_REGIONS:
        region = GEO_REGIONS[r_id]
        # filter dummy_lon_lat_da to only keep values inside geo_region ( values between [LON_MIN, LON_MAX[ and [LAT_MIN, LAT_MAX[ )
        da_region = dummy_lon_lat_da.where(dummy_lon_lat_da.longitude >= region['LON_MIN']).where(dummy_lon_lat_da.longitude < region['LON_MAX']).where(dummy_lon_lat_da.latitude > region['LAT_MIN']).where(dummy_lon_lat_da.latitude <= region['LAT_MAX'])
        # replace remaining dummy values in da_region (values IN geo region) by region id in original da
        dummy_lon_lat_da = dummy_lon_lat_da.where(da_region != dummy_value, other=r_id)

    return dummy_lon_lat_da


"""
## tests
da = generate_dummy_lon_lat_da()
da = generate_geo_regions_mask_da(GEO_REGIONS, da)
print(da)

da.to_netcdf("./geo_regions_test1.nc")
"""