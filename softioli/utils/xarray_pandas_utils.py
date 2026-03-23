import numpy as np
import pandas as pd
import xarray as xr


def histogram_using_pandas(_df, data_var_name, min_bin_edge, max_bin_edge, step,
                           groupby_dims=None, res_var_name=None):
    """
    Function to calculate the histogram of a variable in a dataframe using pandas.cut to groupby on mutiple dimensions
    @param _df: <pandas.DataFrame> containing the histogram variable
    @param data_var_name: <str> name of the histogram variable and groupby columns
    @param min_bin_edge: <float> left edge of the first bin
    @param max_bin_edge: <float> right edge of the last bin
    @param step: <float> bin width
    @params groupby_dims: <list> list of dimension to groupby (default=['latitude', 'longitude'])
    @params res_var_name: <str> name of the resulting variable (default=<res_var_name>_hist)
    @return: <xarray.Dataset>
    """
    if isinstance(_df, xr.Dataset): # check just in case
        _df = _df.to_dataframe()
    if groupby_dims is None:
        groupby_dims = ['latitude', 'longitude']
    # get new variable name or use default value
    if res_var_name is None:
        res_var_name = f'{data_var_name}_hist'

    # for each bin, get edges and midpoint
    hist_edges = np.arange(start=min_bin_edge, stop=max_bin_edge + step, step=step)
    bin_midpoints = hist_edges[:-1] + step / 2
    bin_col = f'{data_var_name}_bin'

    # only keep groupby_dims and data_var_name columns for next steps (.copy() to avoid modifying og dataframe)
    _df = _df[[*groupby_dims, data_var_name]].copy()
    # pd.cut directly assigns each value to its corresponding bin
    _df[bin_col] = pd.cut(_df[data_var_name], bins=hist_edges, labels=bin_midpoints, right=False)

    # groupby latitude, longitude, bin and count nb of values in each group (observed=False otherwise empty groups are dropped instead of = 0)
    _df_hist = _df.groupby(groupby_dims + [bin_col], sort=True, observed=False)[[data_var_name]] \
                    .count().rename(columns={data_var_name: res_var_name})
    # convert to xarray dataset
    _da_hist = _df_hist.to_xarray()[res_var_name].fillna(0).astype('i4')
    _da_hist[bin_col].attrs['comment'] = f'{min_bin_edge} <= bin <= {max_bin_edge}, bin_step = {step}'

    return _da_hist.to_dataset()


def count_using_pandas(_df, data_var_name, groupby_dims=None, res_var_name=None):
    """
    Function to apply 'count' function on a dataset using pandas to groupby on mutiple dimensions
    @param _df: <pandas.DataFrame> containing variable on which the count operation will be applied
    @param data_var_name: <str> name of the variable to count
    @param groupby_dims: <list> dimensions to groupby (default = ['latitude', 'longitude'])
    @param res_var_name: <str> name of the resulting data variable (default = <data_var_name>_count)
    @return: <xarray.Dataset>
    """
    if isinstance(_df, xr.Dataset): # check just in case
        _df = _df.to_dataframe()
    if groupby_dims is None:
        groupby_dims = ['latitude', 'longitude']
    if res_var_name is None:
        res_var_name = f'{data_var_name}_count'
    _df_count = _df.groupby(by=groupby_dims, sort=True)[[data_var_name]] \
                    .count().rename(columns={data_var_name: res_var_name})
    return _df_count.to_xarray()
