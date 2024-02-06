import numpy as np


def histogram_using_pandas(_ds, data_var_name, min_bin_edge, max_bin_edge, step,
                           groupby_dims=None, res_var_name=None):
    """
    Function to calculate the histogram of a variable in a dataset using pandas to groupby on mutiple dimensions
    @param _ds: <xarray.Dataset> containing the histogram variable
    @param data_var_name: <str> name of the histogram variable
    @param hist_params: <dict> histogram parameters (min, max, step, result_var_name)
    @return: <xarray.Dataset>
    """
    if groupby_dims is None:
        groupby_dims = ['latitude', 'longitude']
    hist_edges = np.arange(start=min_bin_edge, stop=max_bin_edge + step, step=step)
    _ds = _ds.assign_coords({f'{data_var_name}_edges': hist_edges})
    # True if <data_var_name>_value < <data_var_name>_edge
    _ds[f'{data_var_name}_bool'] = _ds[f'{data_var_name}'] < _ds[f'{data_var_name}_edges']

    # convert ds to dataframe to make groupby operations easier
    _df = _ds.to_dataframe()
    _df_grouped = _df.groupby(by=groupby_dims+[f'{data_var_name}_edges'], sort=True)

    # get cummulative distribution function (sum <data_var_name>_bool)
    _df_cdf = _df_grouped[f'{data_var_name}_bool'].sum().rename(f'{data_var_name}_cdf')
    _da_cdf = _df_cdf.to_xarray()

    # get new variable name or use default value
    if res_var_name is None:
        res_var_name = f'{data_var_name}_hist'

    # get histogram values from cdf and fill nan values with 0
    _da_hist = _da_cdf.diff(f'{data_var_name}_edges') \
        .fillna(0.).astype('i4') \
        .rename(res_var_name)

    # get middle value of bins
    _da_hist = _da_hist.assign_coords({
        f'{data_var_name}_bin': _da_hist[f'{data_var_name}_edges'] - step / 2
    })

    # swap dims to have hist bin as dim instead of edges + drop edges
    _da_hist = _da_hist.swap_dims({f'{data_var_name}_edges': f'{data_var_name}_bin'}) \
        .drop_vars(f'{data_var_name}_edges')
    return _da_hist.to_dataset()


def count_using_pandas(_ds, data_var_name, groupby_dims=None, res_var_name=None):
    """
    Function to apply 'count' function on a dataset using pandas to groupby on mutiple dimensions
    @param _ds: <xarray.Dataset> containing variable on which the count operation will be applied
    @param data_var_name: name of the variable we should apply count on
    @param groupby_dims: dimensions overwhich to groupby (default = ['latitude', 'longitude'])
    @param res_var_name: name of the resulting data variable (default = <data_var_name>_count)
    @return: <xarray.Dataset>
    """
    if groupby_dims is None:
        groupby_dims = ['latitude', 'longitude']
    # convert to datafram to facilitate groupby
    _df = _ds.to_dataframe()
    _df_grouped = _df.groupby(by=groupby_dims, sort=True)
    # if result_var_name is None --> use default name: <var_name>_count
    if res_var_name is None:
        res_var_name = f'{data_var_name}_count'
    _df_count = _df_grouped.count().rename(columns={f'{data_var_name}': res_var_name})
    return _df_count.to_xarray()
