import numpy as np


def histogram_using_pandas(_ds, data_var_name, hist_params):
    """
    Function to calculate histogram of a variable over longitude and latitude using pandas
    @param _ds: <xarray.Dataset> containing the histogram variable
    @param data_var_name: <str> name of the histogram variable
    @param hist_params: <dict> histogram parameters (min, max, step, result_var_name)
    @return: <xarray.DataAray> histogram
    """
    hist_edges = np.arange(start=hist_params['min_bin_edge'],
                           stop=hist_params['max_bin_edge'] + hist_params['step'],
                           step=hist_params['step'])
    _ds = _ds.assign_coords({f'{data_var_name}_edges': hist_edges})
    _ds[f'{data_var_name}_bool'] = _ds[f'{data_var_name}'] < _ds[f'{data_var_name}_edges']
    # convert ds to dataframe to make groupby operations easier
    _df = _ds.to_dataframe()
    _df_grouped = _df.groupby(by=['latitude', 'longitude', f'{data_var_name}_edges'], sort=True)
    _df_cdf = _df_grouped[f'{data_var_name}_bool'].sum().rename(f'{data_var_name}_cdf')
    _da_cdf = _df_cdf.to_xarray()
    # get new variable name or use default value
    if not "res_var_name" in hist_params.keys() or not hist_params['res_var_name']:
        hist_params['res_var_name'] = f'{data_var_name}_hist'
    # get histogram values from cdf and fill nan values with 0
    _da_hist = _da_cdf \
        .diff(f'{data_var_name}_edges') \
        .fillna(0.).astype('i4') \
        .rename(hist_params['res_var_name'])
    # get middle value of bins
    _da_hist = _da_hist.assign_coords({
        f'{data_var_name}_bin': _da_hist[f'{data_var_name}_edges'] - hist_params['step'] / 2
    })
    # swap dims to have hist bin as dim instead of edges + drop edges
    _da_hist = _da_hist.swap_dims({f'{data_var_name}_edges': f'{data_var_name}_bin'}) \
        .drop_vars(f'{data_var_name}_edges')
    return _da_hist


def count_using_pandas(_ds, data_var_name, count_params):
    """
    Function to apply 'count' function on a dataset using pandas to use groupby on mutiple dimensions
    @param _ds: <xarray.Dataset> containing variable to be
    @param data_var_name:
    @param count_params:
    @return:
    """
    # if groupby_dims not indicated --> use default values: latitude and longitude
    if not 'groupby_dims' in count_params.keys() or not count_params['groupby_dims']:
        count_params['groupby_dims'] = ['latitude', 'longitude']
    # convert to datafram to facilitate groupby
    _df = _ds.to_dataframe()
    _df_grouped = _df.groupby(by=count_params['groupby_dims'], sort=True)
    if not "res_var_name" in count_params.keys() or not count_params['res_var_name']:
        count_params['res_var_name'] = f'{data_var_name}_count'
    _df_count = _df_grouped.count().rename(columns={f'{data_var_name}': count_params['res_var_name']})
    return _df_count.to_xarray()
