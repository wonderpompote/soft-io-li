import numpy as np
import xarray as xr


# import constants as cts

def apply_operation_on_grouped_da(grouped_by_da, operation, operation_dims=None, operation_args=None):
    """
    Function to apply a specific operation on a given grouped by dataArray over given dimensions
    Currently accepted operations:
    - 'sum': will apply sum() function on the DataArrayGroupBy object over given dimensions
    - 'count': will apply count() function on the DataArrayGroupBy over given dimensions
    - 'histogram': will apply( !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
    :param grouped_by_da: <xr.core.groupby.DataArrayGroupBy>
    :param operation: <str> expecting 'sum' or 'count'
    :param operation_dims: <str> or Iterable of dimension names
    :return: result of the operation
    """
    # check input
    if not isinstance(grouped_by_da, xr.core.groupby.DataArrayGroupBy):
        raise TypeError(f'Expecting <xr.core.groupby.DataArrayGroupBy> not {type(grouped_by_da)}')
    if (operation_dims is not None) and all(dim in grouped_by_da.sizes for dim in operation_dims):
        raise AttributeError(f'DataArray does not have {operation_dims} in its dimensions')
    if operation.lower() == 'histogram' and (
            all(arg in operation_args.keys() for arg in ['bins', 'range_start', 'range_stop'])
            or any(val is None for val in operation_args.values())):
        raise ValueError(f'Expection bins, range_start and range_stop values for histogram')
    # operation
    operation = operation.lower()
    if operation == 'sum':
        return grouped_by_da.sum(operation_dims)
    elif operation == 'count':
        return grouped_by_da.count(operation_dims)
    elif operation == 'histogram':
        return grouped_by_da
    else:
        raise ValueError(f'{operation} operation not supported, expecting \'sum\' or \'count\'')


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
    # get histogram values from cdf
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
