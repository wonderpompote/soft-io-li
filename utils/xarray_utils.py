import numpy as np
import xarray as xr

#import constants as cts

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
    if operation.lower() == 'histogram' and (all(arg in operation_args.keys() for arg in ['bins', 'range_start', 'range_stop'])
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



def custom_histogram(x, bins_int):
    return x.groupby_bins(group=np.log10(x), bins=bins_int).count().fillna(0)

def pad_hist_da(hist_da, pad_value=-99):
    padded_values = np.pad(hist_da.values, pad_width=(1,0), mode='constant', constant_values=pad_value)[:1]
    new_hist_da = xr.DataArray(data=padded_values, coords={})
    return np.pad(hist_da.values, pad_width=(1,0), mode='constant', constant_values=pad_value)[:1]