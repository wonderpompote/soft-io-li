import xarray as xr

def apply_operation_on_grouped_da(grouped_by_da, operation, operation_dims=None):
    """
    Function to apply a specific operation on a given grouped by dataArray over given dimensions
    Currently accepted operations:
    - 'sum': will apply sum() function on the DataArrayGroupBy object over given dimensions
    - 'count': will apply count() function on the DataArrayGroupBy over given dimensions
    :param grouped_by_da: <xr.core.groupby.DataArrayGroupBy> <!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!>
    :param operation: <str> expecting 'sum' or 'count'
    :param operation_dims: <str> or Iterable of dimension names
    :return: result of the operation
    """
    # check input
    if not isinstance(grouped_by_da, xr.core.groupby.DataArrayGroupBy):
        raise TypeError(f'Expecting <xr.core.groupby.DataArrayGroupBy> not {type(grouped_by_da)}')
    if (operation_dims is not None) and all(dim in grouped_by_da.sizes for dim in operation_dims):
        raise AttributeError(f'DataArray does not have {operation_dims} in its dimensions')
    # operation
    if operation.lower() == 'sum':
        return grouped_by_da.sum(operation_dims)
    elif operation.lower() == 'count':
        return grouped_by_da.count(operation_dims)
    else:
        raise ValueError(f'{operation} operation not supported, expecting \'sum\' or \'count\'')