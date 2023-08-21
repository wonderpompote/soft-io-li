import xarray as xr


def check_all_dims_in_ds_or_da(dims_to_check, da_or_ds):
    """
    Function to check if given dim(s) are in dataArray dimensions
    :param dims_to_check: <str> or Iterable object of <str>, dmensions to check
    :param da_or_ds: <xarray.DataArray> or <xarray.Dataset>
    :return: <bool> True if all dimensions given are in the dataArray or dataset, False otherwise
    """
    # if iterable, check if filled with str
    if hasattr(dims_to_check, '__iter__') and not all(isinstance(dim, str) for dim in dims_to_check):
        raise TypeError(f'Expecting Iterable of strings')
    # if not iterable, check if str
    elif not isinstance(dims_to_check, str):
        raise TypeError(f'Expecting str or Iterable, not {type(dims_to_check)}')
    # check that we're given a dataArray or dataset
    if not isinstance(da_or_ds, xr.Dataset) and not isinstance(da_or_ds, xr.DataArray):
        raise TypeError(f'Expecting <xarray.Dataset> or <xarray.DataArray> object, not {type(da_or_ds)}')
    if isinstance(dims_to_check, str):
        return dims_to_check in da_or_ds.sizes
    else:
        return all(dim in da_or_ds.sizes for dim in dims_to_check)


def apply_operation_on_grouped_da(grouped_by_da, operation, operation_dims=None):
    """
    Function to apply a specific operation on a given grouped by dataArray over given dimensions
    Currently accepted operations:
    - 'sum': will apply sum() function on the grouped by DataArray object over given dimensions
    - 'count': will apply count() function on the grouped by DataArray over given dimensions
    :param grouped_by_da: <xr.core.groupby.DataArrayGroupBy> <!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!>
    :param operation: <str> expecting 'sum' or 'count'
    :param operation_dims: <str> or Iterable of dimension names
    :return: result of the operation
    """
    # check input
    if not isinstance(grouped_by_da, xr.core.groupby.DataArrayGroupBy):
        raise TypeError(f'Expecting <xr.core.groupby.DataArrayGroupBy> not {type(grouped_by_da)}')
    if (operation_dims is not None) and (not check_all_dims_in_ds_or_da(grouped_by_da, operation_dims)):
        raise AttributeError(f'DataArray does not have {operation_dims} in its dimensions')
    # operation
    if operation.lower() == 'sum':
        return grouped_by_da.sum(operation_dims)
    elif operation.lower() == 'count':
        return grouped_by_da.count(operation_dims)
    else:
        raise ValueError(f'{operation} operation not supported, expecting \'sum\' or \'count\'')
