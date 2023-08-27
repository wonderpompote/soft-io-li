def get_min_max_fp_output_date(fp_out_ds):
    """
    Function to get the min and max date from a FLEXPART output dataset
    :param fp_out_ds: <xarray.Dataset> FP output dataset
    :return: <tuple> (<numpy.datetime64>, <numpy.datetime64>)
    """
    return fp_out_ds.spec001_mr.time.min().values, fp_out_ds.spec001_mr.time.max().values
