from pathlib import Path

def check_str_to_Path(path_to_check):
    """
    Function to check if path_to_check is Path instance. 
    If not, check if path_to_check is a str and convert it to pathlib.Path object
    :path_to_check: should be Path or str
    :return: path_to_check as a pathlib.Path object or raise exception if path_to_check not Path nor str
    """
    if not isinstance(path_to_check, Path):
        if isinstance(path_to_check, str):
            path_to_check = Path(path_to_check)
        else:
            raise TypeError(f'given path should be str or Path object, not {type(path_to_check)}')
    return path_to_check