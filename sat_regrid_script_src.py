import argparse
""" <!> depuis src/ faire $ python -m scripts.<nom_script> sinon ça marche pas à cause des imports jsp quoi"""
from softioli import regrid_sat_files, generate_glm_hourly_regrid_file
from softioli.utils import constants as cts

if __name__ == '__main__':
    parser = argparse.ArgumentParser()

    # nargs donc -f file1 file2 file3 ... PAS -f file1 -f file2 -f file3 ... (ça c'est quand action='append')
    parser.add_argument('-f', '--file_list', required=True, help='List of GLM files to regrid', nargs='+')
    parser.add_argument('--res-path', help='result netcdf file path (mostly used when testing)')
    parser.add_argument('--tests', help='test mode (using default test result path if --res-path arg was forgotten', action='store_true')
    parser.add_argument('--old-glm', help='old GLM file name', action='store_true')

    args = parser.parse_args()

    if args.tests and args.res_path is None:
        args.res_path = '/o3p/patj/test-glm/new_regrid_tests_01-24/'

    print(args)

    regrid_sat_files(path_list=args.file_list, sat_name=cts.GOES_SATELLITE,
                     overwrite=False, result_dir_path=args.res_path, old_glm_filename=args.old_glm)
