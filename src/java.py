#! /bin/python3
# -*- coding: utf-8 -*-
import argparse
import logging
import os
import shutil
import sys
import tarfile
sys.path.append('..')

from src.util.shutil import readchar, putenv, logcall

if __name__ == '__main__':
    parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)

    def_ins_path = '/opt/module'
    def_pkg_path = '/opt/software/jdk-8u212-linux-x64.tar.gz'

    parser.add_argument('-p', '--package-path', default=f'{def_pkg_path}'
                        , help='the software package path to be installed')
    parser.add_argument('-i', '--install-path', default=f'{def_ins_path}'
                        , help='the destination path where the software will be installed to')
    parser.add_argument('-n', '--non-interactive', action='store_true'
                        , help='whether silently install software or not')

    args = parser.parse_args()

    package_path = args.package_path
    install_path = args.install_path
    interactive = not args.non_interactive

    software = os.path.basename(package_path).replace('.tar.gz', '')
    java_home = f'{install_path}/{software}'

    if not os.path.exists(install_path):
        os.makedirs(install_path)

    if os.path.exists(java_home) and interactive:
        logging.info(f'{java_home} already existed, continue or exit (y/n)')
        if 'y' != readchar():
            logging.info('exit install...')
            sys.exit()

    if os.path.exists(java_home):
        shutil.rmtree(java_home)

    logging.info('install...')
    with tarfile.open(package_path, 'r:gz') as tar:
        def is_within_directory(directory, target):
            
            abs_directory = os.path.abspath(directory)
            abs_target = os.path.abspath(target)
        
            prefix = os.path.commonprefix([abs_directory, abs_target])
            
            return prefix == abs_directory
        
        def safe_extract(tar, path=".", members=None, *, numeric_owner=False):
        
            for member in tar.getmembers():
                member_path = os.path.join(path, member.name)
                if not is_within_directory(path, member_path):
                    raise Exception("Attempted Path Traversal in Tar File")
        
            tar.extractall(path, members, numeric_owner=numeric_owner) 
            
        
        safe_extract(tar, install_path)

    putenv(interactive, '/etc/profile.d/xenv.sh', 'JAVA_HOME', java_home)

    logcall('sync')
    logging.info('please relogin shell')
    logging.info(f'{java_home} has been successfully installed')
