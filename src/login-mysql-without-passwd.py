#! /bin/python3
# -*- coding: utf-8 -*-
import argparse
import subprocess
import sys

sys.path.append('..')

from src.util.shutil import logging, logcall

if __name__ == '__main__':
    parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)

    parser.add_argument('-u', '--user', required=True, help='who installed for?')
    parser.add_argument('-p', '--passwd', required=True, help='user password')

    args = parser.parse_args()

    user: str = args.user
    passwd: str = args.passwd

    retcode = subprocess.call('rpm -qa | grep -qE \'mysql|mariadb\'', shell=True)

    if retcode != 0:
        logging.error('please install mysql firstly')
        sys.exit()

    conf = '/etc/my.cnf'

    with open(conf) as file:
        lines = file.readlines()

    find = False
    prefix = []
    suffix = []
    for i, line in enumerate(lines):
        key = '[mysql]'
        if line.find(key) != -1:
            find = True
            prefix = lines[:i]
            suffix = lines[i:]
            break

    if not find:
        prefix = lines
        prefix.append(key + '\n')

    prefix.append(f'user={user}\n')
    prefix.append(f'password={passwd}\n')

    with open(conf, 'w') as file:
        file.writelines(prefix + suffix)

    logcall('systemctl restart mysqld')
