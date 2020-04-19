#! /bin/python3
# -*- coding: utf-8 -*-
import argparse
import sys

sys.path.append('..')

from src.util.shutil import logcall

if __name__ == '__main__':
    parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)

    parser.add_argument('-u', '--user', required=True, help='who installed for?')

    args = parser.parse_args()

    user: str = args.user

    conf = '/etc/pam.d/su'

    with open(conf) as file:
        lines = file.readlines()

    for i, line in enumerate(lines):
        key = '#auth'
        if line.find(key) != -1:
            lines[i] = line.replace(key, 'auth')

    with open(conf, 'w') as file:
        file.writelines(lines)

    logcall(f'usermod -G wheel {user}')
