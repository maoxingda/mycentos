#! /bin/python3
# -*- coding: utf-8 -*-

import os
import sys
import subprocess

if __name__ == '__main__':
    args = sys.argv[1:]
    if not args:
        print('usage: [file or path]...')
        sys.exit()

    hosts = []

    for host in hosts:
        print(f'==============[{host}]==============')
        for path in args:
            path = os.path.abspath(path)
            dirname = path
            if os.path.isfile(path):
                dirname = os.path.dirname(path)
            print(path, dirname)

            subprocess.call(f'ssh {host} "mkdir -p {dirname}"', shell=True)
            if os.path.isfile(path):
                subprocess.call(f'rsync -avz {path} {host}:{dirname}', shell=True)
            elif os.path.isdir(path):
                subprocess.call(f'rsync -avz {path}/ {host}:{dirname}', shell=True)
            else:
                print('unkown error')
