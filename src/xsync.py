#! /bin/python3
# -*- coding: utf-8 -*-
import argparse
import re
import sys

from src.util.shutil import logcall

if __name__ == '__main__':
    parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)

    parser.add_argument('-f', '--file', default='/bin/xsync'
                        , help='the file name will be created')
    parser.add_argument('-w', '--hosts', required=True
                        , help='the host list where the file will be sync to'
                               ', hostname[start-end] or hostname1,hostname2,...')

    args = parser.parse_args()

    hosts = []

    if re.fullmatch(r'(\w+)(,\w+)*', args.hosts):
        hosts = args.hosts.split(',')

    what = re.fullmatch(r'(\w+)\[(\d+)-(\d+)\]', args.hosts)
    if what:
        beg = what.group(2)
        end = what.group(3)
        for host in range(int(beg), int(end) + 1):
            hosts.append(what.group(1) + str(host))

    if len(hosts) == 0:
        print('no host was specified')
        sys.exit()

    with open('xsync.template.py', 'r') as file:
        lines = file.readlines()

    for i in range(len(lines)):
        if lines[i].find('hosts = []') != -1:
            lines[i] = lines[i].replace('hosts = []', f'hosts = {str(hosts)}')
            continue

    with open(args.file, 'w') as file:
        file.writelines(lines)

    logcall(f'chmod +x {args.file}')
