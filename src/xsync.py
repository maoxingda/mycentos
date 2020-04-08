#! /usr/bin/python3
# -*- coding: utf-8 -*-
import argparse
import re
import sys

from src.util import logcall

if __name__ == '__main__':
    parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)

    parser.add_argument('-f', '--file', default='/usr/local/bin/xsync'
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

    sync_lines = []
    hspace1 = '    '
    hspace2 = '        '
    hspace3 = '            '
    sync_lines.append(f'{hspace1}for i, directory in enumerate(sys.argv):\n')
    sync_lines.append(f'{hspace2}if i == 0:\n')
    sync_lines.append(f'{hspace3}continue\n\n')

    sync_lines.append(f'{hspace2}if os.path.isfile(directory):\n')
    sync_lines.append(f'{hspace3}print(\'skipping file: \'' + ' + directory' +')\n')
    sync_lines.append(f'{hspace3}continue\n\n')

    sync_lines.append(f'{hspace2}directory = os.path.abspath(directory)\n\n')
    for host in hosts:
        sync_lines.append(f'{hspace2}print(\'==============[{host}]==============\')\n')
        sync_lines.append(f'{hspace2}subprocess.call(f\'ssh ' + host + ' "mkdir -p {directory}"\', shell=True)\n')
        sync_lines.append(f'{hspace2}subprocess.call(f\'' + 'rsync -avz {directory}/ '
                          + host + ':{directory}\', shell=True)\n\n')

    lines = ['#! /usr/bin/python3\n',
             '# -*- coding: utf-8 -*-\n\n',
             'import os\n',
             'import sys\n',
             'import subprocess\n\n',

             'if __name__ == \'__main__\':\n',
             '    if len(sys.argv) < 2:\n',
             '        print(\'usage: xsync.py directory [directory]...\')\n',
             '        sys.exit()\n\n']

    lines.extend(sync_lines)

    with open(args.file, 'w') as file:
        file.writelines(lines)

    logcall(f'chmod +x {args.file}')
