#! /bin/python3
# -*- coding: utf-8 -*-
import os
import re
import sys

if __name__ == '__main__':
    args = sys.argv[1:]
    env_name = args[0].upper()
    profile_path = '/etc/profile.d/xenv.sh'

    select_lines = []
    if os.path.exists(profile_path):
        with open(profile_path, 'r') as conf:
            initial_lines = conf.readlines()

            for line in initial_lines:
                what = re.search(rf'^#\s+\b{env_name}\b', line)

                if what or line.find(f'export {env_name}=') != -1 \
                        or line.find(f'${env_name}') != -1:
                    continue

                select_lines.append(line)

    if len(select_lines) != 0 and select_lines[-1] != '\n':
        select_lines.append('\n')
        select_lines.append('\n')

    select_lines.append(f'# {env_name}\n')
    select_lines.append(f'export {env_name}={os.getcwd()}\n')
    select_lines.append(f'export PATH=$PATH:${env_name}/bin\n')

    for pathname in args[1:]:
        select_lines.append(f'export PATH=$PATH:${env_name}/{pathname}\n')

    remove_continuous_empty_lines = []
    for line in select_lines:
        if line == '\n' and len(remove_continuous_empty_lines) != 0 \
                and remove_continuous_empty_lines[-1] == '\n':
            continue

        remove_continuous_empty_lines.append(line)

    with open(profile_path, 'w') as conf:
        conf.writelines(remove_continuous_empty_lines)
