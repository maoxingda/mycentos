#! /usr/bin/python3
# -*- coding: utf-8 -*-
import logging
import os
import re
import sys
import termios
import tty

from subprocess import call

LOG_FORMAT = "[%(asctime)s %(levelname)8s] %(message)s"
logging.basicConfig(level=logging.DEBUG, format=LOG_FORMAT)


def logcall(args):
    logging.info(args)
    call(args, shell=True)


def readchar():
    fd = sys.stdin.fileno()
    old_settings = termios.tcgetattr(fd)
    try:
        tty.setraw(sys.stdin.fileno())
        ch = sys.stdin.read(1)
    finally:
        termios.tcsetattr(fd, termios.TCSADRAIN, old_settings)
    return ch


def putenv(interactive: bool, profile_path: str, env_name: str, abspath: str, *relpaths):
    env_name = env_name.upper()

    if re.match(r'^\d', env_name):
        raise ValueError('the evironment variable name can not start with digit：' + env_name)

    select_lines: list[str] = []
    if os.path.exists(profile_path):
        with open(profile_path, 'r') as conf:
            initial_lines = conf.readlines()
            find = False
            for lineno in range(len(initial_lines)):
                what = re.search(rf'^#\s+\b{env_name}\b', initial_lines[lineno])
                if not find and what:
                    find = True
                    if interactive:
                        logging.info(f'[{env_name}={os.environ[env_name]}] already existed'
                                     ', reset or stay the same (y/n)')
                        if 'y' != readchar():
                            logging.info(f'give up setting evironment variable {env_name}')
                            sys.exit()

                if what or initial_lines[lineno].find(f'export {env_name}=') != -1 \
                        or initial_lines[lineno].find(f'${env_name}') != -1:
                    continue
                select_lines.append(initial_lines[lineno])

    if len(select_lines) != 0 and select_lines[-1] != '\n':
        select_lines.append('\n')
        select_lines.append('\n')
    select_lines.append(f'# {env_name}\n')
    select_lines.append(f'export {env_name}={abspath}\n')
    select_lines.append(f'export PATH=$PATH:${env_name}/bin\n')
    for path in relpaths:
        select_lines.append(f'export PATH=$PATH:${env_name}/{path}\n')

    remove_continuous_empty_lines: list[str] = list()
    for i, line in enumerate(select_lines):
        if line == '\n' and len(remove_continuous_empty_lines) != 0 \
                and remove_continuous_empty_lines[-1] == '\n':
            continue
        remove_continuous_empty_lines.append(line)

    with open(profile_path, 'w') as conf:
        conf.writelines(remove_continuous_empty_lines)
