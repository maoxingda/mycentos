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


def yesorno():
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

    select_lines = []
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
                        if 'y' != yesorno():
                            logging.info(f'give up setting evironment variable {env_name}')
                            sys.exit()

                if not what:
                    select_lines.append(initial_lines[lineno])

    select_lines.append(f'# {env_name}\n')
    select_lines.append(f'export {env_name}={abspath}\n')
    select_lines.append(f'export PATH=$PATH:${env_name}/bin\n')
    for path in relpaths:
        select_lines.append(f'export PATH=$PATH:${env_name}/{path}\n')

    with open(profile_path, 'w') as conf:
        conf.writelines(select_lines)


import time


def progress_bar(num):
    j = "#";
    k = "=";
    t = "|/-\\";  # s = " " * (num + 1)

    for i in range(0, num + 1):
        j += "#";
        k += "=";
        s = ("=" * i) + (" " * (num - i))

        # print(int(i/num*100), end='%\r')
        # print('%.2f' % (i/num*100), end='%\r')
        # print('%.2f' % (i*100/num), end='%\r')
        # print('complete percent:', time.strftime("%Y-%m-%d %H:%M:%S", \
        #        time.localtime()), int((i/num)*100), end='%\r')
        # print(str(int(i/num*100)) + '% ' + j + '->', end='\r')
        # print(k + ">" + str(int(i/num*100)), end='%\r')
        # print("[%s]" % t[i%4], end='\r')
        # print("[%s][%s][%.2f" % (t[i%4], k, (i/num*100)), "%]", end='\r')
        print("[%s][%s][%.2f" % (t[i % 4], s, (i / num * 100)), "%]", end='\r')

        time.sleep(0.1)

    print()
