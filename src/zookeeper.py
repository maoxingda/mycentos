#! /bin/python3
# -*- coding: utf-8 -*-
import argparse
import logging
import os
import re
import shutil
import socket
import sys
import tarfile

sys.path.append('..')

from src.util.shutil import readchar, putenv, logcall, chown

if __name__ == '__main__':
    parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)

    def_ins_path = '/opt/module'
    def_pkg_path = '/opt/software/apache-zookeeper-3.5.7-bin.tar.gz'

    parser.add_argument('-u', '--user-group', required=True
                        , help='who installed for, eg maoxd:hadoop')
    parser.add_argument('-p', '--package-path', default=f'{def_pkg_path}'
                        , help='the software package path to be installed')
    parser.add_argument('-i', '--install-path', default=f'{def_ins_path}'
                        , help='the destination path where the software will be installed to')
    parser.add_argument('-w', '--hosts', required=True
                        , help='the host list where the zookeeper cluster'
                               ', hostname[start-end] or hostname1,hostname2,...')
    parser.add_argument('-n', '--non-interactive', action='store_true'
                        , help='whether silently install software or not')

    args = parser.parse_args()

    hosts = []
    user: str = args.user_group
    package_path = args.package_path
    install_path = args.install_path
    interactive = not args.non_interactive

    if not re.fullmatch(r'\w+:\w+', user):
        logging.error('-u, --user-group only support user:group!')
        sys.exit()

    if re.fullmatch(r'(\w+)(,\w+)*', args.hosts):
        hosts = args.hosts.split(',')

    what = re.fullmatch(r'(\w+)\[(\d+)-(\d+)\]', args.hosts)
    if what:
        beg = what.group(2)
        end = what.group(3)
        for host in range(int(beg), int(end) + 1):
            hosts.append(what.group(1) + str(host))

    if len(hosts) == 0:
        logging.warning('-w --hosts only support hostname[start-end] or hostname1,hostname2,...')
        sys.exit()

    software = os.path.basename(package_path).replace('-bin.tar.gz', '')
    zk_home = f'{install_path}/{software}'

    if not os.path.exists(install_path):
        os.makedirs(install_path)

    if os.path.exists(zk_home) and interactive:
        logging.info(f'{zk_home} already existed, continue or exit (y/n)')
        if 'y' != readchar():
            logging.info('exit install...')
            sys.exit()

    if os.path.exists(zk_home):
        shutil.rmtree(zk_home)

    logging.info('install...')
    with tarfile.open(package_path, 'r:gz') as tar:
        tar.extractall(install_path)

    os.rename(f'{zk_home}-bin', f'{zk_home}')
    shutil.copy2(f'{zk_home}/conf/zoo_sample.cfg', f'{zk_home}/conf/zoo.cfg')
    os.makedirs(f'{zk_home}/zkData')

    ipsuffix = socket.gethostbyname(socket.gethostname()).split(".")[-1]
    with open(f'{zk_home}/zkData/myid', 'a+') as file:
        file.write(f'{ipsuffix}\n')

    with open(f'{zk_home}/conf/zoo.cfg', 'r') as file:
        lines = file.readlines()
        for i in range(len(lines)):
            if lines[i].find('dataDir=/tmp/zookeeper') != -1:
                lines[i] = lines[i].replace('/tmp/zookeeper', f'{zk_home}/zkData')
                break
        lines.append('\n')

    lines.append('# the zookeeper cluster\n')
    with open(f'{zk_home}/conf/zoo.cfg', 'w') as file:
        for host in hosts:
            suffix = re.search(r'[a-zA-Z]+([0-9]+)', host).group(1)
            lines.append(f'server.{suffix}={host}:2888:3888\n')
        file.writelines(lines)

    chown(f'{zk_home}', user.split(':')[0], user.split(':')[1])
    putenv(interactive, '/etc/profile.d/xenv.sh', 'ZK_HOME', zk_home)

    logcall('sync')
    logging.info('please relogin shell')
    logging.info(f'{zk_home} has been successfully installed')
