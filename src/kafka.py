#! /usr/bin/python3
# -*- coding: utf-8 -*-
import argparse
import shutil
import socket
import tarfile

from util import *

if __name__ == '__main__':
    parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)

    def_ins_path = '/opt/module'
    def_pkg_path = '/opt/software/kafka_2.11-0.11.0.0.tgz'

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
    package_path = args.package_path
    install_path = args.install_path
    interactive = not args.non_interactive

    if re.fullmatch(r'(\w+)(,\w+)*', args.hosts):
        hosts = args.hosts.split(',')

    what = re.fullmatch(r'(\w+)\[(\d+)-(\d+)\]', args.hosts)
    if what:
        beg = what.group(2)
        end = what.group(3)
        for host in range(int(beg), int(end) + 1):
            hosts.append(what.group(1) + str(host))

    if len(hosts) == 0:
        print('-w --hosts only support hostname[start-end] or hostname1,hostname2,...')
        sys.exit()

    software = os.path.basename(package_path).replace('.tgz', '')
    kafka_home = f'{install_path}/{software}'

    if not os.path.exists(install_path):
        os.makedirs(install_path)

    if os.path.exists(kafka_home) and interactive:
        logging.info(f'{kafka_home} already existed, continue or exit (y/n)')
        if 'y' != readchar():
            logging.info('exit install...')
            sys.exit()

    if os.path.exists(kafka_home):
        shutil.rmtree(kafka_home)

    logging.info('install...')
    with tarfile.open(package_path, 'r:gz') as tar:
        tar.extractall(install_path)

    with open(f'{kafka_home}/config/server.properties') as file:
        lines = file.readlines()

    brokerid = socket.gethostbyname(socket.gethostname()).split(r'.')[-1]
    for i, line in enumerate(lines):
        if line.find('broker.id=') != -1:
            lines[i] = f'broker.id={brokerid}'
        if line.find('delete.topic.enable=') != -1:
            lines[i].replace('#', '')
        if line.find('zookeeper.connect=') != -1:
            lines[i] = f'zookeeper.connect='
            for j, host in enumerate(hosts):
                if j == 0:
                    lines[i] += f'{host}:2181'
                else:
                    lines[i] += f',{host}:2181'

    with open(f'{kafka_home}/config/server.properties', 'w') as file:
        file.writelines(lines)

    putenv(interactive, '/etc/profile.d/custom-env.sh', 'KAFKA_HOME', kafka_home)

    logcall('sync')
    logging.info('please relogin shell')
    logging.info(f'{kafka_home} has been successfully installed')
