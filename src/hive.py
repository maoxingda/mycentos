#! /bin/python3
# -*- coding: utf-8 -*-
import getpass
import os
import re
import shutil
import socket
import sys
import termios
import tty
# from configparser import ConfigParser
from subprocess import call

if __name__ == '__main__':
    # 前提条件
    if len(sys.argv) < 3:
        print('usage: {} <software path> <install path> [--non-interactive]'
              .format(sys.argv[0].split("/")[-1]))
        sys.exit()

    if os.environ['HADOOP_HOME'] == '':
        raise ValueError('请先安装hadoop集群，并设置HADOOP_HOME环境变量')

    inst_root_path = sys.argv[2]
    software_root_path = sys.argv[1]

    if not os.path.exists(inst_root_path):
        os.makedirs(inst_root_path)
        print('make dir: ' + inst_root_path)

    # 是否指定非交互式安装
    if len(sys.argv) > 3 and sys.argv[3] == '--non-interactive':
        non_interactive = True
        print('非交互式安装')

    # 读取mysql用户名和密码
    # config = ConfigParser()
    # config.read('/etc/my.cnf')
    # user = config.get('mysql', 'user')
    # password = config.get('mysql', 'password')
    user = 'root'
    password = '00maoxdMAOXD$$'

    if user != '' and password != '':
        print('the database user: {}, password: {}'.format(user, password))
    else:
        print('no find database user and passwrod configuration, quit installation.')
        sys.exit()

    # 检测指定目录是否已经安装过hive，
    # 如果已安装并且没有指定非交互式安装，则让用户选择是覆盖安装，还是退出该安装程序
    pkg_hive = 'apache-hive-3.1.2'
    hive_install_path = inst_root_path + '/' + pkg_hive
    if os.path.exists(hive_install_path):
        if not non_interactive:
            print(hive_install_path + '已存在，您是想覆盖安装还是退出安装（y/n）：')
            comfirm = yesorno()
            if 'y' == comfirm:
                print('继续安装...')
                shutil.rmtree(hive_install_path)
                print('delete dir: ' + hive_install_path)
            else:
                print('退出安装...')
                sys.exit()
        else:
            shutil.rmtree(hive_install_path)
            print('delete dir: ' + hive_install_path)

    logcall('tar xf {}/{}-bin.tar.gz -C {}'.format(software_root_path, pkg_hive, inst_root_path))
    os.rename('{0}/{1}-bin'.format(inst_root_path, pkg_hive), '{0}/{1}'.format(inst_root_path, pkg_hive))
    print('rename {0}/{1}-bin to '.format(inst_root_path, pkg_hive)
          + '{0}/{1}'.format(inst_root_path, pkg_hive))

    # 设置环境变量HIVE_HOME
    putenv('/home/{}/.bash_profile'.format(getpass.getuser()),
           'HIVE_HOME', '{}/{}'.format(inst_root_path, pkg_hive))
    os.environ['HIVE_HOME'] = '{}/{}'.format(inst_root_path, pkg_hive)
    print('set evironment variable: HIVE_HOME={}'.format('{}/{}'.format(inst_root_path, pkg_hive)))

    # 处理jar包
    logcall('rm $HIVE_HOME/lib/guava-19.0.jar')
    logcall('cp $HADOOP_HOME/share/hadoop/common/lib/guava-27.0-jre.jar $HIVE_HOME/lib')
    logcall('rm $HIVE_HOME/lib/log4j-slf4j-impl-2.10.0.jar')
    logcall('cp {}/mysql-connector-java-5.1.48.jar $HIVE_HOME/lib'.format(software_root_path))

    prevlines = []
    sufflines = []
    with open('{}/etc/hadoop/hadoop-env.sh'.format(os.environ['HADOOP_HOME'])) as cnf:
        readlines = cnf.readlines()
        for i in range(len(readlines)):
            if readlines[i].find('# export HADOOP_CLASSPATH="/some/cool/path/on/your/machine"') != -1:
                prevlines = readlines[:i]
                prevlines.append(readlines[i])
            if readlines[i].find('# Should HADOOP_CLASSPATH be first in the official CLASSPATH?') != -1:
                sufflines = readlines[i:]
            if len(prevlines) != 0 and len(sufflines) != 0:
                break

    prevlines.append('export TEZ_CONF_DIR=$HADOOP_HOME/etc/hadoop\n')
    prevlines.append('export TEZ_JARS={}/tez-0.10.1-SNAPSHOT\n'.format(inst_root_path))
    prevlines.append('export HADOOP_CLASSPATH=$HADOOP_CLASSPATH:${TEZ_CONF_DIR}:${TEZ_JARS}/*:${TEZ_JARS}/lib/*\n\n')
    alllines = prevlines + sufflines
    with open('{}/etc/hadoop/hadoop-env.sh'.format(os.environ['HADOOP_HOME']), 'w') as cnf:
        cnf.writelines(alllines)
    print('set the tez information in hadoop cluster({})'
          .format('{}/etc/hadoop/hadoop-env.sh'.format(os.environ['HADOOP_HOME'])))

    with open('{}/conf/hive-site.xml'.format(os.environ['HIVE_HOME']), 'w') as cnf:
        cnf.write('<?xml version="1.0"?>\n')
        cnf.write('<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>\n')
        cnf.write('<configuration>\n')
        cnf.write('    <property>\n')
        cnf.write('        <name>javax.jdo.option.ConnectionURL</name>\n')
        cnf.write('        <value>jdbc:mysql://{}:3306/metastore?useSSL=false</value>\n'.format(socket.gethostname()))
        cnf.write('    </property>\n')
        cnf.write('    <property>\n')
        cnf.write('        <name>javax.jdo.option.ConnectionDriverName</name>\n')
        cnf.write('        <value>com.mysql.jdbc.Driver</value>\n')
        cnf.write('    </property>\n')
        cnf.write('    <property>\n')
        cnf.write('        <name>javax.jdo.option.ConnectionUserName</name>\n')
        cnf.write('        <value>{}</value>\n'.format(user))
        cnf.write('    </property>\n')
        cnf.write('    <property>\n')
        cnf.write('        <name>javax.jdo.option.ConnectionPassword</name>\n')
        cnf.write('        <value>{}</value>\n'.format(password))
        cnf.write('    </property>\n')
        cnf.write('    <property>\n')
        cnf.write('        <name>hive.metastore.warehouse.dir</name>\n')
        cnf.write('        <value>/user/hive/warehouse</value>\n')
        cnf.write('    </property>\n')
        cnf.write('    <property>\n')
        cnf.write('        <name>hive.metastore.schema.verification</name>\n')
        cnf.write('        <value>false</value>\n')
        cnf.write('    </property>\n')
        cnf.write('    <property>\n')
        cnf.write('        <name>datanucleus.schema.autoCreateAll</name>\n')
        cnf.write('        <value>true</value>\n')
        cnf.write('    </property>\n')
        cnf.write('    <property>\n')
        cnf.write('        <name>hive.metastore.uris</name>\n')
        cnf.write('        <value>thrift://{}:9083</value>\n'.format(socket.gethostname()))
        cnf.write('    </property>\n')
        cnf.write('    <property>\n')
        cnf.write('        <name>hive.server2.thrift.port</name>\n')
        cnf.write('        <value>10000</value>\n')
        cnf.write('    </property>\n')
        cnf.write('    <property>\n')
        cnf.write('        <name>hive.server2.thrift.bind.host</name>\n')
        cnf.write('        <value>{}</value>\n'.format(socket.gethostname()))
        cnf.write('    </property>\n')
        cnf.write('    <property>\n')
        cnf.write('        <name>hive.metastore.event.db.notification.api.auth</name>\n')
        cnf.write('        <value>false</value>\n')
        cnf.write('    </property>\n')
        cnf.write('    <property>\n')
        cnf.write('        <name>hive.execution.engine</name>\n')
        cnf.write('        <value>tez</value>\n')
        cnf.write('    </property>\n')
        cnf.write('</configuration>\n')
    print('set the hive evironment({})'
          .format('{}/conf/hive-site.xml'.format(os.environ['HIVE_HOME'])))

    # 检测指定目录是否已经安装过tez引擎，
    # 如果已安装并且没有指定非交互式安装，则让用户选择是覆盖安装，还是退出该安装程序
    pkg_tez = 'apache-tez-0.9.2'
    tez_install_path = inst_root_path + '/' + pkg_tez
    if os.path.exists(tez_install_path):
        if not non_interactive:
            print(tez_install_path + '已存在，您是想覆盖安装还是退出安装（y/n）：')
            comfirm = yesorno()
            if 'y' == comfirm:
                print('继续安装...')
                shutil.rmtree(tez_install_path)
                print('delete dir: ' + tez_install_path)
            else:
                print('退出安装...')
                sys.exit()
        else:
            shutil.rmtree(tez_install_path)
            print('delete dir: ' + tez_install_path)

    logcall('tar xf {}/{}-bin.tar.gz -C {}'.format(software_root_path, pkg_tez, inst_root_path))
    os.rename('{0}/{1}-bin'.format(inst_root_path, pkg_tez), '{0}/{1}'.format(inst_root_path, pkg_tez))
    print('rename {0}/{1}-bin to '.format(inst_root_path, pkg_tez) + '{0}/tez-0.9.2-3.1.2'.format(inst_root_path))

    with open('{}/{}/conf/tez-site.xml'.format(inst_root_path, pkg_tez), 'w') as cnf:
        cnf.write('<?xml version="1.0" encoding="UTF-8"?>\n')
        cnf.write('<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>\n')
        cnf.write('<configuration>\n')
        cnf.write('    <property>\n')
        cnf.write('        <name>tez.lib.uris</name>\n')
        cnf.write('        <value>${fs.defaultFS}/tez/tez.tar.gz</value>\n')
        cnf.write('    </property>\n')
        cnf.write('    <property>\n')
        cnf.write('        <name>tez.use.cluster.hadoop-libs</name>\n')
        cnf.write('        <value>false</value>\n')
        cnf.write('    </property>\n')
        cnf.write('    <property>\n')
        cnf.write('        <name>tez.history.logging.service.class</name>\n')
        cnf.write('        <value>org.apache.tez.dag.history.logging.ats.ATSHistoryLoggingService</value>\n')
        cnf.write('    </property>\n')
        cnf.write('</configuration>\n')
    print('set the tez evironment({})'
          .format('{}/{}/conf/tez-site.xml'.format(inst_root_path, pkg_tez)))

    logcall('rm {}/{}/lib/slf4j-log4j12-1.7.10.jar'.format(inst_root_path, pkg_tez))

    # 初始化元数据库(需要在配置文件/etc/my.cnf中配置mysql免密登录)
    # logcall('mysql -e "drop database if exists metastore"')
    # logcall('mysql -e "create database metastore"')
    # logcall('schematool -initSchema -dbType mysql >& /dev/null')
    logcall('mysql -e "create database metastore";'
            ' if [[ $? == 0 ]]; then schematool -initSchema -dbType mysql >& /dev/null; fi')

    with open('{}/bin/hivesvr.sh'.format(os.environ['HIVE_HOME']), 'w') as cnf:
        cnf.write('#!/bin/bash\n')
        cnf.write('\n')
        cnf.write('\n')
        cnf.write('HIVE_LOG_DIR=$HIVE_HOME/logs\n')
        cnf.write('META_PID=/tmp/meta.pid\n')
        cnf.write('SERVER_PID=/tmp/server.pid\n')
        cnf.write('\n')
        cnf.write('mkdir -p $HIVE_HOME/logs\n')
        cnf.write('\n')
        cnf.write('function hive_start()\n')
        cnf.write('{\n')
        cnf.write('    nohup hive --service metastore >$HIVE_LOG_DIR/metastore.log 2>&1 &\n')
        cnf.write('    echo $! > $META_PID\n')
        cnf.write('    sleep 8\n')
        cnf.write('    nohup hive --service hiveserver2 >$HIVE_LOG_DIR/hiveserver2.log 2>&1 &\n')
        cnf.write('    echo $! > $SERVER_PID\n')
        cnf.write('}\n')
        cnf.write('\n')
        cnf.write('function hive_stop()\n')
        cnf.write('{\n')
        cnf.write('    if [ -f $META_PID ]\n')
        cnf.write('    then\n')
        cnf.write('        cat $META_PID | xargs kill -9\n')
        cnf.write('        rm $META_PID\n')
        cnf.write('    else\n')
        cnf.write('        echo "Meta PID文件丢失，请手动关闭服务"\n')
        cnf.write('    fi\n')
        cnf.write('    if [ -f $SERVER_PID ]\n')
        cnf.write('    then\n')
        cnf.write('        cat $SERVER_PID | xargs kill -9\n')
        cnf.write('        rm $SERVER_PID\n')
        cnf.write('    else\n')
        cnf.write('        echo "Server2 PID文件丢失，请手动关闭服务"\n')
        cnf.write('    fi\n')
        cnf.write('\n')
        cnf.write('}\n')
        cnf.write('\n')
        cnf.write('case $1 in\n')
        cnf.write('"start")\n')
        cnf.write('    hive_start\n')
        cnf.write('    ;;\n')
        cnf.write('"stop")\n')
        cnf.write('    hive_stop\n')
        cnf.write('    ;;\n')
        cnf.write('"restart")\n')
        cnf.write('    hive_stop\n')
        cnf.write('    sleep 2\n')
        cnf.write('    hive_start\n')
        cnf.write('    ;;\n')
        cnf.write('*)\n')
        cnf.write('    echo Invalid Args!\n')
        cnf.write('    echo "Usage: $(basename $0) start|stop|restart"\n')
        cnf.write('    ;;\n')
        cnf.write('esac\n')
    print('generate the manage hive service script: {}/bin/hivesvr.sh'.format(os.environ['HIVE_HOME']))

    logcall('chmod u+x {}/bin/hivesvr.sh'.format(os.environ['HIVE_HOME']))

    if non_interactive:
        print('重启中...')
        logcall('sync && sudo reboot')
    else:
        print('该软件安装之后需要重新登录（y/n）：')
        if 'y' == yesorno():
            logcall('sync && sudo reboot')
            print('重启中...')
        else:
            print('请手动重启已使安装生效！')
