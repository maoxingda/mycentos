#!/usr/bin/env bash

cd /opt/software || exit

yum install -y zlib-devel bzip2-devel openssl-devel ncurses-devel \
sqlite-devel readline-devel tk-devel gcc make libffi-devel

if ! hash wget; then
    yum install -y wget
fi

if [ ! -f "Python-3.7.0.tgz" ]; then
    wget https://www.python.org/ftp/python/3.7.0/Python-3.7.0.tgz
fi

tar -zxvf Python-3.7.0.tgz -C /opt/module

cd /opt/module/Python-3.7.0 || exit

./configure prefix=/usr/local/python3

make && make install

ln -s /usr/local/python3/bin/pip3 /bin/pip3
ln -s /usr/local/python3/bin/python3 /bin/python3
