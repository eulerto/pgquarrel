#!/bin/sh
set -ex
PGV=`echo "pg$1" | sed 's/\.//g'`
PGPREFIX=$HOME/$PGV
wget https://ftp.postgresql.org/pub/source/v$1/postgresql-$1.tar.bz2
tar -jxf postgresql-$1.tar.bz2
cd postgresql-$1
./configure --prefix=$PGPREFIX --with-perl && make && make install && cd contrib && make && make install
