#!/bin/sh
set -ex
PGV=`echo "pg$1" | sed 's/\.//g'`
PGPREFIX=$HOME/$PGV
PGBIN=$PGPREFIX/bin
mkdir $PGPREFIX/data1
mkdir $PGPREFIX/data2
$PGBIN/initdb $PGPREFIX/data1
$PGBIN/initdb $PGPREFIX/data2
echo "port = 9901" >> $PGPREFIX/data1/postgresql.conf
echo "port = 9902" >> $PGPREFIX/data2/postgresql.conf
$PGBIN/pg_ctl start -D $PGPREFIX/data1
$PGBIN/pg_ctl start -D $PGPREFIX/data2
$PGBIN/psql -p 9901 -c "CREATE DATABASE quarrel1" postgres
$PGBIN/psql -p 9902 -c "CREATE DATABASE quarrel2" postgres
