#!/bin/sh
set -ex
# if you change those env variables, don't forget to change it in run-tests.sh
PGV=`echo "pg$1" | sed 's/\.//g'`
PGPATH1=$HOME/$PGV/bin
PGPATH2=$HOME/$PGV/bin
PGDATA1=$HOME/$PGV/data1
PGDATA2=$HOME/$PGV/data2
PGPORT1=9901
PGPORT2=9902
mkdir $PGDATA1
mkdir $PGDATA2
$PGPATH1/initdb $PGDATA1
$PGPATH2/initdb $PGDATA2
echo "port = $PGPORT1" >> $PGDATA1/postgresql.conf
echo "port = $PGPORT2" >> $PGDATA2/postgresql.conf
$PGPATH1/pg_ctl -w start -D $PGDATA1
$PGPATH2/pg_ctl -w start -D $PGDATA2
$PGPATH1/psql -p $PGPORT1 -c "CREATE ROLE quarrel LOGIN" postgres
$PGPATH2/psql -p $PGPORT2 -c "CREATE ROLE quarrel LOGIN" postgres
$PGPATH1/psql -p $PGPORT1 -c "CREATE DATABASE quarrel1 OWNER quarrel" postgres
$PGPATH2/psql -p $PGPORT2 -c "CREATE DATABASE quarrel2 OWNER quarrel" postgres
