#!/bin/sh
set -ex
# if you change those env variables, don't forget to change it in run-tests.sh
PGUSER1=quarrel
PGUSER2=quarrel
PGPORT1=9901
PGPORT2=9902
PGDB1=quarrel1
PGDB2=quarrel2

PGPATH1=/usr/lib/postgresql/$1/bin
PGPATH2=/usr/lib/postgresql/$2/bin
PGDATA1=$HOME/data1
PGDATA2=$HOME/data2
mkdir $PGDATA1
mkdir $PGDATA2
$PGPATH1/initdb $PGDATA1
$PGPATH2/initdb $PGDATA2
echo "port = $PGPORT1" >> $PGDATA1/postgresql.conf
echo "unix_socket_directories = '/tmp'" >> $PGDATA1/postgresql.conf
echo "port = $PGPORT2" >> $PGDATA2/postgresql.conf
echo "unix_socket_directories = '/tmp'" >> $PGDATA2/postgresql.conf
$PGPATH1/pg_ctl -w start -D $PGDATA1
$PGPATH2/pg_ctl -w start -D $PGDATA2
$PGPATH1/psql -p $PGPORT1 -c "CREATE ROLE $PGUSER1 SUPERUSER LOGIN" postgres
$PGPATH2/psql -p $PGPORT2 -c "CREATE ROLE $PGUSER2 SUPERUSER LOGIN" postgres
$PGPATH1/psql -p $PGPORT1 -c "CREATE DATABASE $PGDB1 OWNER $PGUSER1" postgres
$PGPATH2/psql -p $PGPORT2 -c "CREATE DATABASE $PGDB2 OWNER $PGUSER2" postgres
