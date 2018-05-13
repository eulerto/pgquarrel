#!/bin/sh
set -ex
# you should always use the highest version to avoid keyword quotation problems
PGV=$1
if [ $2 -gt $1 ]; then
	PGV=$2
fi
PGPATH=/usr/lib/postgresql/$PGV/bin
PGPATH1=/usr/lib/postgresql/$1/bin
PGPATH2=/usr/lib/postgresql/$2/bin
# if you change those env variables, don't forget to change it in setup-pg.sh
PGUSER1=quarrel
PGUSER2=quarrel
PGPORT1=9901
PGPORT2=9902
PGDB1=quarrel1
PGDB2=quarrel2

# test needs a relative path
cd pgquarrel/test
# loading quarrel data
$PGPATH1/psql -U $PGUSER1 -p $PGPORT1 -X -f test-server1.sql postgres
$PGPATH2/psql -U $PGUSER2 -p $PGPORT2 -X -f test-server2.sql postgres
# run pgquarrel
LD_LIBRARY_PATH=$HOME/pgquarrel/lib:$LD_LIBRARY_PATH $HOME/pgquarrel/bin/pgquarrel -c test.ini
# apply differences
$PGPATH1/psql -U $PGUSER1 -p $PGPORT1 -X -f /tmp/test.sql $PGDB1
# test again
LD_LIBRARY_PATH=$HOME/pgquarrel/lib:$LD_LIBRARY_PATH $HOME/pgquarrel/bin/pgquarrel -c test2.ini
# comparing dumps
# use same pg_dump version here to avoid diff problems
$PGPATH/pg_dump -s -U $PGUSER1 -p $PGPORT1 -f /tmp/q1.sql $PGDB1
$PGPATH/pg_dump -s -U $PGUSER2 -p $PGPORT2 -f /tmp/q2.sql $PGDB2
diff -u /tmp/q1.sql /tmp/q2.sql
