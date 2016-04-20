#!/bin/sh
set -ex
PGV=`echo "pg$1" | sed 's/\.//g'`
PGPATH1=$HOME/$PGV/bin
PGPATH2=$HOME/$PGV/bin
PGPORT1=9901
PGPORT2=9902
cd pgquarrel
# loading quarrel data
$PGPATH1/psql -p $PGPORT1 -X -f test/test-server1.sql postgres
$PGPATH2/psql -p $PGPORT2 -X -f test/test-server2.sql postgres
# run pgquarrel
$HOME/pgquarrel/bin/pgquarrel -c test/test.ini
# apply differences
$PGPATH1/psql -p $PGPORT1 -X -f /tmp/test.sql quarrel1
# test again
$HOME/pgquarrel/bin/pgquarrel -c test/test2.ini
# comparing dumps
$PGPATH1/pg_dump -s -p $PGPORT1 -f /tmp/q1.sql quarrel1
$PGPATH2/pg_dump -s -p $PGPORT2 -f /tmp/q2.sql quarrel2
diff -u /tmp/q1.sql /tmp/q2.sql
