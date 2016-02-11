#!/bin/sh

# cd .../test
# ./run-test.sh
#

###############################################
# CHANGE STARTS HERE
###############################################
PGPATH1=/home/euler/pg945/bin
PGPATH2=/home/euler/pg945/bin

GCOV="/usr/bin/gcov"
LCOV="/usr/bin/lcov"
GENHTML="/usr/bin/genhtml"
VGCMD="/usr/bin/valgrind --leak-check=full --show-leak-kinds=all"

PGPORT1=9901
PGPORT2=9902

VERBOSE="-v"
CLEANUP=0
COVERAGE=0
VALGRIND=0

CLUSTERPATH=/tmp
###############################################
# CHANGE STOPS HERE
###############################################

TESTWD=`pwd`
BASEWD=`dirname $TESTWD`

PGQUARREL="$BASEWD/pgquarrel"

if [ "$1" = "init" ]; then
	if [ -d $CLUSTERPATH/test1 ]; then
		echo "cluster 1 already exists"
		exit 0
	fi
	if [ -d $CLUSTERPATH/test2 ]; then
		echo "cluster 2 already exists"
		exit 0
	fi

	echo "initdb'ing cluster 1..."
	$PGPATH1/initdb -D $CLUSTERPATH/test1
	echo "initdb'ing cluster 2..."
	$PGPATH2/initdb -D $CLUSTERPATH/test2

	echo "port = $PGPORT1" >> $CLUSTERPATH/test1/postgresql.conf
	echo "port = $PGPORT2" >> $CLUSTERPATH/test2/postgresql.conf
else
	if [ ! -d $CLUSTERPATH/test1 ]; then
		echo "cluster 1 does not exist"
		exit 0
	fi
	if [ ! -d $CLUSTERPATH/test2 ]; then
		echo "cluster 2 does not exist"
		exit 0
	fi
fi

if [ ! -f $CLUSTERPATH/test1/postmaster.pid ]; then
	$PGPATH1/pg_ctl start -l $CLUSTERPATH/test1/server.log -D $CLUSTERPATH/test1
fi

if [ ! -f $CLUSTERPATH/test2/postmaster.pid ]; then
	$PGPATH2/pg_ctl start -l $CLUSTERPATH/test2/server.log -D $CLUSTERPATH/test2
fi

sleep 2

echo "loading quarrel1..."
$PGPATH1/psql -p $PGPORT1 -X -f test-server1.sql postgres > /dev/null
echo "loading quarrel2..."
$PGPATH2/psql -p $PGPORT2 -X -f test-server2.sql postgres > /dev/null

echo "quarrel..."
if [ $VALGRIND -eq 1 ]; then
	$VGCMD $PGQUARREL $VERBOSE -c test.ini
	exit 0
else
	$PGQUARREL $VERBOSE -c test.ini
fi

echo "applying changes..."
$PGPATH1/psql -p $PGPORT1 -X -f /tmp/test.sql quarrel1 > /dev/null


echo "test again..."
$PGQUARREL -c test2.ini

echo "comparing dumps..."
$PGPATH1/pg_dump -s -p $PGPORT1 -f /tmp/q1.sql quarrel1 2> /dev/null
$PGPATH1/pg_dump -s -p $PGPORT2 -f /tmp/q2.sql quarrel2 2> /dev/null
diff -u /tmp/q1.sql /tmp/q2.sql

if [ $CLEANUP -eq 1 ]; then
	rm -f /tmp/test.sql
	rm -f /tmp/test2.sql
	rm -f /tmp/q1.sql
	rm -f /tmp/q2.sql
fi

if [ $COVERAGE -eq 1 ]; then
	echo "coverage test..."
	mkdir -p $BASEWD/coverage
	cd $BASEWD/src
	for i in *.c; do
		echo "$i $i.gcno $i.gcov.out"
		$GCOV -b -f -p -o $BASEWD/CMakeFiles/pgquarrel.dir/src/$i.gcno $BASEWD/src/$i > $BASEWD/CMakeFiles/pgquarrel.dir/src/$i.gcov.out
	done
	$LCOV -c -d $BASEWD/CMakeFiles/pgquarrel.dir/src -o $BASEWD/CMakeFiles/pgquarrel.dir/src/lcov.info --gcov-tool $GCOV
	$GENHTML --show-details --legend --output-directory=$BASEWD/coverage --title=PostgreSQL --num-spaces=4 $BASEWD/CMakeFiles/pgquarrel.dir/src/lcov.info
fi

echo "E N D"

exit 0
