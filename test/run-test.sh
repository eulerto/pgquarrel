#!/usr/bin/env bash

# cd .../test
# ./run-test.sh
#

###############################################
# CHANGE STARTS HERE
###############################################
GCOV="/usr/bin/gcov"
LCOV="/usr/bin/lcov"
GENHTML="/usr/bin/genhtml"
VGCMD="/usr/bin/valgrind --leak-check=full --show-leak-kinds=all"

PGUSER1=${PGUSER1:-"quarrel"}
PGUSER2=${PGUSER2:-"quarrel"}

PGPORT1=${PGPORT1:-9901}
PGPORT2=${PGPORT2:-9902}

VERBOSE=${VERBOSE:-"-v -v -v"}
CLEANUP=${CLEANUP:-0}
COVERAGE=${COVERAGE:-0}
VALGRIND=${VALGRIND:-0}
STOPAFTERTESTS=${STOPAFTERTESTS:-1}

CLUSTERPATH=${CLUSTERPATH:-"/tmp"}
###############################################
# CHANGE STOPS HERE
###############################################

PGV1="10"
if [ ! -z $1 ]; then
	PGV1=$1
fi
PGDIR1="pg$PGV1"

PGV2="10"
if [ ! -z "$2" ]; then
	PGV2=$2
fi
PGDIR2="pg$PGV2"

PGPATH1=$HOME/$PGDIR1/bin
PGPATH2=$HOME/$PGDIR2/bin


TESTWD=`pwd`
BASEWD=`dirname $TESTWD`

PGQUARREL="$BASEWD/pgquarrel"

if [ "$3" = "init" ]; then
	if [ -d $CLUSTERPATH/test1 ]; then
		echo "cluster 1 already exists"
		exit 0
	fi
	if [ -d $CLUSTERPATH/test2 ]; then
		echo "cluster 2 already exists"
		exit 0
	fi

	if [ ! -f $PGPATH1/initdb ]; then
		echo "$PGPATH1/initdb is not found"
		exit 0
	fi

	if [ ! -f $PGPATH2/initdb ]; then
		echo "$PGPATH2/initdb is not found"
		exit 0
	fi

	echo "initdb'ing cluster 1..."
	$PGPATH1/initdb -U $PGUSER1 -D $CLUSTERPATH/test1
	echo "initdb'ing cluster 2..."
	$PGPATH2/initdb -U $PGUSER2 -D $CLUSTERPATH/test2

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

vtmp=$(cat $CLUSTERPATH/test1/PG_VERSION | sed 's/\.//')
if [ "$PGV1" != "$vtmp" ]; then
	echo "cluster 1 ($vtmp) mismatch server version ($PGV1)"
	exit 0
fi

vtmp=$(cat $CLUSTERPATH/test2/PG_VERSION | sed 's/\.//')
if [ "$PGV2" != "$vtmp" ]; then
	echo "cluster 2 ($vtmp) mismatch server version ($PGV2)"
	exit 0
fi

if [ ! -f $PGPATH1/pg_ctl ]; then
	echo "$PGPATH1/pg_ctl is not found"
	exit 0
fi

if [ ! -f $PGPATH2/pg_ctl ]; then
	echo "$PGPATH2/pg_ctl is not found"
	exit 0
fi

if [ ! -f $CLUSTERPATH/test1/postmaster.pid ]; then
	$PGPATH1/pg_ctl start -l $CLUSTERPATH/test1/server.log -D $CLUSTERPATH/test1
fi

if [ ! -f $CLUSTERPATH/test2/postmaster.pid ]; then
	$PGPATH2/pg_ctl start -l $CLUSTERPATH/test2/server.log -D $CLUSTERPATH/test2
fi

sleep 2

echo "loading quarrel1..."
$PGPATH1/psql -U $PGUSER1 -p $PGPORT1 -X -f test-server1.sql postgres > /dev/null
echo "loading quarrel2..."
$PGPATH2/psql -U $PGUSER2 -p $PGPORT2 -X -f test-server2.sql postgres > /dev/null

echo "quarrel..."
if [ $VALGRIND -eq 1 ]; then
	if [ ! -f $VGCMD ]; then
		echo "valgrind is not installed"
	else
		$VGCMD $PGQUARREL $VERBOSE -c test.ini
	fi
	exit 0
else
	$PGQUARREL $VERBOSE -c test.ini
fi

echo "applying changes..."
$PGPATH1/psql -U $PGUSER1 -p $PGPORT1 -X -f /tmp/test.sql quarrel1 > /dev/null


echo "test again..."
$PGQUARREL -c test2.ini

echo "comparing dumps..."
if [ $PGV1 -ge $PGV2 ]; then
	$PGPATH1/pg_dump -s -U $PGUSER1 -p $PGPORT1 -f /tmp/q1.sql quarrel1 2> /dev/null
	$PGPATH1/pg_dump -s -U $PGUSER2 -p $PGPORT2 -f /tmp/q2.sql quarrel2 2> /dev/null
else
	$PGPATH2/pg_dump -s -U $PGUSER1 -p $PGPORT1 -f /tmp/q1.sql quarrel1 2> /dev/null
	$PGPATH2/pg_dump -s -U $PGUSER2 -p $PGPORT2 -f /tmp/q2.sql quarrel2 2> /dev/null
fi
#diff -u /tmp/q1.sql /tmp/q2.sql
diff -u <(sort /tmp/q1.sql) <(sort /tmp/q2.sql)

if [ $CLEANUP -eq 1 ]; then
	rm -f /tmp/test.sql
	rm -f /tmp/test2.sql
	rm -f /tmp/q1.sql
	rm -f /tmp/q2.sql
fi

if [ $COVERAGE -eq 1 ]; then
	if [ ! -f $GCOV ]; then
		echo "gcov is not installed"
	elif [ ! -f $LCOV ]; then
		echo "lcov is not installed"
	elif [ ! -f $GENHTML ]; then
		echo "genhtml is not installed"
	else
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
fi

if [ $STOPAFTERTESTS -eq 1 ]; then
	$PGPATH1/pg_ctl stop -w -D $CLUSTERPATH/test1
	$PGPATH1/pg_ctl stop -w -D $CLUSTERPATH/test2
fi

echo "E N D"

exit 0
