#!/bin/sh
set -ex
# you should always use the highest version to avoid keyword quotation problems
PGV=$1
if [ $2 -gt $1 ]; then
	PGV=$2
fi
PGPREFIX=/usr/pgsql-$PGV/bin
git clone https://github.com/eulerto/pgquarrel.git
cd pgquarrel
cmake -DCMAKE_INSTALL_PREFIX=$HOME/pgquarrel -DCMAKE_PREFIX_PATH=$PGPREFIX .
make VERBOSE=1 install
