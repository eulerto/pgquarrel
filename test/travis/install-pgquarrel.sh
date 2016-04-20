#!/bin/sh
set -ex
PGV=`echo "pg$1" | sed 's/\.//g'`
PGPREFIX=$HOME/$PGV
git clone https://github.com/eulerto/pgquarrel.git
cd pgquarrel
cmake -DCMAKE_INSTALL_PREFIX=$HOME/pgquarrel -DCMAKE_PREFIX_PATH=$PGPREFIX .
make VERBOSE=1 install
