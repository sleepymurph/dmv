#!/bin/bash

EACH_BYTES=$1
shift
DIR_SPLIT_MAX=$1
shift
DIR_DEPTH_MAX=$1
shift
OTHER_ARGS=$*

run_test() {
    set -x
    ./filesystem_limit_micro.py \
        --each-file-size=$EACH_BYTES \
        --dir-split=$S \
        --dir-depth=$D \
        $OTHER_ARGS \
        | tee ${EACH_BYTES}x${S}x${D}--$(date +%F)-$(hostname).txt
    set +x
}

S=00
D=00
run_test

for S in $(seq -f "%02.0f" 1 $DIR_SPLIT_MAX)
do
    for D in $(seq -f "%02.0f" 1 $DIR_DEPTH_MAX)
    do
        run_test
    done
done
