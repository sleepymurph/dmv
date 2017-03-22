#!/bin/bash

EACH_BYTES=$1
shift
DIR_SPLIT_MAX=$1
shift
DIR_DEPTH_MAX=$1
shift

echo $DIR_SPLIT_MAX
echo $DIR_DEPTH_MAX

for S in $(seq -f "%02.0f" 0 $DIR_SPLIT_MAX)
do
    for D in $(seq -f "%02.0f" 0 $DIR_DEPTH_MAX)
    do
        set -x
        ./filesystem_limit_micro.py \
            --each-file-size=$EACH_BYTES \
            --dir-split=$S \
            --dir-depth=$D \
            $* \
            | tee ${EACH_BYTES}x${S}x${D}--$(date +%F)-$(hostname).txt
        set +x
    done
done
