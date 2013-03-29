#!/bin/sh

nclients=10

for i in `seq 1 ${nclients}` 
do
    ./spin $1 $2 &
done

wait 
