#!/bin/sh

nclients=2

./clean $1 $2 
wait
for i in `seq 1 ${nclients}` 
do
    ./spin $1 $2 &
done

wait 
