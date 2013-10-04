#!/bin/bash

source config.sh

clients=("192.168.79.41" "192.168.79.42" "192.168.79.43")

function stopExp(){

    let e=${#clients[@]}-1
    for i in `seq 0 $e`
    do
        echo "stopping on ${clients[$i]}"
        nohup ${SSHCMD} ${clients[$i]} "killall -SIGTERM dist_access.sh" 2&>1 > /dev/null &
    done

}

trap "echo 'Caught Quit Signal'; stopExp; wait; exit 255" SIGINT SIGTERM

let e=${#clients[@]}-1
for i in `seq 0 $e`
do
    ssh ${clients[$i]} "cd ${SCRIPT_DIR} && ${SCRIPT_DIR}/dist_access.sh" $1 $2 $3&
done

wait 
    
