#!/bin/bash

source config.sh

if [ $1 = "Z" ]
then
    servers=${zservers}
else
    servers=${cservers}
fi


function stopExp(){

    let e=${#clients[@]}-1
    for i in `seq 0 $e`
    do
        echo "stopping on ${clients[$i]}"
        ${SSHCMD} root@${clients[$i]} "killall -SIGTERM dist_access.sh"
    done

}

trap "echo 'Caught Quit Signal'; stopExp; wait; exit 255" SIGINT SIGTERM

# echo "Cleaning ..."
# ${SSHCMD} root@${clients[0]} "cd ${SCRIPT_DIR} && ${SCRIPT_DIR}/clean" ${servers}
# sleep 1
# wait

echo "Copying files ..."
let e=${#clients[@]}-1
for i in `seq 0 $e`
do
    ${SSHSCP} * root@${clients[$i]}:${SCRIPT_DIR} & 
    ${SSHSCP} ../pssolib/*.py  root@${clients[$i]}:${PSSOLIB_DIR} &
done

wait 

echo "Launching."
let e=${#clients[@]}-1
for i in `seq 0 $e`
do
    ${SSHCMD} root@${clients[$i]} "cd ${SCRIPT_DIR} && ${SCRIPT_DIR}/dist_access.sh" $1 $2 ${servers} &
done

wait 
    
