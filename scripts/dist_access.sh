#!/bin/bash

source config.sh

function absolute(){
    awk ' { if($1>=0) { print $1} else {print $1*-1 }}'
}

function stopExp(){
    killall -SIGTERM access 2&>1 > /dev/null &
    exit 0
}

# 0 - Usage 
if [ $# -ne 3 ]
then
    echo "Usage: $0 <P|Z> <spin|cas> <host1:port1>,<host2:port2>,..."
    exit 1
fi;

# 1 - Clean-up
rm -Rf ${EXP_TMP_DIR}/*
if [ ! -e ${EXP_TMP_DIR} ]
then
    mkdir ${EXP_TMP_DIR}
fi

trap "stopExp; wait; exit 255" SIGINT SIGTERM

# 2 - Launch experiments

for nclients in `seq ${client_min} ${client_incr} ${client_max}`
do

    napseq=`seq ${nap_min} ${nap_incr} ${nap_max}`
    # napseq="$((${nclients}*5)) $((${nclients}*10)) $((${nclients}*20)) $((${nclients}*40)) $((${nclients}*80))"
    for nap in ${napseq}
    do

        # 2.1 - Run an  experiment
	for i in `seq 1 ${nclients}` 
	do
	    objectid=`uuidgen`
	    ./access $1 $2 $3 ${objectid} ${N_IT} ${nap} &> ${EXP_TMP_DIR}/$i &
	done
	wait 

         # 2.2 Gather results
 	tlat=0
	rclients=0
	for i in `seq 1 ${nclients}` 
	do
	    tmp=`grep PID ${EXP_TMP_DIR}/$i | grep -v pycassa | awk  '{sum+=$2;i++} END{if(sum!=0){print sum/i}}'`
	    if [ -n "${tmp}" ]
	    then
		let rclients++
		tlat=`echo "${tlat}+${tmp}"| sed 's/E/*10^/g'`
	    else
    		echo -ne "."
	    fi;
	done
	latency=`echo "scale=2;(${tlat})/${rclients}" | ${bc}`

	# FIXME
 	tspread=0
	rclients=0
	for i in `seq 1 ${nclients}` 
	do
	    tmp=`grep PID ${EXP_TMP_DIR}/$i | grep -v pycassa | awk  '{sum+=$2;i++} END{if(sum!=0){ print sum/i}}'`
	    if [ -n "${tmp}" ]
	    then
		let rclients++
		lat=`echo "${tmp}"| sed 's/E/*10^/g'`
		spread=`echo "scale=2;(${lat}-${latency})^2" | ${bc}`
		tspread=`echo "${tspread}+${spread#-}"`		
	    fi;
	done
 	stddev=`echo "scale=2;sqrt((${tspread})/${rclients})" | ${bc}`

	echo -e "${rclients}\t${latency}\t${stddev}\t${nap}"

	# ./isLinearizable
	# isLinearizable=$?	
	# if [[ ${isLinearizable} -eq 1 ]]
	# then
	#     print "Non-linearizable run; stopping."
	#     exit -1
	# fi

	sleep 5
	
    done
    
done

