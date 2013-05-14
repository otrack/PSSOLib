#!/bin/bash

bc=`which bc`
EXP_TMP_DIR=/tmp/exp
N_IT=100

function absolute(){
    awk ' { if($1>=0) { print $1} else {print $1*-1 }}'
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
# if [[ $1 == 'P' ]]
# then
#     ./clean $3 "access"
#     wait
#     sleep 5
# fi

# 2 - Launch experiments

client_min=11
client_max=20
client_incr=1

nap_min=0
nap_max=0
nap_incr=10

for nclients in `seq ${client_min} ${client_incr} ${client_max}`
do

    napseq=`seq ${nap_min} ${nap_incr} ${nap_max}`
    # napseq="$((${nclients}*5)) $((${nclients}*10)) $((${nclients}*20)) $((${nclients}*40)) $((${nclients}*80))"
    for nap in ${napseq}
    do

        # 2.1 - Run an  experiment
	objectid=`uuidgen`
	for i in `seq 1 ${nclients}` 
	do
	    ./access $1 $2 $3 ${objectid} ${N_IT} ${nap} &> ${EXP_TMP_DIR}/$i &
	done
	wait 

         # 2.2 Gather results
 	tlat=0
	for i in `seq 1 ${nclients}` 
	do
	    tmp=`grep PID ${EXP_TMP_DIR}/$i | awk  '{sum+=$2;i++} END{print sum/i}'`
	    if [ -n "${tmp}" ]
	    then
		tlat=`echo "${tlat}+${tmp}"| sed 's/E/*10^/g'`
	    else
    		echo "Experiment is corrupted; aborting !"
    		exit 1
	    fi;
	done
	latency=`echo "scale=2;(${tlat})/${nclients}" | ${bc}`

	# FIXME
 	tspread=0
	for i in `seq 1 ${nclients}` 
	do
	    tmp=`grep PID ${EXP_TMP_DIR}/$i | awk  '{sum+=$2;i++} END{print sum/i}'`
	    if [ -n "${tmp}" ]
	    then
		lat=`echo "${tmp}"| sed 's/E/*10^/g'`
		spread=`echo "scale=2;${lat}-${latency}" | ${bc}`
		tspread=`echo "${tspread}+${spread#-}"`
	    else
    		echo "Experiment is corrupted; aborting !"
    		exit 1
	    fi;
	done
 	stddev=`echo "scale=2;(${tspread})/${nclients}" | ${bc}`

	echo -e "${nclients}\t${latency}\t${stddev}\t${nap}"
	
    done
    
done

