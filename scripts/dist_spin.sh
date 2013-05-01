#!/bin/bash

bc=`which bc`
EXP_TMP_DIR=/tmp/spinexp
N_IT=100

# 0 - Usage 
if [ $# -ne 3 ]
then
    echo "Usage: $0 <P|Z> <host1:port1>,<host2:port2>,...  <keyspace>"
    exit 1
fi;

# 1 - Clean-up
rm -Rf ${EXP_TMP_DIR}/*
if [ ! -e ${EXP_TMP_DIR} ]
then
    mkdir ${EXP_TMP_DIR}
fi;

# 2 - Launch experiments

client_min=1
client_max=10
client_incr=1

in_nap_min=1
in_nap_max=1
in_nap_incr=1

out_nap_min=0
out_nap_max=50
out_nap_incr=10

for nclients in `seq ${client_min} ${client_incr} ${client_max}`
do

    for in_nap in `seq ${in_nap_min} ${in_nap_incr} ${in_nap_max}`
    do

	for out_nap in `seq ${out_nap_min} ${out_nap_incr} ${out_nap_max}`
	do
	    
	    # 2.0 - Clean-up
	    if [[ $1 == 'P' ]]
	    then
		echo "cleaning"
		./clean $2 $3
	    fi

            # 2.1 - Run an  experiment
	    for i in `seq 1 ${nclients}` 
	    do
		./spin $1 $2 $3 ${N_IT} ${in_nap} ${out_nap} &> ${EXP_TMP_DIR}/$i &
	    done
	    wait 

            # 2.2 Gather results
 	    latency=0
	    for i in `seq 1 ${nclients}` 
	    do
		tmp=`grep PID ${EXP_TMP_DIR}/$i | awk  '{print $2}'`
		if [ -n "${tmp}" ]
		then
		    latency=`echo "${latency}+${tmp}"| sed 's/E/*10^/g'`
		else
    		    echo "Experiment is corrupted; aborting !"
    		    exit 1
		fi;
	    done
	    latency=`echo "scale=2;(${latency})/${nclients}" | ${bc}`
	    echo -e "${nclients}\t${latency}\t${in_nap}\t${out_nap}"
	  
	done

    done
    
done

