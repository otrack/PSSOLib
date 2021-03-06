#!/bin/bash

EXP_TMP_DIR=/tmp/exp

# 0 - Usage 
if [ $# -ne 3 ]
then
    echo "Usage: $0 <ncrawlers> <rooturl> <depth>"
    exit 1
fi;
ncrawlers=$1
rooturl=$2
depth=$3

# 1 - Clean-up
rm -Rf ${EXP_TMP_DIR}/*
if [ ! -e ${EXP_TMP_DIR} ]
then
    mkdir ${EXP_TMP_DIR}
fi

# 2 - Launch crawling
sed s/\%NCRAWLERS\%/${ncrawlers}/ default.cfg.tmpl > default.cfg
for i in `seq 1 1 ${ncrawlers}`
do
    ./crawl ${rooturl} -d ${depth} ${rooturl} --dot  >  ${EXP_TMP_DIR}/${i}.dot & 
done

wait
# cat ${EXP_TMP_DIR}/*.dot | gvpack -g | dot -Tps -o ${EXP_TMP_DIR}/output.ps
