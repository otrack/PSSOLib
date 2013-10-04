#!/bin/sh

SSHCMD="ssh -o 'StrictHostKeyChecking no'"

bc=`which bc`
SCRIPT_DIR=/opt/PSSOLIB/scripts #/home/otrack/ALL/myWork/Implementation/PSSOLib/scripts
EXP_TMP_DIR=/tmp/exp
N_IT=2000

client_min=50
client_max=100
client_incr=10

nap_min=0
nap_max=0
nap_incr=10