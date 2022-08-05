#!/bin/bash

ETCFILE=$1
GTID=$2
REQUIREID=$3
TS=`date +%s`

if [ $# -ge 4 ]; then
    UUID=$4
fi

# get binlog path
BINLOG=`grep -rn "log-bin" ${ETCFILE} | awk -F "=" '{print $2}'`
SBINLOG=`eval echo "${BINLOG}"`
FBPATH=${SBINLOG%/*}"/flashback_"${REQUIREID}"_"${TS}
LOGFBPATH=${SBINLOG%/*}"/log_flashback_"${REQUIREID}"_"${TS}
NODE_MGR_CONF=`pwd`/../conf/node_mgr.cnf

if [ $# -ge 4 ]; then
    ./util/kunlun_flashback --exclude_gtids=${GTID} --etc_file=${ETCFILE} --node_mgr_cnf_file=${NODE_MGR_CONF} --output_file=${FBPATH} --previous_master_uuid=${UUID} --general_log_file=${LOGFBPATH}
else
    ./util/kunlun_flashback --exclude_gtids=${GTID} --etc_file=${ETCFILE} --node_mgr_cnf_file=${NODE_MGR_CONF} --output_file=${FBPATH} --general_log_file=${LOGFBPATH}
fi