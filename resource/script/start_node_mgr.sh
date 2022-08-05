#!/bin/bash

cnfpath=`realpath ../conf/node_mgr.cnf`

num=`ps -ef | grep ${cnfpath} | grep -v grep | grep -v vim | wc -l`
[ $num -gt 0 ] && {
        echo `pwd`"node_mgr is running, so quit to start again" 
        exit 0
}

./node_mgr ${cnfpath} >../log/std.log 2>&1

ps -ef | grep ${cnfpath} | grep -v grep
