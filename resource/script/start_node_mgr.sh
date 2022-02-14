#!/bin/bash

cnfpath=`realpath ../conf/node_mgr.cnf`
./node_mgr ${cnfpath}

ps -ef | grep ${cnfpath} | grep -v grep
