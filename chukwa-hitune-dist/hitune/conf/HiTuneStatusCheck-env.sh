#!/bin/bash

# hdfs url of your chukwa cluster, the default value can be extracted from conf/chukwa-collector-conf.xml
HDFS_URL=

# the username of each node in your hadoop cluster. set as root by default
USERNAME=

# the password of each node in your hadoop cluster.
PASSWORD=

# the chukwa's master node
CHUKWA_HOST=

#the regex of the logging time to check
CHECKPOINT=""  

#the possible hadoop's web ports
HADOOP_WEB_PORT=( 50030 50060 50070 50075 )

#agent's log folder
CHKUWA_AGENT_LOG_DIR=
#collector's log folder
CHKUWA_COLLECTOR_LOG_DIR=
#dp's log folder
CHKUWA_DP_LOG_DIR=

#agent's metrics folder
CHUKWA_AGENT_HADOOP_METRIC_DIR=
CHUKWA_AGENT_HITUNE_INSTRUMENT_DIR=
CHUKWA_AGENT_HADOOP_HISTORY_DIR=