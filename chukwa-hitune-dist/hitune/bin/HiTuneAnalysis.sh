#!/bin/bash
#
################################################################################
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
################################################################################

DIR=`dirname $0`;

. ${DIR}/../conf/HiTune-env.sh

export HITUNE_HOME=`cd "$DIR/../"; pwd`
CHUKWA_HOME="$HITUNE_HOME/.."
cat ${CHUKWA_HOME}/bin/chukwa-config.sh |sed "s|\$0|${CHUKWA_HOME}/bin/chukwa-config.sh|" > ${HITUNE_HOME}/bin/chukwa-config.sh
. ${HITUNE_HOME}/bin/chukwa-config.sh
rm -rf ${HITUNE_HOME}/bin/chukwa-config.sh
. ${CHUKWA_HOME}/conf/chukwa-env.sh


if [ "$HADOOP_CONF_DIR" != "" ]; then
  CLASSPATH=${HADOOP_CONF_DIR}:${CLASSPATH}
fi

# put the chukwa*core*.jar into HDFS
LIBJAR=`ls ${CHUKWA_HOME}/chukwa*core*.jar` 
if [ -e "$LIBJAR" ] ;then
  DC_LIBJAR=file://${LIBJAR}
fi 

#BACKGROUND="false"
APP='hituneanalyzer'
CLASS='hitune.analysis.mapreduce.Analysis'
CLASSPATH=${CLASSPATH}:${HITUNE_HOME}/HiTuneAnalysis-0.9.jar


export CHUKWA_LOG_DIR=${HITUNE_LOG_DIR}
export CHUKWA_PID_DIR=${HITUNE_LOG_DIR}


exec ${JAVA_HOME}/bin/java ${JAVA_OPT} -DHITUNE_HOME=${HITUNE_HOME} -Dtmpjars=${DC_LIBJAR} -Djava.library.path=${JAVA_LIBRARY_PATH} -DCHUKWA_HOME=${CHUKWA_HOME} -DCHUKWA_CONF_DIR=${CHUKWA_CONF_DIR} -DCHUKWA_LOG_DIR=${CHUKWA_LOG_DIR} -DCHUKWA_DATA_DIR=${CHUKWA_DATA_DIR} -DAPP=${APP} -Dlog4j.configuration=chukwa-log4j.properties -classpath ${CHUKWA_CONF_DIR}:${CLASSPATH}:${CHUKWA_CORE}:${HADOOP_JAR}:${COMMON}:${tools} ${CLASS} $OPTS $@ 
