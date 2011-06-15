#!/bin/bash

DIR=`dirname $0`;
. ${DIR}/../conf/HiTune-env.sh

export HITUNE_HOME=`cd "$DIR/../"; pwd`
CHUKWA_HOME="$HITUNE_HOME/.."

. $CHUKWA_HOME/conf/chukwa-env.sh
. $HADOOP_CONF_DIR/hadoop-env.sh

if [ "$HADOOP_LOG_DIR" = "" ]; then
       HADOOP_LOG_DIR=$HADOOP_HOME/logs
fi
. $HITUNE_HOME/conf/HiTuneStatusCheck-env.sh




# This file is to check the HiTune's run-time status
  
PRIVATE_FOUR_SPACEPRIVATE_TAB="    "
PRIVATE_TAB="\t"
PRIVATE_DOTS="..."
PRIVATE_TMP_LOG=/tmp/HiTuneStatusCheck.out
PRIVATE_TMP_ERR=/tmp/HiTuneStatusCheck.log
PRIVATE_INTERVAL=50000;
PRIVATE_CURRENT_DAY=`date +%Y-%m-%d`
RRIVATE_CURRENT_HOUR=`date +%k`
PRIVATE_REPORT=/tmp/HiTuneStatusCheck.report
PRIVATE_TMP_DIR=/tmp/HiTuneStatusCheck


usage(){
    echo -e
    echo -e "$0 a tool to check the status of the Chukwa agents, collectors and data processors. The generated status report helps to identify whether there are any issues in Chukwa.  The detailed configuration file can be found in \"conf/HiTuneStatusCheck-env.sh\"."
    
	echo -e  
	echo -e  "After the check, the tool generates a report at \"/tmp/HiTuneStatusCheck.report\". The report contains 3 sections, for agents, collectors and data processors respectively. In each section, it displays the following content:"
	
	echo -e  "${PRIVATE_FOUR_SPACEPRIVATE_TAB}COMPONENT${PRIVATE_TAB}agent, collector or data-processor (including demux and postprocessing)"
	echo -e  "${PRIVATE_FOUR_SPACEPRIVATE_TAB}HOST/IP${PRIVATE_TAB}the hostname or ip address of the machine that is checked"
    echo -e  "${PRIVATE_FOUR_SPACEPRIVATE_TAB}SERVICE_STATUS${PRIVATE_TAB}the running status of the service ¨C either ON or OFF. For agent-node, it also dumps the number of running adaptors" 
	echo -e  "${PRIVATE_FOUR_SPACEPRIVATE_TAB}WARN_NUM${PRIVATE_TAB}the number of WARN lines in the log file of the corresponding component" 
	echo -e  "${PRIVATE_FOUR_SPACEPRIVATE_TAB}ERROR_NUM${PRIVATE_TAB}the number of ERROR lines in log file in the corresponding component"
    echo -e  "${PRIVATE_FOUR_SPACEPRIVATE_TAB}STATUS${PRIVATE_TAB}the overall status of the components, OK, WARN, or ERROR"
	echo -e  "-----------------------------------------------------------------------"
	echo -e  "In addition, for agent nodes, the tool dumps the number of hadoop-metrics, historylog, and HiTuneInstrument files under \"Moredetails\"; for data-processor, it dumps the list of files in the Chukwa repos folder, and highlights those xxxInError folder with \"*\" at the end of that folder name."
	
    
}

checkargs(){
if [ "$HDFS_URL" = "" ];then
    HDFS_URL=`grep hdfs $CHUKWA_HOME/conf/chukwa-collector-conf.xml|grep value|sed "s|.*>\(.*\)</.*|\1|g"`
fi

if [ "$USERNAME" = "" ]; then
     USERNAME=root
fi
if [ "$PASSWORD" = "" ]; then
    (echo exit)|`_expect` > $PRIVATE_TMP_ERR
	test $? = 0 && \
    echo "[ERROR]No password. please configure the password in $HITUNE_HOME/conf/HiTuneStatusChecker-env.sh" && \
	exit 1
fi

if [ "$HADOOP_WEB_PORT" = "" ]; then
    HADOOP_WEB_PORT=( 50030 50060 50070 50075 )
fi

if [ "$CHKUWA_AGENT_LOG_DIR" = "" ]; then
    CHKUWA_AGENT_LOG_DIR=${CHUKWA_LOG_DIR}
fi

if [ "$CHKUWA_COLLECTOR_LOG_DIR" = "" ]; then
    CHKUWA_COLLECTOR_LOG_DIR=${CHUKWA_LOG_DIR}
fi
if [ "$CHKUWA_DP_LOG_DIR" = "" ]; then
    CHKUWA_DP_LOG_DIR=${CHUKWA_LOG_DIR}
fi

if [ "$CHUKWA_AGENT_HADOOP_METRIC_DIR" = "" ]; then
    CHUKWA_AGENT_HADOOP_METRIC_DIR=${CHKUWA_AGENT_LOG_DIR}/metrics
fi

if [ "$CHUKWA_AGENT_HITUNE_INSTRUMENT_DIR" = "" ]; then
    CHUKWA_AGENT_HITUNE_INSTRUMENT_DIR=${CHUKWA_HOME}/hitune_output
fi
if [ "$CHUKWA_AGENT_HADOOP_HISTORY_DIR" = "" ]; then
    CHUKWA_AGENT_HADOOP_HISTORY_DIR=${HADOOP_LOG_DIR}/history/done
fi

}

################################################################
# util funcs
################################################################
waiting(){
   PID=$1
   if [ $PID = "" ]; then
        echo "[ERROR]missing process id" 1>&2 ;
        exit
   fi
   while [ "`ps ax|grep $PID|grep -v grep|awk '{print $1}'`" = "$PID" ]; do
        for j in '-' '\\' '|' '/';  do
        echo -ne "\033[1D$j"
        usleep $PRIVATE_INTERVAL
    done  
   done
}

dumpconf(){
    component="$1";
    LOG_DIR="$2"
    
    if [ "$LOG_DIR" = "" ]; then
        LOG_DIR=${CHUKWA_LOG_DIR}
    fi
    
    echo -en "${PRIVATE_FOUR_SPACEPRIVATE_TAB}${PRIVATE_FOUR_SPACEPRIVATE_TAB}${component}_log_file: ${PRIVATE_DOTS}${PRIVATE_DOTS} "
    echo "${LOG_DIR}/${component}.log"

    if [ "`echo ${LOG_DIR}|sed "s|${HADOOP_LOG_DIR}|_HITTEN_|g"|grep _HITTEN_`" != "" ]; then
        echo -en "${PRIVATE_FOUR_SPACEPRIVATE_TAB}${PRIVATE_FOUR_SPACEPRIVATE_TAB}${component}_log_file_web_folder: ${PRIVATE_DOTS}${PRIVATE_DOTS} "
        echo `echo  ${LOG_DIR}|sed "s|${HADOOP_LOG_DIR}||g"`"/${component}.log"
    fi
    
}

########################
# for each node
########################
# 1. check service is ready
# 2. check service logs
# 3. check raw data is ready
# 4. check any Error in repos folder 
check(){
   
    components=`echo $1|sed "s|,| |g"`
    hosts=`echo $2|sed "s|,| |g"`
    
    
    echo -en "${PRIVATE_FOUR_SPACEPRIVATE_TAB}$components "
    echo "["`date`"]"
    echo 
    header=( COMPONENT HOST/IP SERVICE_STATUS WARN_NUM ERROR_NUM STATUS )
    echo "${PRIVATE_FOUR_SPACEPRIVATE_TAB}${PRIVATE_FOUR_SPACEPRIVATE_TAB}"${header[@]}
    for _h in $hosts; do
        for _c in $components; do
            checkstatus $_h $_c
        done
    done
    
    
    echo
    echo "${PRIVATE_FOUR_SPACEPRIVATE_TAB}${PRIVATE_FOUR_SPACEPRIVATE_TAB}Moredetails:"
    for _h in $hosts; do
        for _c in $components; do
            moredetails $_h $_c
        done
    done
    
    echo 
    echo 
    echo
}

printStatus(){
    status=$1
    if [ $status = "ERROR" ]; then
        echo -ne "[\e[31;40m$status\e[0m]"
    else
        echo -ne "[\e[32;40m$status\e[0m]"
    fi
}


count(){
    keywords=`echo $1|sed "s|,| |g"`
    targetfile=$2
    
    if [ "$CHECKPOINT" = "" ]; then
        CHECKPOINT="$PRIVATE_CURRENT_DAY $RRIVATE_CURRENT_HOUR.*"
    fi
    for word in $keywords; do
        CNT=`grep -E $CHECKPOINT $targetfile|grep -E "$word"|wc -l`
        echo -en $CNT
        echo -en " "
    done
    
}

_expect(){
    EXPECT=`which expect`
    if [ $? = 0 ]; then
        echo -n $EXPECT
		return 0
    else 
        echo -e "[Error]No expect" 1>&2
        exit 1;
    fi
}

_wget(){
    WGET=`which wget`
    if [ $? = 0 ]; then
        echo -n "$WGET -T 10 -t 3"
		return 0
    else 
        echo -e "[Error]No wget" 1>&2
        exit 1;
    fi
}

checkservice(){
    host=`echo $1|awk -F: '{print $1}'`
    port=`echo $1|awk -F: '{print $2}'`
    component=$2
    service=UNKNOWN
    workers=0
    
    if [ "$component" = "agent" ]; then
        if [ "$port" = "" ]; then
            port=9093
        fi
        service="`(sleep 1; echo close)|telnet $host $port 2>&1 |grep "Connection refused" |wc -l`"
        if [ "$service" = "0" ]; then
            workers="`(sleep 1; echo list;sleep 1; echo close)|telnet $host $port 2>&1|grep adaptor|wc -l`"
            service=ON
        else
            service=OFF
        fi
    fi
    
    if [ "$component" = "collector" ]; then
        if [ "$port" = "" ]; then
            port=8080
        fi
        `_wget` http://$host:$port -O $PRIVATE_TMP_LOG  >> /dev/null 2>&1
        if [ $? = 0 ]; then
            service=ON
        else
            service=OFF
        fi
    fi
    
    if [ "$component" = "Demux" ];then
	    num=0
        `_expect` $HITUNE_HOME/script/ssh.exp $USERNAME $PASSWORD $host "ps ax|grep java|grep Demux|grep -v grep" 2> $PRIVATE_TMP_LOG 1> /dev/null
		if [ $? = 0 ]; then
			num=`grep Demux $PRIVATE_TMP_LOG|grep java|grep -v grep|wc -l`
			
		fi
		
		test $num -ge 1 && service=ON
		test $num -lt 1 && service=OFF
    fi
    
    if [ "$component" = "postprocess" ];then
	    num=0
        `_expect` $HITUNE_HOME/script/ssh.exp $USERNAME $PASSWORD $host "ps ax|grep java|grep PostProcessor|grep -v grep" 2> $PRIVATE_TMP_LOG 1> /dev/null
		
        if [ $? = 0 ]; then
		    num=`grep PostProcessor $PRIVATE_TMP_LOG|grep java|grep -v grep|wc -l`
            
		fi
		test $num -ge 1 && service=ON
        test $num -lt 1 && service=OFF
    fi
    
    
    echo -en $service
    if [ "$workers" != "0" ]; then echo -en :$workers; fi
    
    
    
}

checklog(){
    host=`echo $1|awk -F: '{print $1}'`
    port=`echo $1|awk -F: '{print $2}'`
    component=$2
    LOG_DIR=$3
    warning=-1
    error=-1
    keywords="WARN,ERROR"
    
    if [ "$LOG_DIR" = "" ]; then
        LOG_DIR=${CHUKWA_LOG_DIR}
    fi
    
    flag=0
    if [ "`echo ${LOG_DIR}|sed "s|${HADOOP_LOG_DIR}|_HITTEN_|g"|grep _HITTEN_`" != "" ]; then
        WEB_FILE=`echo  ${LOG_DIR}|sed "s|${HADOOP_LOG_DIR}||g"`"/${component}.log"
        for i in ${HADOOP_WEB_PORT[@]}; do
            `_wget` http://$host:$i/logs$WEB_FILE -O $PRIVATE_TMP_LOG
            if [ $? = 0 ]; then
                 count $keywords $PRIVATE_TMP_LOG
                 flag=1
                 break
            fi
        done
        #test $flag = 0 && echo -en "$warning $error"
    fi
    if [ $flag = 0 ]; then    
        `_expect` $HITUNE_HOME/script/ssh.exp $USERNAME $PASSWORD $host "cat ${LOG_DIR}/${component}.log" 2> $PRIVATE_TMP_LOG 1> /dev/null
		if [ $? = 0 ]; then
           if [ "`grep "No such file or directory" $PRIVATE_TMP_LOG |wc -l`" = 1 ]; then
            echo -en "$warning $error"
           else
            count $keywords $PRIVATE_TMP_LOG
           fi
		else
		   echo -en "$warning $error"
		fi
    fi
    
    
}

checkhealth(){
    result=0;
    
    
    in=$2
    _s="`echo $in|awk -F: \"{print \\$1}\"`";
    test "$_s" != "0" && result=-1
    test "$_s" = "-1" && result=-2
    
    in=$3
    _s="`echo $in|awk -F: \"{print \\$1}\"`";
    test "$_s" != "0" && result=1
    test "$_s" = "-1" && result=-2
    
    in=$1
    _s="`echo $in|awk -F: \"{print \\$1}\"`";
    test "$_s" != "ON" && result=1
    
    
    test $result = 0 &&  printStatus OK
    test $result = 1 &&  printStatus ERROR
    test $result = -1 &&  printStatus WARN
    test $result = -2 &&  printStatus UNKOWN
    
}

checkstatus(){
    host=$1
    component=$2
    STATUS=OK
    #echo "[INFO]checking $host $component"
    
    
    echo -en "${PRIVATE_FOUR_SPACEPRIVATE_TAB}${PRIVATE_FOUR_SPACEPRIVATE_TAB}"
    echo -en "$component "
    echo -en "$host "
    
    SERVICE=`checkservice $host $component`
    if [ "$component" = "agent" ]; then
        LOG=`checklog $host $component $CHKUWA_AGENT_LOG_DIR`
    elif [ "$component" = "collector" ]; then
        LOG=`checklog $host $component $CHKUWA_COLLECTOR_LOG_DIR`
    else
        LOG=`checklog $host $component $CHKUWA_DP_LOG_DIR`
    fi
    
    echo -en "$SERVICE "
    echo -en "$LOG "
    checkhealth $SERVICE $LOG
    echo  
}

moredetails(){
    host=`echo $1|awk -F: '{print $1}'`
    port=`echo $1|awk -F: '{print $2}'`
    component=$2
    
    
    
    
    if [ $component = "agent" ];then
        
        echo -en "${PRIVATE_FOUR_SPACEPRIVATE_TAB}${PRIVATE_FOUR_SPACEPRIVATE_TAB}${PRIVATE_FOUR_SPACEPRIVATE_TAB}$host " 
        toscan=$CHUKWA_AGENT_HADOOP_METRIC_DIR
        flag=0
        cnt=-1
        if [ "`echo ${CHUKWA_AGENT_HADOOP_METRIC_DIR}|sed "s|${HADOOP_LOG_DIR}|_HITTEN_|g"|grep _HITTEN_`" != "" ]; then
            WEB_FILE=`echo  ${CHUKWA_AGENT_HADOOP_METRIC_DIR}|sed "s|${HADOOP_LOG_DIR}||g"`
            for i in ${HADOOP_WEB_PORT[@]}; do
                `_wget` http://$host:$i/logs$WEB_FILE -O $PRIVATE_TMP_LOG
                if [ $? = 0 ]; then
                     grep -v "Parent Directory" $PRIVATE_TMP_LOG|grep "<TR><TD>"|wc -l > $PRIVATE_TMP_LOG
                     flag=1
                     break
                fi
            done
        fi
        if [ $flag = 0 ]; then
            `_expect` $HITUNE_HOME/script/ssh.exp $USERNAME $PASSWORD $host "ls $toscan" 2> $PRIVATE_TMP_LOG 1> /dev/null
			if [ $? = 0 ]; then
                test "`grep "No such file or directory" $PRIVATE_TMP_LOG`" = "" && cnt=`cat $PRIVATE_TMP_LOG|wc -l`
			fi
        else
            cnt=`cat $PRIVATE_TMP_LOG`
        fi
        
        
        echo -en "HadoopMetrics:$cnt "
        
        
        toscan=$CHUKWA_AGENT_HADOOP_HISTORY_DIR
        flag=0
        cnt=-1
        if [ "`echo ${CHUKWA_AGENT_HADOOP_HISTORY_DIR}|sed "s|${HADOOP_LOG_DIR}|_HITTEN_|g"|grep _HITTEN_`" != "" ]; then
            WEB_FILE=`echo  ${CHUKWA_AGENT_HADOOP_HISTORY_DIR}|sed "s|${HADOOP_LOG_DIR}||g"`
            for i in ${HADOOP_WEB_PORT[@]}; do
                `_wget` http://$host:$i/logs$WEB_FILE -O $PRIVATE_TMP_LOG
                if [ $? = 0 ]; then
                     rst=`grep -v "Parent Directory" $PRIVATE_TMP_LOG|grep "<TR><TD>"|wc -l`
                     echo $rst > $PRIVATE_TMP_LOG
                     flag=1
                     break
                fi
            done
        fi
        if [ $flag = 0 ]; then
		    
            `_expect` $HITUNE_HOME/script/ssh.exp $USERNAME $PASSWORD $host "ls $toscan" 2> $PRIVATE_TMP_LOG 1> /dev/null
			if [ $? = 0 ]; then
                test "`grep "No such file or directory" $PRIVATE_TMP_LOG`" = "" && cnt=`grep job $PRIVATE_TMP_LOG|wc -l`
			fi
        else
            cnt=`cat $PRIVATE_TMP_LOG`
        fi
        
        
        echo -en "HadoopHistoryLogs:$cnt "
        
        
        toscan=$CHUKWA_AGENT_HITUNE_INSTRUMENT_DIR
		cnt=-1
        `_expect` $HITUNE_HOME/script/ssh.exp $USERNAME $PASSWORD $host "ls $toscan" 2> $PRIVATE_TMP_LOG 1> /dev/null
		if [ $? = 0 ]; then
        test "`grep "No such file or directory" $PRIVATE_TMP_LOG`" = "" && cnt=`grep log $PRIVATE_TMP_LOG|wc -l `
		fi
        echo -en "HiTuneInstruments:$cnt "
        echo 
    fi
    
    if [ $component = "postprocess" ];then
        echo "${PRIVATE_FOUR_SPACEPRIVATE_TAB}${PRIVATE_FOUR_SPACEPRIVATE_TAB}${PRIVATE_FOUR_SPACEPRIVATE_TAB}reposfolderlist:"
        sh $HADOOP_HOME/bin/hadoop fs -fs $HDFS_URL -ls $chukwaRecordsRepository* |awk '{print $8}' >  $PRIVATE_TMP_LOG
        
        cnt=0
        for line in `cat $PRIVATE_TMP_LOG`; do
            isError=`echo $line|grep InError`
            if [ "$isError" = "" ]; then
                isError=0
            else
                isError=1
            fi
            echo -en "${PRIVATE_FOUR_SPACEPRIVATE_TAB}${PRIVATE_FOUR_SPACEPRIVATE_TAB}${PRIVATE_FOUR_SPACEPRIVATE_TAB}${PRIVATE_FOUR_SPACEPRIVATE_TAB}$line"
            test $isError = 1 && echo '*' && let cnt++
            test $isError = 0 && echo 
        done
        echo -en "${PRIVATE_FOUR_SPACEPRIVATE_TAB}${PRIVATE_FOUR_SPACEPRIVATE_TAB}${PRIVATE_FOUR_SPACEPRIVATE_TAB}${PRIVATE_FOUR_SPACEPRIVATE_TAB}InErrorFolder:$cnt"
    fi
}


#echo ; echo ; echo ;
dumpall(){
echo "${PRIVATE_FOUR_SPACEPRIVATE_TAB}---------------- Dump Configuration ----------------"

echo -en "${PRIVATE_FOUR_SPACEPRIVATE_TAB}Agents"
echo 
    echo -en "${PRIVATE_FOUR_SPACEPRIVATE_TAB}${PRIVATE_FOUR_SPACEPRIVATE_TAB}agent_nodes: ${PRIVATE_DOTS}${PRIVATE_DOTS} "
    AGENTS=`cat $CHUKWA_HOME/conf/agents|sed -e :a -e 'N;s/\n/,/;ta' `
    echo "$AGENTS" 

    dumpconf agent $CHKUWA_AGENT_LOG_DIR
    
    echo -en "${PRIVATE_FOUR_SPACEPRIVATE_TAB}${PRIVATE_FOUR_SPACEPRIVATE_TAB}[HadoopMetrics]: ${PRIVATE_DOTS}${PRIVATE_DOTS} "
    echo ${CHUKWA_AGENT_HADOOP_METRIC_DIR};
    echo -en "${PRIVATE_FOUR_SPACEPRIVATE_TAB}${PRIVATE_FOUR_SPACEPRIVATE_TAB}[HadoopHistoryLogs]: ${PRIVATE_DOTS}${PRIVATE_DOTS} "
    echo $HADOOP_LOG_DIR/history/done;
    echo -en "${PRIVATE_FOUR_SPACEPRIVATE_TAB}${PRIVATE_FOUR_SPACEPRIVATE_TAB}[HiTuneInstruments]: ${PRIVATE_DOTS}${PRIVATE_DOTS} "
    echo $CHUKWA_AGENT_HITUNE_INSTRUMENT_DIR;


echo 
echo -en "${PRIVATE_FOUR_SPACEPRIVATE_TAB}Collectors"
echo 
    echo -en "${PRIVATE_FOUR_SPACEPRIVATE_TAB}${PRIVATE_FOUR_SPACEPRIVATE_TAB}collector_nodes: ${PRIVATE_DOTS}${PRIVATE_DOTS} "
    COLLECTS=`cat $CHUKWA_HOME/conf/collectors|sed -e :a -e 'N;s/\n/,/;ta'`
    echo "$COLLECTS"  
    dumpconf collector $CHKUWA_COLLECTOR_LOG_DIR


echo 
echo -en "${PRIVATE_FOUR_SPACEPRIVATE_TAB}Dataprocess"
echo ""
    echo -en "${PRIVATE_FOUR_SPACEPRIVATE_TAB}${PRIVATE_FOUR_SPACEPRIVATE_TAB}dataprocessing_nodes: ${PRIVATE_DOTS}${PRIVATE_DOTS} "
    test "$CHUKWA_HOST" = "" && DEMUX=`hostname`
    test "$CHUKWA_HOST" != "" && DEMUX=$CHUKWA_HOST
    echo "$DEMUX"
    dumpconf Demux $CHKUWA_DP_LOG_DIR
    dumpconf postprocess $CHKUWA_DP_LOG_DIR


    echo -en "${PRIVATE_FOUR_SPACEPRIVATE_TAB}${PRIVATE_FOUR_SPACEPRIVATE_TAB}Aggregation folder: ${PRIVATE_DOTS}${PRIVATE_DOTS} "
#$HADOOP_HOME/bin/hadoop fs -fs $HDFS_URL -ls $chukwaRecordsRepository/*
    echo "${HDFS_URL}$chukwaRecordsRepository"

}

########################
# 1. check Instruments
# 2. check Aggregations
# 3. check data before HiTuneAnalysis


checkall(){
echo ; echo ; echo
test -e $PRIVATE_REPORT && rm -rf $PRIVATE_REPORT
echo "${PRIVATE_FOUR_SPACEPRIVATE_TAB}---------------- Health Checker ----------------"
echo -en "${PRIVATE_FOUR_SPACEPRIVATE_TAB}Check Instruments:${PRIVATE_DOTS}${PRIVATE_DOTS}"
check agent $AGENTS 1>> $PRIVATE_REPORT 2>>$PRIVATE_TMP_ERR &
waiting $!
echo -e "\033[1D.DONE"

echo -en "${PRIVATE_FOUR_SPACEPRIVATE_TAB}Check Aggregation:${PRIVATE_DOTS}${PRIVATE_DOTS}"
check collector $COLLECTS 1>> $PRIVATE_REPORT 2>>$PRIVATE_TMP_ERR &
waiting $!
echo -e "\033[1D.DONE"


echo -en "${PRIVATE_FOUR_SPACEPRIVATE_TAB}Check Dataprocessing:${PRIVATE_DOTS}${PRIVATE_DOTS}"
check Demux,postprocess $DEMUX 1>> $PRIVATE_REPORT 2>>$PRIVATE_TMP_ERR &
waiting $!
echo -e "\033[1D.DONE"

}

reportall(){
# dump the final report about each phase
echo ; echo ; echo 
echo "${PRIVATE_FOUR_SPACEPRIVATE_TAB}---------------- Status Report ----------------"
if [ -e $PRIVATE_REPORT ]; then
    echo "${PRIVATE_FOUR_SPACEPRIVATE_TAB}SAVING FINAL REPORT AS: $PRIVATE_REPORT"
    echo
    cat $PRIVATE_REPORT
fi
}

while [ $# -ge 1 ]; do
    case $1 in
    -h)
        usage
		exit 0
        ;;
	*)
	    usage
		exit 1
		;;
    esac
done


main(){
    checkargs;
    dumpall;
    checkall;
    reportall;
}

main;