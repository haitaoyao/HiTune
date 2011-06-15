/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package hitune.analysis.mapreduce;

import java.io.File;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;


/**
 * To get or set analysis job related configuration items.
 * 
 */
public class AnalysisProcessorConfiguration {

    static Logger log = Logger.getLogger(AnalysisProcessorConfiguration.class);

    public final static String baseid = "HiTune.analyzer.targetjob.baseid";
    public final static String reportconf = "HiTune.analyzer.configfolder";
    public final static String basestart = "HiTune.analyzer.targetjob.starttime_sec";
    public final static String baseend = "HiTune.analyzer.targetjob.endtime_sec";
    
    public final static String filefilter = "HiTune.analyzer.filefilter.class";
    public final static String filefilter_pattern = "HiTune.analyzer.filefilter.pattern";
    /**
     * Input data's folder
     */
    public final static String datasource = "datasource";
    
    /**
     * Home of the report output folder.
     */
    public final static String reportfolder = "HiTune.output.reportfolder.home";
    /**
     * The report output folder over HDFS
     */
    public final static String reportfile = "HiTune.output.reportfolder.name";
    /**
     * The analyzer's job name, which might be "HadoopMetrics", "HistoryLogJob", "SystemLog", "MapInstruments"
     */
    public final static String reportengine = "analyzerengine";
    /**
     * The output csv file's name
     */
    public final static String outputfilename = "outputfilename";
    /**
     * The start point of the focusing time period. Generally, it should be the start time of a job.
     */
    public final static String starttime = "starttime";
    /**
     * The end point of the focusing time period. Generally, it should be the end time of a job.
     */
    public final static String endtime = "endtime";
    /**
     * For those system related statistics, it should imply the focusing devices. 
     * For example, which harddrive (sda, sdb ,...); which CPU(CPU_0,CPU_1,...); which Ethernet device (eth0,eth1...)
     */
    public final static String devics = "devices";
    /**
     * For those focusing nodes in the cluster
     */
    public final static String nodes = "HiTune.analyzer.targetcluster";
    /**
     * Focusing category for Hadoop metrics. For example, it has several categories under dfs metrics, 
     * which are datanode, namenode and etc.
     */
    public final static String category = "category";
    /**
     * Focusing metrics
     */
    public final static String metrics = "metrics";
    /**
     * Focusing job's id
     */
    public final static String jobid = "jobid";
    
    public final static String attemptid = "attemptid";
    public final static String taskid = "taskid";
    /**
     * Focusing phases which shows certain period of event happens for each task among task pool.
     */
    public final static String phase = "phase";
    
    public final static String limit = "limit";
    public final static String funcInStackFormat = "funcInStackFormat";

    public final static String mapoutputKeyClass = "HiTune.analyzer.key.class";
    public final static String mapoutputValueClass = "HiTune.analyzer.value.class";
    
    /**
     * @param args
     */
    public static void main(String[] args) {
        // TODO Auto-generated method stub

    }

}
