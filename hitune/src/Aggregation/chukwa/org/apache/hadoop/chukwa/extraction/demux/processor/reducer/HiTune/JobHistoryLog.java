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
package org.apache.hadoop.chukwa.extraction.demux.processor.reducer.HiTune;

import hitune.analysis.mapreduce.AnalysisProcessorConfiguration;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Iterator;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.chukwa.extraction.demux.processor.reducer.ReduceProcessor;
import org.apache.hadoop.chukwa.extraction.engine.ChukwaRecord;
import org.apache.hadoop.chukwa.extraction.engine.ChukwaRecordKey;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.log4j.Logger;


/**
 * 
 *
 */
public class JobHistoryLog implements ReduceProcessor {
    static Logger log = Logger.getLogger(JobHistoryLog.class);
    String regex = ".*_([0-9]{12}_[0-9]{4})_([r|m])_.*";
    Pattern p = null;
    Matcher matcher = null;



    static final String joblist_folder = "/.JOBS";
    /**
     * 
     */
    public JobHistoryLog() {
        // TODO Auto-generated constructor stub
        p = Pattern.compile(regex);
    }

    /* (non-Javadoc)
     * @see org.apache.hadoop.chukwa.extraction.demux.processor.reducer.ReduceProcessor#getDataType()
     */
    @Override
    public String getDataType() {
        // TODO Auto-generated method stub
        return "HiTune.JobHistoryLog";
    }

    /* (non-Javadoc)
     * @see org.apache.hadoop.chukwa.extraction.demux.processor.reducer.ReduceProcessor#process(org.apache.hadoop.chukwa.extraction.engine.ChukwaRecordKey, java.util.Iterator, org.apache.hadoop.mapred.OutputCollector, org.apache.hadoop.mapred.Reporter)
     */
    @Override
    public void process(ChukwaRecordKey key, Iterator<ChukwaRecord> values,
            OutputCollector<ChukwaRecordKey, ChukwaRecord> output,
            Reporter reporter) {
        // TODO Auto-generated method stub
        ChukwaRecord record = null;

        JobConf jobconf = null;

        String jobID = null;
        String taskID = null;
        String attemptID = null;
        ChukwaRecordKey newkey = null;
        while (values.hasNext()) {
            newkey = null;
            record = values.next();
            String keyValue = key.getKey();
            log.debug("key:"+ keyValue);
            int index = -1;
            if((index = keyValue.indexOf("Job/"))!= -1){
                jobID = keyValue.substring(keyValue.indexOf("/")+1);
                if(jobconf==null){
                    jobconf=new JobConf(jobID.substring(4));
                }
                jobconf.update(record.getTime()/1000);
                newkey = new ChukwaRecordKey();
                newkey.setReduceType("HiTune.JobHistoryLog" + "/" + jobID + "/" + "Job");
            }
            else if((index = keyValue.indexOf("MapTask/"))!= -1){
                taskID = keyValue.substring(keyValue.indexOf("/")+1);
                matcher = p.matcher(taskID);
                if (matcher.find()) {
                    jobID = "job_" + matcher.group(1).trim();
//                    if(jobconf==null){
//                        jobconf=new JobConf(jobID.substring(4));
//                    }
//                    jobconf.update(record.getTime()/1000);
                    newkey = new ChukwaRecordKey();
                    newkey.setReduceType("HiTune.JobHistoryLog" + "/" + jobID + "/" + "MapTask");
                }

            }
            else if((index = keyValue.indexOf("ReduceTask/"))!= -1){
                taskID = keyValue.substring(keyValue.indexOf("/")+1);
                matcher = p.matcher(taskID);
                if (matcher.find()) {
                    jobID = "job_" + matcher.group(1).trim();
//                    if(jobconf==null){
//                        jobconf=new JobConf(jobID.substring(4));
//                    }
//                    jobconf.update(record.getTime()/1000);
                    newkey = new ChukwaRecordKey();
                    newkey.setReduceType("HiTune.JobHistoryLog" + "/" + jobID + "/" + "ReduceTask");
                }

            }
            else if((index = keyValue.indexOf("MapAttempt/"))!= -1){
                attemptID = keyValue.substring(keyValue.indexOf("/")+1);
                matcher = p.matcher(attemptID);
                if (matcher.find()) {
                    jobID = "job_" + matcher.group(1).trim();
//                    if(jobconf==null){
//                        jobconf=new JobConf(jobID.substring(4));
//                    }
//                    jobconf.update(record.getTime()/1000);
                    newkey = new ChukwaRecordKey();
                    newkey.setReduceType("HiTune.JobHistoryLog" + "/" + jobID + "/" + "MapAttempt");
                }

            }
            else if((index = keyValue.indexOf("ReduceAttempt/"))!= -1){
                attemptID = keyValue.substring(keyValue.indexOf("/")+1);
                matcher = p.matcher(attemptID);
                if (matcher.find()) {
                    jobID = "job_" + matcher.group(1).trim();
//                    if(jobconf==null){
//                        jobconf=new JobConf(jobID.substring(4));
//                    }
//                    jobconf.update(record.getTime()/1000);
                    newkey = new ChukwaRecordKey();
                    newkey.setReduceType("HiTune.JobHistoryLog" + "/" + jobID + "/" + "ReduceAttempt");
                }

            }

            if(newkey!=null && record!=null){
                newkey.setKey(keyValue + "/" + record.getTime());
                log.debug("newkey:" + newkey.getKey());
                log.debug("newkeyType:" + newkey.getReduceType());
                try {
                    log.debug("emit key: "+ newkey.toString() + " record: " + record.toString());
                    output.collect(newkey,record);
                } catch (IOException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
            }

        }

        if(jobconf != null){
            jobconf.flush();
        }

    }

    /**
     * To output the job corresponding configuration xml file to certain folder (configured in chukwa-job)
     * 
     * 1. no job_xxxxx.xml existing, create a new one, with <min, max> time
     * 2. job_xxxx.xml existing, load that file and update it with <min, max> time
     *
     * job_xxxx.xml file is stored on HDFS
     */
    class JobConf {

        long startTime = -1;
        long endTime = -1;
        String jobid = null;
        Configuration conf = null;
        FileSystem fs = null;

        public JobConf(String jobid){
            this.jobid = jobid;
            conf = new Configuration(false);
            Path xmlfile = new Path(joblist_folder + "/"+ jobid + ".xml");
            try {
                fs = xmlfile.getFileSystem(new Configuration());
            } catch (IOException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
       }


        public void update(long timestamp){
            log.debug("update timestamp: " + timestamp);
            if(timestamp < startTime || startTime == -1) startTime = timestamp;
            if(timestamp > endTime || endTime == -1) endTime = timestamp;
            log.debug("starttime: " +startTime + " endtime: " + endTime );
        }

        public void waitForLock() throws IOException, InterruptedException{
            log.debug("Trying to get lock");
            long timeout = 120;
            long start = System.currentTimeMillis();
            while(fs.exists(new Path(joblist_folder + "/"+ jobid + ".lock"))){
                Thread.sleep(5000);
                if(System.currentTimeMillis() - start > timeout){
                    throw new InterruptedException("timeout");
                }
            }
        }

        public boolean LockON() throws IOException, InterruptedException{
            waitForLock();
            log.debug("create lock");
            return fs.createNewFile(new Path(joblist_folder + "/"+ jobid + ".lock"));
        }

        public void LockOFF() throws IOException{
            log.debug("release lock");
            fs.delete(new Path(joblist_folder + "/"+ jobid + ".lock"));
        }

        public void flush(){
            try {
                //Path lock = new Path(joblist_folder + "/"+ jobid + ".lock");

                if(conf!=null ){
                    if(startTime != -1 && endTime != -1){
                        int count = 0;
                        while(!LockON()){
                            count++;
                            if(count>5){
                                log.error("Too many retries.");
                                throw new InterruptedException("Too many retries.");
                            }
                        }

                        String hdfsUrlPrefix = conf.get("fs.default.name");
                        Path xmlfile = new Path(joblist_folder + "/"+ jobid + ".xml");
                        fs = xmlfile.getFileSystem(new Configuration());

                        if(fs.exists(xmlfile)){
                            log.debug("get existing file: " + xmlfile.toString());
                            Configuration newconf = new Configuration(false);
                            newconf.addResource(fs.open(xmlfile));
                            long stime = newconf.getLong(AnalysisProcessorConfiguration.basestart, -1);
                            long etime = newconf.getLong(AnalysisProcessorConfiguration.baseend, -1);
                            if(stime != -1) update(stime);
                            if(etime != -1) update(etime);
                            fs.delete(xmlfile);   
                        }
                        conf.setStrings(AnalysisProcessorConfiguration.baseid, jobid);
                        conf.setLong(AnalysisProcessorConfiguration.basestart, startTime);
                        conf.setLong(AnalysisProcessorConfiguration.baseend, endTime);
                        conf.setStrings(AnalysisProcessorConfiguration.reportfile, jobid);
                        log.debug("starttime: " +startTime + " endtime: " + endTime );
                        log.debug("create configuration file");
                        FSDataOutputStream out = fs.create(xmlfile);
                        conf.writeXml(out);
                        out.flush();
                        out.close();
                        LockOFF();

                    }
                    else {
                        log.warn("no updated data");
                    }
                }
            } catch (IOException e) {
                // TODO Auto-generated catch block
                log.error("cannot do the xmlfile's filesystem operation");
                log.error(e);
                e.printStackTrace();
            }catch (InterruptedException e){
                log.error("waiting for lock to release");
                log.error(e);
                e.printStackTrace();
            }


        }
    }

    /**
     * @param args
     */
    public static void main(String[] args) {
        // TODO Auto-generated method stub

    }

}
