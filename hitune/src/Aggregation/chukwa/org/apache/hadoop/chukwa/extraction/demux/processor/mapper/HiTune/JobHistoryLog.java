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
package org.apache.hadoop.chukwa.extraction.demux.processor.mapper.HiTune;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.chukwa.extraction.demux.processor.mapper.AbstractProcessor;
import org.apache.hadoop.chukwa.extraction.engine.ChukwaRecord;
import org.apache.hadoop.chukwa.extraction.engine.ChukwaRecordKey;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.log4j.Logger;
import org.apache.log4j.ConsoleAppender;

/**
 * 
 *
 */
public class JobHistoryLog extends AbstractProcessor {
    static Logger log = Logger.getLogger(JobHistoryLog.class);
    static String reduceType = "HiTune.JobHistoryLog";


    public JobHistoryLog() {
        // TODO Auto-generated constructor stub

    }

    /* (non-Javadoc)
     * @see org.apache.hadoop.chukwa.extraction.demux.processor.mapper.AbstractProcessor#parse(java.lang.String, org.apache.hadoop.mapred.OutputCollector, org.apache.hadoop.mapred.Reporter)
     */
    @Override
    protected void parse(String recordEntry,
            OutputCollector<ChukwaRecordKey, ChukwaRecord> output,
            Reporter reporter) throws Throwable {
        try{
            // TODO Auto-generated method stub
            log.info("recordEntry: " + recordEntry + " dataType: " + chunk.getDataType());

            if(chunk.getStreamName().indexOf(".crc")!=-1||chunk.getStreamName().indexOf(".xml")!=-1){
                log.info("Ignore the XML|CRC file");
                return;
            }

            String []lines = recordEntry.split(Util.LINE_FEED);
            boolean unknowChunk = true;
            for (String line: lines){
                if(line==null || line.equals(Util.EMPTY))continue;

                HashMap<String, String> splits = new HashMap<String, String>();
                ChukwaRecord record = null;

                int index_recordType = line.indexOf(" ");
                if(index_recordType==-1){
                    log.warn("No proper record Type can be found. line: " + line);
                    continue;
                    //throw new Exception("No proper record Type can be found.");
                }else {

                    String recordType = line.substring(0, index_recordType);
                    splits.put("RECORD_TYPE", recordType);

                    String remaining = line.substring(index_recordType).trim();
                    String[] parts = remaining.split("\" ");
                    for(String part : parts){
                        log.debug("part:" + part);
                        String key="";
                        String value = "";
                        if(part.indexOf("=\"")!= -1){
                            String[] kVal = part.split("=\"");
                            if(kVal[0]!=null) key = kVal[0];
                            if(kVal.length > 1 && kVal[1]!=null) value = kVal[1];
                            if(!key.equals("")){
                                log.debug("put: " + key + "=" + value);
                                splits.put(key, value);
                            }
                        }
                    }
                    
                    //job.csv
                    if(recordType.equals("Job")){
                        long timestamp = 0;
                        if(splits.containsKey("SUBMIT_TIME")){
                            timestamp = Long.parseLong(splits.get("SUBMIT_TIME"));
                        }
                        if(splits.containsKey("LAUNCH_TIME")){
                            timestamp = Long.parseLong(splits.get("LAUNCH_TIME"));
                        }
                        if(splits.containsKey("FINISH_TIME")){
                            timestamp = Long.parseLong(splits.get("FINISH_TIME"));
                        }
                        if(timestamp != 0){
                            key = new ChukwaRecordKey();
                            record = new ChukwaRecord();
                            buildGenericRecord(record, null, timestamp, reduceType);
                            key.setKey("Job/" + splits.get("JOBID"));
                            addFields(splits, record);
                        }
                        unknowChunk=false;
                    }

                    //MapTask.csv & ReduceTask.csv
                    else if(recordType.equals("Task")){
                        if(splits.containsKey("TASK_TYPE")){
                            long timestamp = 0;
                            if(splits.containsKey("START_TIME")){
                                timestamp = Long.parseLong(splits.get("START_TIME"));
                            }
                            if(splits.containsKey("FINISH_TIME")){
                                timestamp = Long.parseLong(splits.get("FINISH_TIME"));
                            }
                            if(timestamp != 0){
                                key = new ChukwaRecordKey();
                                record = new ChukwaRecord();
                                buildGenericRecord(record, null, timestamp, reduceType);
                                if(splits.get("TASK_TYPE").equalsIgnoreCase("REDUCE")){
                                    key.setKey("ReduceTask/" + splits.get("TASKID"));
                                }
                                else {
                                    key.setKey("MapTask/" + splits.get("TASKID"));
                                }
                                addFields(splits, record);
                            }

                        }
                        unknowChunk=false;
                    }

                    //MapAttempt.csv
                    else if(recordType.equals("MapAttempt")){
                        long timestamp = 0;
                        if(splits.containsKey("START_TIME")){
                            timestamp = Long.parseLong(splits.get("START_TIME"));
                        }
                        if(splits.containsKey("FINISH_TIME")){
                            timestamp = Long.parseLong(splits.get("FINISH_TIME"));
                        }
                        if(timestamp != 0){
                            key = new ChukwaRecordKey();
                            record = new ChukwaRecord();
                            buildGenericRecord(record, null, timestamp, reduceType);
                            key.setKey("MapAttempt/" + splits.get("TASK_ATTEMPT_ID"));
                            addFields(splits, record);
                        }
                        unknowChunk=false;
                    }

                    //ReduceAttempt.csv
                    else if(recordType.equals("ReduceAttempt")){
                        long timestamp = 0;
                        if(splits.containsKey("START_TIME")){
                            timestamp = Long.parseLong(splits.get("START_TIME"));
                        }
                        if(splits.containsKey("FINISH_TIME")){
                            timestamp = Long.parseLong(splits.get("FINISH_TIME"));
                        }
                        if(timestamp != 0){
                            key = new ChukwaRecordKey();
                            record = new ChukwaRecord();
                            buildGenericRecord(record, null, timestamp, reduceType);
                            key.setKey("ReduceAttempt/" + splits.get("TASK_ATTEMPT_ID"));
                            addFields(splits, record);
                        }
                        unknowChunk=false;
                    }else if(recordType.equals("Meta")){
                        log.warn("Meta info: " + recordType + " line: " +line);
                        unknowChunk=false;
                    }else {
                        
                        log.warn("Unkwon RecordType: " + recordType + " line: " +line);
                    }
                    if(record != null){
                        //prepare counters
                        
                        if(splits.containsKey("COUNTERS") ){
                            extractCounters(record, splits.get("COUNTERS"));
                            record.removeValue("COUNTERS");
                            record.add("COUNTERS", "1");
                        }

                        log.debug("emit record: " + record.toString());
                        output.collect(key, record);
                    }
                }
            }
            if(unknowChunk){
                throw new Exception("Unknown record schema!");
            }

        }catch(Exception e){
            log.error("cannot parse the record line: " + recordEntry+ " dataType: " + chunk.getDataType());
            e.printStackTrace();
            throw new Exception(e);
        }
    }

    protected void extractCounters(ChukwaRecord record, String input){
        String regex = "[^\\(]*\\)\\([0-9]+";
        Pattern p = Pattern.compile(regex);
        log.debug("counter contents: " + input);
        Matcher matcher=p.matcher(input);
        while(matcher.find()){
            String tmp = matcher.group();
            String []items = tmp.split("\\)\\(");
            record.add(items[0], items[1]);
            log.debug("data schema: " + items[0] + " data fields: " + items[1]);
        }
    }

    protected void addFields(HashMap<String,String> map, ChukwaRecord record){
        for (Iterator iter = map.entrySet().iterator(); iter.hasNext();) {
            Map.Entry entry = (Map.Entry) iter.next();
            String key = (String) entry.getKey();
            String val = (String) entry.getValue();
            log.debug("data schema: " + key + " data fields: " + val);
            record.add(key, val);
        }
    }

    /**
     * @param args
     */
    public static void main(String[] args) {
        // TODO Auto-generated method stub

    }

}
