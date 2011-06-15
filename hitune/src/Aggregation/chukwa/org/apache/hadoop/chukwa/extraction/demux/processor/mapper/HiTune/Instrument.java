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
public class Instrument extends AbstractProcessor {
    static Logger log = Logger.getLogger(Instrument.class);
    static String[] headers = null;
    String headermark = "TimeStamp";
    static String reduceType = "HiTune.Instrument";
    /**
     * 
     */
    public Instrument() {
        // TODO Auto-generated constructor stub
        headers = new String[]{"Timestamp", "ThreadID","ThreadName","isDaemon","ThreadPriority","ThreadState","TaskID","CallStack"};
    }

    /* (non-Javadoc)
     * @see org.apache.hadoop.chukwa.extraction.demux.processor.mapper.AbstractProcessor#parse(java.lang.String, org.apache.hadoop.mapred.OutputCollector, org.apache.hadoop.mapred.Reporter)
     */
    @Override
    protected void parse(String recordEntry,
            OutputCollector<ChukwaRecordKey, ChukwaRecord> output,
            Reporter reporter) throws Throwable {
        try {
            // TODO Auto-generated method stub
            
            
            log.debug("recordEntry: " + recordEntry + " dataType: " + chunk.getDataType());

            String []lines = recordEntry.split(Util.LINE_FEED);
            boolean unknowChunk = true;
            for (String line: lines){
                if(line==null || line.equals(Util.EMPTY))continue;
                
                String [] fields = null;
                ChukwaRecord record = null;
                long timestamp = 0;
                
                int index = line.indexOf(headermark);
                if(index!= -1 ){
                    if(headers == null)headers = Util.parseRecordLine(line,Util.COMMA);
                    unknowChunk=false;
                    continue;
                }
                else {
                    fields = Util.parseRecordLine(line,Util.COMMA);
                    if(fields != null && fields.length == headers.length){
                        timestamp = Long.parseLong(fields[0]); //Timestamp in milliseconds
                        record = new ChukwaRecord();
                        key = new ChukwaRecordKey();
                        buildGenericRecord(record, null, timestamp, reduceType);
                        
                        unknowChunk =false;
                        for (int j = 0; j < fields.length; j++){
                            log.debug("data schema: " + headers[j] + " data fields: " + fields[j]);
                            record.add(headers[j], fields[j]);
                        }
                        log.debug("setTime: " + timestamp);
                        record.setTime(timestamp);
                        log.debug("emit record: " + record.toString());
                        output.collect(key, record);
                    }else {
                        if(fields.length == (headers.length -1)){
                            //no call stack case
                            unknowChunk =false;
                            log.warn("No callstack: " + line);
                        }else{
                            log.warn("Unknown record schema: " + line);
                        }
                    }
                   
                }
                
                
            }
            if(unknowChunk){
                StringBuilder headerline = new StringBuilder();
                for(String item: headers){
                    if(headerline.length()!=0)headerline.append(",");
                    headerline.append(item);
                }
                throw new Exception("Unknown record schema! Header of: " + headerline + "is expected");
            }
        }
        catch(Exception e){
            log.error("cannot parse the record: " + recordEntry+ " dataType: " + chunk.getDataType());
            e.printStackTrace();
            throw new Exception(e);
        }
    }

   
    /**
     * @param args
     */
    public static void main(String[] args) {
        // TODO Auto-generated method stub
    }

}
