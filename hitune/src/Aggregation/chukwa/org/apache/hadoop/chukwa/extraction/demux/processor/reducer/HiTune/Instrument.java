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

import java.io.IOException;
import java.util.Iterator;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.chukwa.extraction.demux.processor.mapper.HiTune.Util;
import org.apache.hadoop.chukwa.extraction.demux.processor.reducer.ReduceProcessor;
import org.apache.hadoop.chukwa.extraction.engine.ChukwaRecord;
import org.apache.hadoop.chukwa.extraction.engine.ChukwaRecordKey;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.log4j.Logger;

/**
 * 
 *
 */
public class Instrument implements ReduceProcessor {
    static Logger log = Logger.getLogger(Instrument.class);
    String regex = "attempt_([0-9]{12}_[0-9]{4})_([r|m])_[0-9]{6}_[0-9]";
    Pattern p = null;
    Matcher matcher = null;
    /**
     * 
     */
    public Instrument() {
        // TODO Auto-generated constructor stub
        p = Pattern.compile(regex);
    }

    /* (non-Javadoc)
     * @see org.apache.hadoop.chukwa.extraction.demux.processor.reducer.ReduceProcessor#getDataType()
     */
    @Override
    public String getDataType() {
        // TODO Auto-generated method stub
        return "HiTune.Instrument";
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
        String jobID = null;
        String attemptID = null;
        while (values.hasNext()) {
            ChukwaRecordKey newkey = new ChukwaRecordKey();
            newkey.setKey(key.getKey());

            record = values.next();
            attemptID = record.getValue("TaskID");
            log.debug("attemptID: " + attemptID);
            if(attemptID == null || attemptID.equals(Util.EMPTY)){
                newkey.setReduceType(getDataType());
            }
            else {
                matcher = p.matcher(attemptID);
                if (matcher.find()) {
                    jobID= "job_" + matcher.group(1).trim();
                    String subType = jobID;
                    if(matcher.group(2).equals("m")){
                        subType += "/" + "MAP";
                    }
                    else if(matcher.group(2).equals("r")){
                        subType += "/" + "REDUCE";
                    }
                    newkey.setReduceType(getDataType()+"/" + subType + "/" + attemptID);
                }
                else {
                    newkey.setReduceType(getDataType() + "/" + attemptID);
                }
            }
            try {
                log.debug("emit key: "+ newkey.toString() + " record: " + record.toString());
                output.collect(newkey, record);
            } catch (IOException e) {
                // TODO Auto-generated catch block
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
