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
package hitune.analysis.mapreduce.proxy;

import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.record.Record;

/**
 * 
 *
 */
public class ChukwaRecord extends HiTuneRecordProxy<org.apache.hadoop.chukwa.extraction.engine.ChukwaRecord> {

    //org.apache.hadoop.chukwa.extraction.engine.ChukwaRecord record = null;
    /**
     * @param record
     */
    public ChukwaRecord(org.apache.hadoop.chukwa.extraction.engine.ChukwaRecord record) {
        super(record);
        //this.record = (org.apache.hadoop.chukwa.extraction.engine.ChukwaRecord) record;
        // TODO Auto-generated constructor stub
    }

    /* (non-Javadoc)
     * @see hitune.analysis.mapreduce.proxy.HiTuneRecordProxy#add(java.lang.String, java.lang.String)
     */
    @Override
    public void add(String field, String value) {
        // TODO Auto-generated method stub
        ((org.apache.hadoop.chukwa.extraction.engine.ChukwaRecord)this.record).add(field, value);
    }

    /* (non-Javadoc)
     * @see hitune.analysis.mapreduce.proxy.HiTuneRecordProxy#copyCommonFields(org.apache.hadoop.record.Record)
     */
    @Override
    public org.apache.hadoop.chukwa.extraction.engine.ChukwaRecord copyCommonFields(org.apache.hadoop.chukwa.extraction.engine.ChukwaRecord record) {
        // TODO Auto-generated method stub
        ((org.apache.hadoop.chukwa.extraction.engine.ChukwaRecord)this.record).setTime(((org.apache.hadoop.chukwa.extraction.engine.ChukwaRecord)record).getTime());

        ((org.apache.hadoop.chukwa.extraction.engine.ChukwaRecord)this.record).add(org.apache.hadoop.chukwa.extraction.engine.Record.tagsField, 
                ((org.apache.hadoop.chukwa.extraction.engine.ChukwaRecord)record).getValue(org.apache.hadoop.chukwa.extraction.engine.Record.tagsField));
        ((org.apache.hadoop.chukwa.extraction.engine.ChukwaRecord)this.record).add(org.apache.hadoop.chukwa.extraction.engine.Record.sourceField, 
                ((org.apache.hadoop.chukwa.extraction.engine.ChukwaRecord)record).getValue(org.apache.hadoop.chukwa.extraction.engine.Record.sourceField));
        ((org.apache.hadoop.chukwa.extraction.engine.ChukwaRecord)this.record).add(org.apache.hadoop.chukwa.extraction.engine.Record.applicationField, 
                ((org.apache.hadoop.chukwa.extraction.engine.ChukwaRecord)record).getValue(org.apache.hadoop.chukwa.extraction.engine.Record.applicationField));
        return null;
    }

    

    /* (non-Javadoc)
     * @see hitune.analysis.mapreduce.proxy.HiTuneRecordProxy#getTime()
     */
    @Override
    public Long getTime() {
        // TODO Auto-generated method stub
        return ((org.apache.hadoop.chukwa.extraction.engine.ChukwaRecord)this.record).getTime();
    }

    /* (non-Javadoc)
     * @see hitune.analysis.mapreduce.proxy.HiTuneRecordProxy#getValue(java.lang.String)
     */
    @Override
    public String getValue(String field) {
        // TODO Auto-generated method stub
        return ((org.apache.hadoop.chukwa.extraction.engine.ChukwaRecord)this.record).getValue(field);
    }

    /* (non-Javadoc)
     * @see hitune.analysis.mapreduce.proxy.HiTuneRecordProxy#setTime(long)
     */
    @Override
    public void setTime(long timestamp) {
        // TODO Auto-generated method stub
        ((org.apache.hadoop.chukwa.extraction.engine.ChukwaRecord)this.record).setTime(timestamp);
    }

    

    @Override
    public Map<String, String> getCommonFields() {
        // TODO Auto-generated method stub
        Map<String, String> results = new HashMap<String, String>();
        results.put(org.apache.hadoop.chukwa.extraction.engine.Record.tagsField, 
                ((org.apache.hadoop.chukwa.extraction.engine.ChukwaRecord)this.record).getValue(org.apache.hadoop.chukwa.extraction.engine.Record.tagsField));
        results.put(org.apache.hadoop.chukwa.extraction.engine.Record.sourceField, 
                ((org.apache.hadoop.chukwa.extraction.engine.ChukwaRecord)this.record).getValue(org.apache.hadoop.chukwa.extraction.engine.Record.sourceField));
        results.put(org.apache.hadoop.chukwa.extraction.engine.Record.applicationField, 
                ((org.apache.hadoop.chukwa.extraction.engine.ChukwaRecord)this.record).getValue(org.apache.hadoop.chukwa.extraction.engine.Record.applicationField));
        return results;
    }

    @Override
    public String[] getFields() {
        // TODO Auto-generated method stub
        return ((org.apache.hadoop.chukwa.extraction.engine.ChukwaRecord)this.record).getFields();
    }

    @Override
    public String getHost() {
        // TODO Auto-generated method stub
        return ((org.apache.hadoop.chukwa.extraction.engine.ChukwaRecord)this.record).getValue(org.apache.hadoop.chukwa.extraction.engine.ChukwaRecord.sourceField);
    }

    @Override
    public void setHost(String hostname) {
        // TODO Auto-generated method stub
        ((org.apache.hadoop.chukwa.extraction.engine.ChukwaRecord)this.record).add(org.apache.hadoop.chukwa.extraction.engine.ChukwaRecord.sourceField, hostname);
    }
    
    public String toString(){
        return ((org.apache.hadoop.chukwa.extraction.engine.ChukwaRecord)this.record).toString();
    }

}
