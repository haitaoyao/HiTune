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

import org.apache.hadoop.record.Record;

/**
 * 
 *
 */
public class ChukwaRecordKey extends HiTuneKeyProxy<org.apache.hadoop.chukwa.extraction.engine.ChukwaRecordKey> {

    /**
     * @param key
     */
    public ChukwaRecordKey(org.apache.hadoop.chukwa.extraction.engine.ChukwaRecordKey key) {
        super(key);
        // TODO Auto-generated constructor stub
    }

    /* (non-Javadoc)
     * @see hitune.analysis.mapreduce.proxy.HiTuneKeyProxy#getDataType()
     */
    @Override
    public String getDataType() {
        // TODO Auto-generated method stub
        return ((org.apache.hadoop.chukwa.extraction.engine.ChukwaRecordKey)this.key).getReduceType();
    }

    /* (non-Javadoc)
     * @see hitune.analysis.mapreduce.proxy.HiTuneKeyProxy#getKey()
     */
    @Override
    public String getKey() {
        // TODO Auto-generated method stub
        return ((org.apache.hadoop.chukwa.extraction.engine.ChukwaRecordKey)this.key).getKey();
    }

    /* (non-Javadoc)
     * @see hitune.analysis.mapreduce.proxy.HiTuneKeyProxy#setDataType(java.lang.String)
     */
    @Override
    public void setDataType(String datatype) {
        // TODO Auto-generated method stub
        ((org.apache.hadoop.chukwa.extraction.engine.ChukwaRecordKey)this.key).setReduceType(datatype);
    }

    /* (non-Javadoc)
     * @see hitune.analysis.mapreduce.proxy.HiTuneKeyProxy#setKey(java.lang.String)
     */
    @Override
    public void setKey(String key) {
        // TODO Auto-generated method stub
        ((org.apache.hadoop.chukwa.extraction.engine.ChukwaRecordKey)this.key).setKey(key);
    }
    
    public String toString(){
        return ((org.apache.hadoop.chukwa.extraction.engine.ChukwaRecordKey)this.key).toString();
    }

}
