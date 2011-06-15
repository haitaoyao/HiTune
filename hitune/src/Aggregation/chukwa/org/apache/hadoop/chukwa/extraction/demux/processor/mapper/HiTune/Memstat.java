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
public class Memstat extends Sysstat {

    static Logger log = Logger.getLogger(Memstat.class);
    
    /**
     * 
     */
    public Memstat() {
        init();
    }

    @Override
    void init() {
        headers = new String[]{"Timestamp", "pgpgin/s","pgpgout/s","fault/s","majflt/s","pgfree/s"
                ,"pgscank/s","pgscand/s","pgsteal/s","%vmeff","kbmemfree","kbmemused","%memused"
                ,"kbbuffers","kbcached","kbcommit","%commit","kbswpfree","kbswpused","%swpused","kbswpcad","%swpcad"};
        headermark = "pgpgin";
        
    }
    
    /**
     * @param args
     */
    public static void main(String[] args) {
        // TODO Auto-generated method stub

    }

   

}
