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
package hitune.analysis.mapreduce.processor.FileFilter;

import hitune.analysis.mapreduce.AnalysisProcessorConfiguration;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.log4j.Logger;

/**
 * 
 *
 */
public class ChukwaJobBasedFileFilter extends ChukwaFileFilter {

    static Logger log = Logger.getLogger(ChukwaJobBasedFileFilter.class);
    
    String jobid = null;
    
    
    /**
     * @param conf
     */
    public ChukwaJobBasedFileFilter(Configuration conf, String pattern){
        super(conf, pattern);
        jobid = conf.get(AnalysisProcessorConfiguration.jobid);
        log.debug("pattern: " + pattern);
    }

    /* (non-Javadoc)
     * @see hitune.analysis.mapreduce.processor.FileFilter#filter(org.apache.hadoop.fs.Path)
     */
    @Override
    public String filter(Path toScan) {
        if(jobid == null || jobid.equals("") ){
            return null;
        }
        else {
            log.debug("to scan job folder: "+ toScan.toString()+"/" + jobid);
            Path jobpath = new Path(toScan.toString()+"/" + jobid);
            StringBuilder str = new StringBuilder();
            scan(jobpath, str);
            log.debug("file list: "+str.toString());
            return str.toString();
        }
   
    }

    /**
     * @param args
     */
    public static void main(String[] args) {
        // TODO Auto-generated method stub

    }

}
