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

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;

import hitune.analysis.mapreduce.AnalysisProcessorConfiguration;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.StringUtils;
import org.apache.log4j.Logger;

/**
 * 
 *
 */
public class ChukwaTimeBasedFileFilter extends ChukwaFileFilter {

    static Logger log = Logger.getLogger(ChukwaTimeBasedFileFilter.class);
    static final long DAY_IN_SECONDS = 24 * 3600;
    static final long MAX_TIMESTAMP_IN_SECOND = ((long)Math.pow(10, 10));
    
    static SimpleDateFormat day = new java.text.SimpleDateFormat("yyyyMMdd");
    long starttime = -1;
    long endtime = -1;
    
    /**
     * 
     */
    public ChukwaTimeBasedFileFilter(Configuration conf, String pattern) {
        super(conf, pattern);
        starttime = conf.getLong(AnalysisProcessorConfiguration.starttime,-1);
        endtime = conf.getLong(AnalysisProcessorConfiguration.endtime,-1);
        log.debug("starttime: " + starttime  + " endtime: " + endtime + " MAX_TIMESTAMP_IN_SECOND: " + MAX_TIMESTAMP_IN_SECOND );
        if(((int)(starttime / MAX_TIMESTAMP_IN_SECOND)) <  1){
            starttime = starttime * 1000;
        }
        
        if(((int)(endtime / MAX_TIMESTAMP_IN_SECOND)) < 1){
            endtime = endtime * 1000;
        }
    }
    
    
    protected long getDate(long timestamp_million){
        Calendar canlendar = getTime(timestamp_million);
        canlendar.set(Calendar.HOUR, 0);
        canlendar.set(Calendar.MINUTE, 0);
        canlendar.set(Calendar.SECOND,0);
        canlendar.set(Calendar.MILLISECOND, 0);
        return canlendar.getTimeInMillis();
    }
    
    protected Calendar getTime(long timestamp_million){
        Calendar canlendar = Calendar.getInstance();
        canlendar.setTimeInMillis(timestamp_million);
        return canlendar;
    }
     
    protected void getDayPath(String day, String parentpath, StringBuilder newPath, Configuration conf){
        log.debug("day: " + day + " parentpath: " + parentpath);
        if(inputValidation(conf, parentpath + "/" + day + "/*.evt", null)){
            if(newPath.length()!=0){
                newPath.append(SEPARATOR);    
            }
            newPath.append(parentpath + "/" + day + "/*.evt" );   
        }
        if(inputValidation(conf, parentpath + "/" + day + "/*/*.evt" , null)){
            if(newPath.length()!=0){
                newPath.append(SEPARATOR);    
            }
            newPath.append(parentpath + "/" + day + "/*/*.evt" );
            
        }
        if(inputValidation(conf, parentpath + "/" + day + "/*/*/*.evt", null)){
            if(newPath.length()!=0){
                newPath.append(SEPARATOR);    
            }
            newPath.append(parentpath + "/" + day + "/*/*/*.evt" );
            
        }
    }
    
    /**
     * 
     * @param starttimestamp in millisecond
     * @param endtimestamp in millisecond
     * @return
     */
    protected List<String> passedDay(long starttimestamp, long endtimestamp){
        List<String> dates = new ArrayList<String>();
        long start = getDate(starttimestamp);
        long end = getDate(endtimestamp);
        long date = start ;
        log.debug("start:" + start);
        log.debug("end:" + end);
        while (date <= end){
            log.debug("date:" + date);
            dates.add(day.format(getTime(date).getTime()));
            date += (DAY_IN_SECONDS *1000);
        }
        return dates;
    }

    /* (non-Javadoc)
     * @see hitune.analysis.mapreduce.processor.FileFilter#filter(org.apache.hadoop.fs.Path)
     */
    @Override
    public String filter(Path toScan) {
        // TODO Auto-generated method stub
        
        StringBuilder inputPath = new StringBuilder();

        for (String day : passedDay(starttime , endtime )){
             log.debug("path item: " + toScan);
             getDayPath(day, toScan.toString(), inputPath ,conf);
        }
        
        return inputPath.toString();
    }

    /**
     * @param args
     */
    public static void main(String[] args) {
        // TODO Auto-generated method stub

    }

}
