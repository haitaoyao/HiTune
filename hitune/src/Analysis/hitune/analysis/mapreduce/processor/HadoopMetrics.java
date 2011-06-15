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
package hitune.analysis.mapreduce.processor;

import hitune.analysis.mapreduce.AnalysisProcessorConfiguration;
import hitune.analysis.mapreduce.CSVFileOutputFormat;
import hitune.analysis.mapreduce.HiTuneRecord;
import hitune.analysis.mapreduce.MultiSequenceFileInputFormat;
import hitune.analysis.mapreduce.TextArrayWritable;


import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;


import org.apache.hadoop.chukwa.extraction.engine.ChukwaRecord;
import org.apache.hadoop.chukwa.extraction.engine.ChukwaRecordKey;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.record.Record;
import org.apache.log4j.Logger;


/**
 * Filter the metrics data between user defined period of time. 
 * The job only reports out those focusing(chosen by user in the configuration file) metrics, 
 * in case overloading for the results diagnosis.
 * 
 *
 */
public class HadoopMetrics extends AnalysisProcessor {

    static Logger log = Logger.getLogger(HadoopMetrics.class);

    /**
     * Filter each record with the time, metrics, and node list
     * 
     *
     */
    public static class MapClass<K extends Record, V extends Record> extends MapReduceBase implements
    Mapper<K, V, K, V>{


        static List <String> nodelist = new ArrayList<String>();
        static List <String> metrics = new ArrayList<String>();
        static long starttime = 0;
        static long endtime = 0;

        JobConf conf = null;

        @Override
        public void configure(JobConf jobConf) {
            super.configure(jobConf);
            this.conf = jobConf;
            init();
        }

        private void init(){
            this.starttime = Long.parseLong(conf.get(AnalysisProcessorConfiguration.starttime));
            this.endtime = Long.parseLong(conf.get(AnalysisProcessorConfiguration.endtime));
            String nodes = conf.get(AnalysisProcessorConfiguration.nodes);
            this.nodelist = String2List(nodes, SEPERATOR_COMMA);
            String metrics = conf.get(AnalysisProcessorConfiguration.metrics);
            this.metrics = String2List(metrics,SEPERATOR_COMMA );
        }

        @Override
        public void map(K key, V value,
                OutputCollector<K, V> output,
                Reporter reporter) throws IOException {
            // TODO Auto-generated method stub
            //doing the filter

            String category = this.conf.get(AnalysisProcessorConfiguration.category);
            HiTuneRecord valproxy = new HiTuneRecord(value);
            
            long timestamp = valproxy.getTime();
            String hostname = valproxy.getHost();
            
            if(timestamp >= this.starttime && timestamp <= this.endtime){
                if(isMatched(this.nodelist,hostname)){
                   
                    try {
                        V newvalue = (V) value.getClass().getConstructor().newInstance();
                        HiTuneRecord newvalproxy = new HiTuneRecord(newvalue);
                        newvalproxy.copyCommonFields(value);
                        
                                        
                        
                        for(String metric : this.metrics){
                            String val = valproxy.getValue(metric);
                            if(val == null){
                                val = "";
                            }
                            newvalproxy.add(metric,val);
                            if(category!=null){
                                newvalproxy.add(AnalysisProcessorConfiguration.category,category);
                            }
                        }
                        output.collect(key, (V) newvalproxy.getObject());
                    } catch (IllegalArgumentException e) {
                        // TODO Auto-generated catch block
                        log.warn(e);
                        e.printStackTrace();
                    } catch (SecurityException e) {
                        // TODO Auto-generated catch block
                        log.warn(e);
                        e.printStackTrace();
                    } catch (InstantiationException e) {
                        // TODO Auto-generated catch block
                        log.warn(e);
                        e.printStackTrace();
                    } catch (IllegalAccessException e) {
                        // TODO Auto-generated catch block
                        log.warn(e);
                        e.printStackTrace();
                    } catch (InvocationTargetException e) {
                        // TODO Auto-generated catch block
                        log.warn(e);
                        e.printStackTrace();
                    } catch (NoSuchMethodException e) {
                        // TODO Auto-generated catch block
                        log.warn(e);
                        e.printStackTrace();
                    }
                    

                }
            }
        }

    }

   


    /**
     * @param conf
     */
    public HadoopMetrics(Configuration conf) {
        super(conf);
        // TODO Auto-generated constructor stub
    }

    
    /* (non-Javadoc)
     * @see org.apache.hadoop.chukwa.analysis.HiTune.AnalysisProcessor#run()
     */
    @Override
    public void run() {
        // TODO Auto-generated method stub
        long timestamp = System.currentTimeMillis();
        JobConf conf = new JobConf(this.conf,HadoopMetrics.class);
        try{
            conf.setJobName(this.getClass().getSimpleName()+ timestamp);
            conf.setInputFormat(MultiSequenceFileInputFormat.class);
            conf.setMapperClass(HadoopMetrics.MapClass.class);
            conf.setReducerClass(SystemLog.ReduceClass.class);
            Class<? extends WritableComparable> outputKeyClass = 
                Class.forName(conf.get(AnalysisProcessorConfiguration.mapoutputKeyClass)).asSubclass(WritableComparable.class);
            Class<? extends Writable> outputValueClass =
                Class.forName(conf.get(AnalysisProcessorConfiguration.mapoutputValueClass)).asSubclass(Writable.class);
            conf.setMapOutputKeyClass(outputKeyClass);
            conf.setMapOutputValueClass(outputValueClass);

            conf.setOutputKeyClass(Text.class);

            conf.setOutputValueClass(TextArrayWritable.class);
            conf.setOutputFormat(CSVFileOutputFormat.class);

            String outputPaths = conf.get(AnalysisProcessorConfiguration.reportfolder) + "/" + conf.get(AnalysisProcessorConfiguration.reportfile);
            String temp_outputPaths = getTempOutputDir(outputPaths );

            if(this.inputfiles != null){
                log.debug("inputPaths:" + inputfiles);
                FileInputFormat.setInputPaths(conf,inputfiles);
                FileOutputFormat.setOutputPath(conf,new Path(temp_outputPaths));
                try {
                    JobClient.runJob(conf);
                    moveResults(conf,outputPaths,temp_outputPaths);
                } catch (IOException e) {
                    // TODO Auto-generated catch block
                    log.warn("For " + getOutputFileName() + " :JOB fails!");
                    log.warn(e);
                    e.printStackTrace();
                    this.MOVE_DONE = false;
                }

            }
            else{
                log.warn( "For " + getOutputFileName() + " :No input path!");
            }

        }catch(Exception e){
            
            log.warn("Job preparation failure!");
            log.warn(e);
            e.printStackTrace();
        }

    }

    /**
     * @param args
     */
    public static void main(String[] args) {
        // TODO Auto-generated method stub

    }

}
