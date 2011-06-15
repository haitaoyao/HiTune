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
import hitune.analysis.mapreduce.HiTuneKey;
import hitune.analysis.mapreduce.HiTuneRecord;
import hitune.analysis.mapreduce.MultiSequenceFileInputFormat;
import hitune.analysis.mapreduce.TextArrayWritable;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.chukwa.extraction.engine.ChukwaRecord;
import org.apache.hadoop.chukwa.extraction.engine.ChukwaRecordKey;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
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
 * 
 *
 */
public class HistoryLog extends AnalysisProcessor {

    static Logger log = Logger.getLogger(HistoryLog.class);
    /**
     * Filter the record with job id and metric name.
     * 
     *
     */
    public static class MapClass<K extends Record, V extends Record> extends MapReduceBase implements
    Mapper<K, V, K, V>{   

        @Override
        public void map(K key, V value,
                OutputCollector<K, V> output,
                Reporter reporter) throws IOException {
            // TODO Auto-generated method stub
            //doing the filter
            // Get the records of same job id
            HiTuneRecord valproxy = new HiTuneRecord(value);
            HiTuneKey keyproxy = new HiTuneKey(key);

            log.debug(key.toString());
            log.debug(value.toString());
            try {
                K newkey = (K) key.getClass().getConstructor().newInstance();
                HiTuneKey newkeyproxy = new HiTuneKey(newkey);
                String []parts = keyproxy.getDataType().split("/");
                String recordType = parts[parts.length-1];
                log.debug("record_type: " + recordType);
                if(recordType.equals("Job")){
                    newkeyproxy.setKey(valproxy.getValue("JOBID"));
                }
                else if(recordType.equals("MapAttempt")){
                    newkeyproxy.setKey(valproxy.getValue("TASK_ATTEMPT_ID"));
                }
                else if(recordType.equals("MapTask")){
                    newkeyproxy.setKey(valproxy.getValue("TASKID"));
                }
                else if(recordType.equals("ReduceAttempt")){
                    newkeyproxy.setKey(valproxy.getValue("TASK_ATTEMPT_ID"));
                }
                else if(recordType.equals("ReduceTask")){
                    newkeyproxy.setKey(valproxy.getValue("TASKID"));
                }
                if (newkeyproxy.getKey()!=null && newkeyproxy.getKey().length()!=0){
                    newkeyproxy.setDataType(keyproxy.getDataType());
                    output.collect((K) newkeyproxy.getObject(),(V) value);
                }
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

    public static class ReduceClass<K extends Record, V extends Record>  extends MapReduceBase implements
    Reducer<K, V, Text, TextArrayWritable>{

        static boolean initialized = false;
        static List <String> metrics = new ArrayList<String>();
        JobConf conf = null;

        @Override
        public void configure(JobConf jobConf) {
            super.configure(jobConf);
            this.conf = jobConf;
            init();
        }

        private void init(){
            String metrics = conf.get(AnalysisProcessorConfiguration.metrics);
            this.metrics = String2List(metrics,SEPERATOR_COMMA );
        }

        @Override
        public void reduce(K key, Iterator<V> values,
                OutputCollector<Text, TextArrayWritable> output, Reporter reporter)
        throws IOException {
            // TODO Auto-generated method stub
            //organizing into csv format
            Text newKey = null;
            TextArrayWritable newValue = null;

            Map<String, String> map = new HashMap<String, String>();
            Map<String, String> filter = new HashMap<String, String>();
            HiTuneRecord valproxy = null;
            while(values.hasNext()){
                valproxy= new HiTuneRecord(values.next());
                //copyGernetic(record, newrecord);
                for(String metric : valproxy.getFields()){
                    log.debug("copy metric:" + metric);
                    String val = valproxy.getValue(metric);
                    if(val == null){
                        val = "";
                    }
                    map.put(metric,val);
                }
            }
            if(valproxy!=null){
                //do filtering
                filter.putAll(valproxy.getCommonFields());

                for(String metric : this.metrics){
                    log.debug("collect metric:" + metric);
                    String val = map.get(metric);
                    if(val == null){
                        val = "";
                    }
                    filter.put(metric,val);
                }
                filter.put("id", new HiTuneKey(key).getKey());


                String [] fields = filter.keySet().toArray(new String[0]);
                if(!initialized){
                    newValue = new TextArrayWritable(fields);
                    output.collect(newKey, newValue);
                    initialized = true;
                    newValue = null;
                }
                String [] contents = new String[fields.length];
                for (int i=0; i<fields.length; i++  ){
                    contents[i] = filter.get(fields[i]);
                }
                newValue = new TextArrayWritable(contents);
                output.collect(newKey, newValue);
            }
        }

    }
    /**
     * @param conf
     */
    public HistoryLog(Configuration conf) {
        super(conf);
        // TODO Auto-generated constructor stub
    }

    public void run() {
        // TODO Auto-generated method stub
        long timestamp = System.currentTimeMillis();
        JobConf conf = new JobConf(this.conf,HistoryLog.class);
        try{
            conf.setJobName(this.getClass().getSimpleName()+ timestamp);
            conf.setInputFormat(MultiSequenceFileInputFormat.class);
            conf.setMapperClass(HistoryLog.MapClass.class);
            conf.setReducerClass(HistoryLog.ReduceClass.class);
            conf.setOutputKeyClass(Text.class);

            Class<? extends WritableComparable> outputKeyClass = 
                Class.forName(conf.get(AnalysisProcessorConfiguration.mapoutputKeyClass)).asSubclass(WritableComparable.class);
            Class<? extends Writable> outputValueClass =
                Class.forName(conf.get(AnalysisProcessorConfiguration.mapoutputValueClass)).asSubclass(Writable.class);
            conf.setMapOutputKeyClass(outputKeyClass);
            conf.setMapOutputValueClass(outputValueClass);

            conf.setOutputValueClass(TextArrayWritable.class);
            conf.setOutputFormat(CSVFileOutputFormat.class);


            String outputPaths = conf.get(AnalysisProcessorConfiguration.reportfolder) + "/" + conf.get(AnalysisProcessorConfiguration.reportfile);
            String temp_outputPaths = getTempOutputDir(outputPaths );



            if(this.inputfiles !=null){
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
