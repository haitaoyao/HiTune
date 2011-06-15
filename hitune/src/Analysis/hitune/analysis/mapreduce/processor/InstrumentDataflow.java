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
import java.io.StringBufferInputStream;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;


import org.apache.hadoop.chukwa.extraction.engine.ChukwaRecord;
import org.apache.hadoop.chukwa.extraction.engine.ChukwaRecordKey;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.UTF8;
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
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import org.xml.sax.SAXException;

import org.xml.sax.SAXException;



/**
 * Re-organize the instrumented data for Map tasks, and get the statistics 
 * 
 *
 */
public class InstrumentDataflow extends AnalysisProcessor {

    static Logger log = Logger.getLogger(InstrumentDataflow.class);

    /**
     * Get each phase's metrics including :
     * 1. its function list, function sampling count
     * 2. its start,end time
     * 3. its status list and status count
     * 4. its function-status count
     * <br> Each output record represents one sampling point.
     */
    public static class MapClass<K extends Record, V extends Record> extends MapReduceBase implements
    Mapper<K, V, K, V>{
        JobConf conf = null;
        List <String> nodelist = new ArrayList<String>();
        Map <String,List<String>> phases = new HashMap<String,List<String>>();
        Map <String,String> phasealias = new HashMap<String,String>();
        List <String> statuslist = new ArrayList<String>();
        @Override
        public void configure(JobConf jobConf) {
            super.configure(jobConf);
            this.conf = jobConf;
            init();
        }

        void parsePhase(){
            DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
            // Now use the factory to create a DOM parser (a.k.a. a DocumentBuilder)
            DocumentBuilder parser;
            try {
                parser = factory.newDocumentBuilder();
                // Parse the file and build a Document tree to represent its content
                Document document = parser.parse(new StringBufferInputStream("<root>"+conf.get("phases") + "</root>"));
                // Ask the document for a list of all phases
                NodeList rows = document.getElementsByTagName(AnalysisProcessorConfiguration.phase);
                int phasenumber = rows.getLength();
                for (int i = 0; i< phasenumber; i++){
                    Node phase = rows.item( i );
                    NodeList fields = phase.getChildNodes();
                    String phasename = null;
                    String stacks = null;
                    String funcs = null;
                    List<String> functionlist = new ArrayList<String>();
                    for (int j = 0; j < fields.getLength(); j++) {
                        Node fieldNode = fields.item(j);
                        if (!(fieldNode instanceof Element))
                            continue;
                        Element field = (Element)fieldNode;
                        if ("phasename".equals(field.getTagName()) && field.hasChildNodes())
                            phasename =  ((org.w3c.dom.Text)field.getFirstChild()).getData().trim();
                        else if ("stack".equals(field.getTagName()) && field.hasChildNodes())
                            stacks = ((org.w3c.dom.Text)field.getFirstChild()).getData();
                        else if ("functions".equals(field.getTagName()) && field.hasChildNodes())
                            funcs = ((org.w3c.dom.Text)field.getFirstChild()).getData();
                    }
                    if(stacks!=null && stacks.length()!=0) stacks = stacks.replace(" ", "");
                    else stacks="";
                    phasealias.put(stacks, phasename);

                    if(funcs == null){
                        continue;
                    }
                    for(String func: funcs.split(SEPERATOR_COMMA)){
                        functionlist.add(func);
                    }
                    this.phases.put(stacks, functionlist);
                }
            } catch (ParserConfigurationException e) {
                // TODO Auto-generated catch block
                log.warn(e);
                e.printStackTrace();
            } catch (SAXException e) {
                // TODO Auto-generated catch block
                log.warn(e);
                e.printStackTrace();
            } catch (IOException e) {
                // TODO Auto-generated catch block
                log.warn(e);
                e.printStackTrace();
            }
        }

        private void init(){
            String nodes = conf.get(AnalysisProcessorConfiguration.nodes);
            this.nodelist = String2List(nodes, SEPERATOR_COMMA);
            String status = conf.get("status");
            this.statuslist = String2List(status, SEPERATOR_COMMA);
            parsePhase();
        }

        private String count(String dest, List<String> patternList){
            StringBuilder results = new StringBuilder();
            if(dest==null || patternList==null || patternList.size()<=0){
                return "";
            }

            for(String pattern : patternList){
                Pattern p = Pattern.compile(pattern);
                Matcher matcher = p.matcher(dest);
                if(matcher.find()){
                    results.append("1").append(SEPERATOR_COMMA);
                }
                else{
                    results.append("0").append(SEPERATOR_COMMA);
                }
            }
            log.debug("results:" + results.toString());
            return results.toString().substring(0, results.length()-1);
        }

        @Override
        public void map(K key, V value,
                OutputCollector<K, V> output,
                Reporter reporter) throws IOException {
            // TODO Auto-generated method stub
            //doing the filter

            //<key,value>
            //<[AttemptID/PhaseStack/PhaseAlias], [ThreadName,ThreadId,starttime,endtime,funlist,funcountlist,statelist,statecountlist,funStateMatric]>
            HiTuneRecord valproxy = new HiTuneRecord(value);
            String hostname = valproxy.getHost();
            String status  = valproxy.getValue("ThreadState");
            String stack = valproxy.getValue("CallStack");
            String attemptID = valproxy.getValue("TaskID");
            log.debug("hostname:" + hostname + " ThreadState:" + status + " stack:" + stack + " attemptID:" + attemptID);
            if(isMatched(this.nodelist,hostname)){
                for(String s : phasealias.keySet()){
                    log.debug("phasealias:" +s);
                    if(s==null || s.length()==0)s="";
                    Pattern p = Pattern.compile(s);
                    if(stack!=null && stack.length()!=0)stack=stack.replace(" ", "");
                    else stack="";
                    Matcher matcher = p.matcher(stack);
                    if(matcher.find()){
                        try{
                            log.debug("find pattern");
                            K newkey = (K) key.getClass().getConstructor().newInstance();
                            V newval = (V) value.getClass().getConstructor().newInstance();

                            HiTuneKey newkeyproxy = new HiTuneKey(newkey);
                            HiTuneRecord newvalproxy = new HiTuneRecord(newval);

                            newkeyproxy.setKey(attemptID + "/" + s + "/" + phasealias.get(s));
                            newkeyproxy.setDataType(new HiTuneKey(key).getDataType());
                            newvalproxy.copyCommonFields(value);


                            newvalproxy.add("thread_id", valproxy.getValue("ThreadID"));
                            newvalproxy.add("thread_name", valproxy.getValue("ThreadName"));
                            newvalproxy.add("attempt_id", attemptID);
                            newvalproxy.add("phase_stack", s);
                            newvalproxy.add("phase_name", phasealias.get(s));
                            newvalproxy.add("start", "" + newvalproxy.getTime());
                            newvalproxy.add("count" , "1");
                            log.debug("status:" + conf.get("status"));
                            newvalproxy.add("statusList", conf.get("status"));
                            newvalproxy.add("statusCount", count(status, this.statuslist));

                            log.debug("funList:" + this.phases.get(s));
                            newvalproxy.add("funList", List2String(this.phases.get(s),SEPERATOR_COMMA));
                            newvalproxy.add("funCount", count(stack, this.phases.get(s)));
                            newvalproxy.add(AnalysisProcessorConfiguration.jobid, conf.get(AnalysisProcessorConfiguration.jobid));

                            log.debug("Key:" + newkeyproxy.toString() + " Record" + newkeyproxy.toString());
                            output.collect((K)newkeyproxy.getObject(), (V)newvalproxy.getObject());
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

    }

    /**
     * Calculate each phase's statistics: 
     * 1. choose minimum start time as the start time.
     * 2. choose maximum end time as the end time.
     * 3. sum the sampling count with certain status.
     * 4. sum the function sampling count
     * 5. sum the phase's count
     * The analyzer won't tell that if the phase is  continuous or not in the time sequence.
     */
    public static class ReduceClass<K extends Record, V extends Record> extends MapReduceBase implements
    Reducer<K, V, Text, TextArrayWritable>{

        static boolean initialized = false;

        /**
         * Add two vectors
         * @param a
         * @param b
         * @param seperator
         * @return
         */
        String vectorAdd(String a, String b, String seperator){
            StringBuilder result = new StringBuilder();
            List<String> list_a = String2List(a,seperator);
            List<String> list_b = String2List(b,seperator);

            if(list_a == null || list_b == null||list_a.size()!=list_b.size() || list_a.size()==0 || list_b.size()==0){
                return "";
            }
            for( int i =0; i< list_a.size(); i++){
                int _a = Integer.parseInt(list_a.get(i));
                int _b = Integer.parseInt(list_b.get(i));
                int sum = _a + _b;
                result.append(sum).append(seperator);
            }
            return result.toString().substring(0, result.length()-seperator.length());
        }

        @Override
        public void reduce(K key, Iterator<V> values,
                OutputCollector<Text, TextArrayWritable> output, Reporter reporter)
        throws IOException {
            // TODO Auto-generated method stub
            //organizing into csv format
            Map<String, String> newRecord = new HashMap<String,String>();
            String []headers = new String[]{"attempt_id","breakdown_count", "breakdown_name", "breakdown_type","host", 
                    "job_id", "phase_count", "phase_end", "phase_name", "phase_stack", "phase_start", 
                    "thread_id", "thread_name"};
            for(String head:headers){
                newRecord.put(head, "");
            }



            long start = -1, end = -1;
            long phaseCount=0; 
            String funcCount="", statusCount="";
            String funcList="", statusList="";
            while(values.hasNext()){
                HiTuneRecord valproxy = new HiTuneRecord(values.next());
                long phaseStart = Long.parseLong(valproxy.getValue("start"));
                long phaseEnd = Long.parseLong(valproxy.getValue("start"));
                start = start == -1 ? phaseStart : Math.min(start,phaseStart);
                end = end == -1 ?phaseEnd : Math.max(end,phaseEnd);
                phaseCount++;
                funcCount = funcCount == "" ? valproxy.getValue("funCount"):vectorAdd(valproxy.getValue("funCount"),funcCount, SEPERATOR_COMMA);
                statusCount = statusCount == "" ? valproxy.getValue("statusCount"):vectorAdd(valproxy.getValue("statusCount"),statusCount, SEPERATOR_COMMA);
                newRecord.put("host", valproxy.getHost());
                newRecord.put("job_id", valproxy.getValue(AnalysisProcessorConfiguration.jobid));
                newRecord.put("phase_stack", valproxy.getValue("phase_stack"));
                newRecord.put("phase_name", valproxy.getValue("phase_name"));
                newRecord.put("attempt_id", valproxy.getValue("attempt_id"));
                newRecord.put("thread_id", valproxy.getValue("thread_id"));
                newRecord.put("thread_name", valproxy.getValue("thread_name"));
                funcList = valproxy.getValue("funList");
                statusList = valproxy.getValue("statusList");
            }

            newRecord.put("phase_start", ""+start);
            newRecord.put("phase_end", ""+end);
            newRecord.put("phase_count", ""+phaseCount);

            if(!initialized){
                TextArrayWritable newValue = new TextArrayWritable(newRecord.keySet().toArray(new String[0]));
                output.collect(null, newValue);
                initialized = true;
            }

            if(!funcCount.equals("")){
                newRecord.put("breakdown_type", "function");
                log.debug("funcList: " + funcList);
                log.debug("funcCount: " + funcCount);
                List <String> tmp = String2List(funcList,SEPERATOR_COMMA );
                List <String> counts = String2List(funcCount,SEPERATOR_COMMA );
                for(int i = 0 ; i < tmp.size(); i++ ){
                    log.debug("function:" +tmp.get(i) + " count:" + counts.get(i));
                    newRecord.put("breakdown_name", tmp.get(i));
                    newRecord.put("breakdown_count", ""+counts.get(i));
                    String [] contents = new String[newRecord.keySet().size()];
                    int j = 0;
                    for (String index:  newRecord.keySet() ){
                        contents[j] = newRecord.get(index);
                        log.debug("content: " + index + "," +contents[j] );
                        j++;

                    }
                    TextArrayWritable newValue = new TextArrayWritable(contents);
                    output.collect(null, newValue);
                    contents=null;
                }
            }
            if(!statusCount.equals("")){
                newRecord.put("breakdown_type", "state");
                log.debug("statusList: " + statusList);
                log.debug("statusCount: " + statusCount);
                List <String> tmp = String2List(statusList,SEPERATOR_COMMA );
                List <String> counts = String2List(statusCount,SEPERATOR_COMMA );
                for(int i = 0 ; i < tmp.size(); i++ ){
                    log.debug("function:" +tmp.get(i) + " count:" + counts.get(i));
                    newRecord.put("breakdown_name", tmp.get(i));
                    newRecord.put("breakdown_count", ""+counts.get(i));
                    String [] contents = new String[newRecord.size()];
                    int j = 0;
                    for (String index:  newRecord.keySet() ){
                        contents[j] = newRecord.get(index);
                        j++;
                        log.debug("content: " + index + "," +contents[i] );
                    }
                    TextArrayWritable newValue = new TextArrayWritable(contents);
                    output.collect(null, newValue);
                    contents=null;
                }
            }
        }

    }
    /**
     * @param conf
     */
    public InstrumentDataflow(Configuration conf) {
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

        JobConf conf = new JobConf(this.conf,InstrumentDataflow.class);
        try{
            conf.setJobName(this.getClass().getSimpleName()+ timestamp);
            conf.setInputFormat(MultiSequenceFileInputFormat.class);
            conf.setMapperClass(InstrumentDataflow.MapClass.class);
            conf.setReducerClass(InstrumentDataflow.ReduceClass.class);
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


            if(this.inputfiles != null){
                log.debug("inputPaths:" + inputfiles);
                FileInputFormat.setInputPaths(conf,inputfiles);
                FileOutputFormat.setOutputPath(conf,new Path(temp_outputPaths));

                //FileInputFormat.setInputPathFilter(conf, evtFileFilter.class);
                //conf.setNumReduceTasks(1);

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
