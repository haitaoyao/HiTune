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
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
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
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.mapred.lib.IdentityMapper;
import org.apache.hadoop.record.Record;
import org.apache.log4j.Logger;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

/**
 * 
 *
 */
public class InstrumentSamplingTop extends AnalysisProcessor {

    static Logger log = Logger.getLogger(InstrumentSamplingTop.class);


    public static class MapClass<K extends Record, V extends Record> extends MapReduceBase implements
    Mapper<K, V, K, V>{
        JobConf conf = null;
        List <String> nodelist = new ArrayList<String>();
        Map <String,List<String>> phases = new HashMap<String,List<String>>();
        Map <String,String> phasealias = new HashMap<String,String>();
        List <String> statuslist = new ArrayList<String>();
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
                e.printStackTrace();
                log.warn(e);
            } catch (SAXException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
                log.warn(e);
            } catch (IOException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
                log.warn(e);
            }
        }

        private void init(){
            String nodes = conf.get(AnalysisProcessorConfiguration.nodes);
            this.nodelist = String2List(nodes, SEPERATOR_COMMA);
            String status = conf.get("status");
            this.statuslist = String2List(status, SEPERATOR_COMMA);
            parsePhase();
        }

        private String getFuncPattern(String dest, List<String> patternList){
            String result = dest;
            if(dest==null ){
                return "";
            }
            if (patternList!=null && patternList.size()>0){
                for(String pattern : patternList){
                    Pattern p = Pattern.compile(pattern);
                    Matcher matcher = p.matcher(dest);
                    if(matcher.find()){
                        result=pattern;
                    }
                }
            }
            return result;
        }

        @Override
        public void map(K key, V value,
                OutputCollector<K, V> output,
                Reporter reporter) throws IOException {
            // TODO Auto-generated method stub
            //doing the filter

            //<key,value>
            //<[AttemptID/PhaseAlias/ThreadName/ThreadId/Func], [Callee,isLast]>
            HiTuneRecord valproxy = new HiTuneRecord(value);
            HiTuneKey keyproxy = new HiTuneKey(key);

            String hostname = valproxy.getHost();
            String status  = valproxy.getValue("ThreadState");
            String stack = valproxy.getValue("CallStack");
            if(stack !=null && stack.length()!=0)stack = stack.replace(" ", "");
            else stack = "";
            String attemptID = valproxy.getValue("TaskID");
            log.debug("hostname:" + hostname + " ThreadState:" + status + " stack:" + stack + " attemptID:" + attemptID);
            if(isMatched(this.nodelist,hostname)){
                if(isMatched(this.statuslist, status)){
                    for(String s : phasealias.keySet()){
                        log.debug("phasealias:" +s);

                        String phase_name = phasealias.get(s);
                        if(s==null || s.length()==0)s="";
                        Pattern p = Pattern.compile(s);
                        if(stack!=null && stack.length()!=0)stack=stack.replace(" ", "");
                        else stack="";
                        Matcher matcher = p.matcher(stack);
                        if(matcher.find()){


                            String thread_id = valproxy.getValue("ThreadID");
                            String thread_name = valproxy.getValue("ThreadName");


                            try {
                                K newkey = (K) key.getClass().getConstructor().newInstance();
                                V newvalue = (V) value.getClass().getConstructor().newInstance();

                                HiTuneRecord newvalproxy = new HiTuneRecord(newvalue);
                                HiTuneKey newkeyproxy = new HiTuneKey(newkey);

                                String[] fcs = stack.split("#");    

                                String[] funcs = new String [fcs.length+2];
                                funcs[0]="_PHASE_";                          
                                funcs[1]=getFuncPattern(stack, this.phases.get(s));

                                System.arraycopy(fcs,0,funcs,2,fcs.length);
                                for (int i =0; i< funcs.length; i++){

                                    newkeyproxy.setKey(attemptID + "/" + phase_name + "/" + thread_name + "/" + thread_id + "/" + funcs[i]);
                                    newkeyproxy.setDataType(keyproxy.getDataType());
                                    newvalproxy.copyCommonFields(value);
                                    newvalproxy.add("func", funcs[i]);
                                    newvalproxy.add("thread_id", thread_id);
                                    newvalproxy.add("thread_name", thread_name);
                                    newvalproxy.add("phase_name",phase_name );
                                    newvalproxy.add("phase_stack", s);
                                    newvalproxy.add("attempt_id", attemptID);
                                    newvalproxy.add("Callee", "1");
                                    if(i==2){
                                        newvalproxy.add("isLast", "1");
                                    }else{
                                        newvalproxy.add("isLast", "0");
                                    }
                                    output.collect((K)newkeyproxy.getObject(), (V)newvalproxy.getObject());
                                }
                            } catch (IllegalArgumentException e) {
                                // TODO Auto-generated catch block
                                e.printStackTrace();
                                log.warn(e);
                            } catch (SecurityException e) {
                                // TODO Auto-generated catch block
                                e.printStackTrace();
                                log.warn(e);
                            } catch (InstantiationException e) {
                                // TODO Auto-generated catch block
                                e.printStackTrace();
                                log.warn(e);
                            } catch (IllegalAccessException e) {
                                // TODO Auto-generated catch block
                                e.printStackTrace();
                                log.warn(e);
                            } catch (InvocationTargetException e) {
                                // TODO Auto-generated catch block
                                e.printStackTrace();
                                log.warn(e);
                            } catch (NoSuchMethodException e) {
                                // TODO Auto-generated catch block
                                e.printStackTrace();
                                log.warn(e);
                            }


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
    Reducer<K, V, K, V>{

        static boolean initialized = false;

        @Override
        public void reduce(K key, Iterator<V> values,
                OutputCollector<K, V> output, Reporter reporter)
        throws IOException {
            // TODO Auto-generated method stub
            //key: <[AttemptID/PhaseAlias/ThreadName/ThreadId/func]>
            long callee_num = 0;
            long lastlevel_callee_num = 0;

            try{
                V val = null;
                HiTuneRecord valproxy = null;
                K newkey = (K) key.getClass().getConstructor().newInstance();
                HiTuneKey newkeyproxy = new HiTuneKey(newkey);
                while(values.hasNext()){
                    val = (V)values.next();
                    valproxy = new HiTuneRecord(val);
                    callee_num += Integer.parseInt(valproxy.getValue("Callee"));
                    lastlevel_callee_num += Integer.parseInt(valproxy.getValue("isLast"));
                }

                V newvalue = (V) val.getClass().getConstructor().newInstance();
                HiTuneRecord newvalproxy = new HiTuneRecord(newvalue);
                newvalproxy.copyCommonFields(val);

                newvalproxy.add("callee_num", ""+callee_num);
                newvalproxy.add("last_level_callee_num", ""+lastlevel_callee_num );
                newvalproxy.add("attempt_id",valproxy.getValue("attempt_id") );
                newvalproxy.add("phase_name",valproxy.getValue("phase_name") );
                newvalproxy.add("phase_stack", valproxy.getValue("phase_stack") );
                newvalproxy.add("thread_name",valproxy.getValue("thread_name") );
                newvalproxy.add("thread_id",valproxy.getValue("thread_id") );
                newvalproxy.add("host", valproxy.getHost());
                newvalproxy.add("func", valproxy.getValue("func") );


                newkeyproxy.setKey(valproxy.getValue("attempt_id") 
                        + "/" + valproxy.getValue("phase_name")
                        + "/" + valproxy.getValue("thread_name")
                        + "/" + valproxy.getValue("thread_id"));
                newkeyproxy.setDataType(new HiTuneKey(key).getDataType());

                output.collect((K)newkeyproxy.getObject(), (V)newvalproxy.getObject());
            } catch (IllegalArgumentException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            } catch (SecurityException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            } catch (InstantiationException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            } catch (IllegalAccessException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            } catch (InvocationTargetException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            } catch (NoSuchMethodException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }

        }



    }

    public static class TopClass<K extends Record, V extends Record> extends MapReduceBase implements
    Reducer<K, V, Text, TextArrayWritable>{
        JobConf conf = null;
        int limitNum = 100;
        static boolean initialized = false;
        boolean funcInStackFormat = false;
        @Override
        public void configure(JobConf jobConf) {
            super.configure(jobConf);
            this.conf = jobConf;
            this.limitNum = conf.getInt(AnalysisProcessorConfiguration.limit, 100);
            this.funcInStackFormat = conf.getBoolean(AnalysisProcessorConfiguration.funcInStackFormat, false);
        }

        class RecordComparator implements Comparator<HiTuneRecord>{
            public int compare(HiTuneRecord r1, HiTuneRecord r2){
                if(r1==r2){
                    log.debug("instance compare" );
                    return 0;
                }else {
                    int result = (int)(Long.parseLong(r2.getValue("callee_num")) 
                            - Long.parseLong(r1.getValue("callee_num")));
                    log.debug("result: " + result);
                    if(result == 0){
                        return r2.getValue("func").compareTo(r1.getValue("func"));
                    }else {
                        return result;
                    }
                }

            }
        }

        @Override
        public void reduce(K key, Iterator<V> values,
                OutputCollector<Text, TextArrayWritable> output,
                Reporter reporter) throws IOException {
            // TODO Auto-generated method stub

            Map<String, String> newRecord = new HashMap<String,String>();

            String []headers =  new String[]{"attempt_id","phase_name", "thread_name", "thread_id", "callee_num", 
                    "last_level_callee_num", "host", "phase_stack", "func", "phase_count"};
            for(String head:headers){
                newRecord.put(head, "");
            }
            if(!initialized){
                TextArrayWritable newValue = new TextArrayWritable(newRecord.keySet().toArray(new String[0]));
                output.collect(null, newValue);
                initialized = true;
            }


            TreeSet <HiTuneRecord> arrays = new TreeSet<HiTuneRecord>(new RecordComparator());

            TreeSet <HiTuneRecord> stackarrays = new TreeSet<HiTuneRecord>(new RecordComparator());
            HiTuneRecord phase = null;
            //log.debug("key: " + key.toString());
            while(values.hasNext()){
                try{
                    HiTuneRecord temp_proxyval = new HiTuneRecord(values.next());

                    V newvalue = (V) temp_proxyval.getObject().getClass().getConstructor().newInstance();
                    HiTuneRecord proxyval = new HiTuneRecord(newvalue);

                    for (String field: temp_proxyval.getFields()){
                        proxyval.add(field, temp_proxyval.getValue(field));
                    }

                    String function = proxyval.getValue("func");
                    log.debug(" val: " + proxyval.toString());

                    if (function.equals("_PHASE_")){
                        phase = proxyval;
                        continue;
                    }else{
                        if(function.indexOf("#")!=-1 ){
                            if(funcInStackFormat){
                                stackarrays.add(proxyval);
                                if(stackarrays.size()> limitNum){
                                    stackarrays.remove(stackarrays.last());
                                }
                            }
                        }else{
                            //log.debug("add new val: " + val);
                            arrays.add(proxyval);
                            if(arrays.size()> limitNum){
                                arrays.remove(arrays.last());
                            }
                        }

                    }

                } catch (IllegalArgumentException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                } catch (SecurityException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                } catch (InstantiationException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                } catch (IllegalAccessException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                } catch (InvocationTargetException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                } catch (NoSuchMethodException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }

            }

            if(funcInStackFormat){
                int len = limitNum<stackarrays.size()?limitNum:stackarrays.size();
                HiTuneRecord[] candidates = stackarrays.toArray(new HiTuneRecord[0]);
                for (int i = 0; i <stackarrays.size(); i++ ){
                    HiTuneRecord val = candidates[i];
                    if(val!=null){
                        newRecord.clear();
                        for(String head : headers){
                            if(head.equals("phase_count")){
                                newRecord.put(head,  phase.getValue("callee_num"));
                            }else{
                                newRecord.put(head, val.getValue(head));
                            }

                        }
                        String [] contents = new String[newRecord.keySet().size()];
                        int j = 0;
                        for (String index:  newRecord.keySet() ){
                            contents[j] = newRecord.get(index);
                            log.debug("content: " + index + "," +contents[j] );
                            j++;

                        }
                        TextArrayWritable newValue = new TextArrayWritable(contents);
                        output.collect(null, newValue);
                    }


                }
            }else {
                int len = limitNum<arrays.size()?limitNum:arrays.size();
                HiTuneRecord[] candidates = arrays.toArray(new HiTuneRecord[0]);

                for (int i = 0; i <len; i++ ){

                    HiTuneRecord val = candidates[i];
                    log.debug("dump val: " + val);
                    if(val!=null){
                        newRecord.clear();
                        for(String head : headers){
                            if(head.equals("phase_count")){
                                newRecord.put(head,  phase.getValue("callee_num"));
                            }else{
                                newRecord.put(head, val.getValue(head));
                            }
                        }
                        String [] contents = new String[newRecord.keySet().size()];
                        int j = 0;
                        for (String index:  newRecord.keySet() ){
                            contents[j] = newRecord.get(index);
                            log.debug("content: " + index + "," +contents[j] );
                            j++;

                        }
                        TextArrayWritable newValue = new TextArrayWritable(contents);
                        output.collect(null, newValue);
                    }


                }

            }
        }

    }
    /**
     * @param conf
     */
    public InstrumentSamplingTop(Configuration conf) {
        super(conf);
        // TODO Auto-generated constructor stub
    }

    /* (non-Javadoc)
     * @see hitune.analysis.mapreduce.processor.AnalysisProcessor#run()
     */
    @Override
    public void run() {
        // TODO Auto-generated method stub
        long timestamp = System.currentTimeMillis();
        try{
            JobConf conf = new JobConf(this.conf,InstrumentSamplingTop.class);
            conf.setJobName(this.getClass().getSimpleName()+"_1_"+ timestamp);

            conf.setInputFormat(MultiSequenceFileInputFormat.class);
            conf.setMapperClass(InstrumentSamplingTop.MapClass.class);
            conf.setReducerClass(InstrumentSamplingTop.ReduceClass.class);

            Class<? extends WritableComparable> outputKeyClass = 
                Class.forName(conf.get(AnalysisProcessorConfiguration.mapoutputKeyClass)).asSubclass(WritableComparable.class);
            Class<? extends Writable> outputValueClass =
                Class.forName(conf.get(AnalysisProcessorConfiguration.mapoutputValueClass)).asSubclass(Writable.class);
            conf.setMapOutputKeyClass(outputKeyClass);
            conf.setMapOutputValueClass(outputValueClass);
            conf.setOutputKeyClass(outputKeyClass);
            conf.setOutputValueClass(outputValueClass);


            conf.setOutputFormat(SequenceFileOutputFormat.class);


            String outputPaths = conf.get(AnalysisProcessorConfiguration.reportfolder) + "/" + conf.get(AnalysisProcessorConfiguration.reportfile);

            String temp_outputPaths = getTempOutputDir(outputPaths );


            if(this.inputfiles != null){
                log.debug("inputPaths:" + inputfiles);
                FileInputFormat.setInputPaths(conf,inputfiles);
                FileOutputFormat.setOutputPath(conf,new Path(outputPaths+ "_1_"+ timestamp));


                try {

                    //first job
                    JobClient.runJob(conf);

                    JobConf secondconf = new JobConf(this.conf,InstrumentSamplingTop.class);
                    secondconf.setJobName(this.getClass().getSimpleName()+"_2_"+ timestamp);
                    secondconf.setInputFormat(SequenceFileInputFormat.class);
                    secondconf.setMapperClass(IdentityMapper.class);
                    secondconf.setReducerClass(InstrumentSamplingTop.TopClass.class);

                    secondconf.setMapOutputKeyClass(outputKeyClass);
                    secondconf.setMapOutputValueClass(outputValueClass);

                    secondconf.setOutputKeyClass(Text.class);
                    secondconf.setOutputValueClass(TextArrayWritable.class);
                    secondconf.setOutputFormat(CSVFileOutputFormat.class);
                    FileInputFormat.setInputPaths(secondconf,outputPaths+ "_1_"+ timestamp);
                    FileOutputFormat.setOutputPath(secondconf,new Path(temp_outputPaths));

                    //second job to get ranking list
                    JobClient.runJob(secondconf);
                    moveResults(secondconf,outputPaths,temp_outputPaths);
                    Path temp = new Path(outputPaths+ "_1_"+ timestamp);
                    temp.getFileSystem(conf).delete(temp);
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
