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
import hitune.analysis.mapreduce.processor.FileFilter.FileFilter;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Iterator;
import java.util.List;


import org.apache.hadoop.chukwa.extraction.engine.ChukwaRecord;
import org.apache.hadoop.chukwa.extraction.engine.ChukwaRecordKey;
import org.apache.hadoop.chukwa.extraction.engine.Record;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputCommitter;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.lib.IdentityMapper;
import org.apache.hadoop.mapred.lib.IdentityReducer;
import org.apache.hadoop.mapred.lib.NLineInputFormat;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;


/**
 * The analysis processing thread, which will invoke a Map/Reduce job to do a specific analysis job.
 * 
 */
public abstract class AnalysisProcessor implements Runnable {

    static Logger log = Logger.getLogger(AnalysisProcessor.class);
    private Thread thread = null;
    String source = "";
    Configuration conf = null;
    static final String SEPERATOR_COMMA = ",";
    static final long DAY_IN_SECONDS = 24 * 3600;
    static SimpleDateFormat day = new java.text.SimpleDateFormat("yyyyMMdd");
    protected boolean MOVE_DONE = false; 

    String inputfiles = null;

    /**
     * Temp report folder to store the reports before all analysis jobs are done.
     */
    static final String REPORT_TMP = "_TMP";



    /**
     * 
     */
    public AnalysisProcessor(Configuration conf) {
        // TODO Auto-generated constructor stub
        this.conf = conf;
        log.debug(this.conf.get("tmpjars"));
        thread = new Thread(this);
        //To create report folder
        GenReportHome();
    }

    private void GenReportHome(){
        try {
            FileSystem fs = FileSystem.get(this.conf);
            Path reportfolder = new Path(this.conf.get(AnalysisProcessorConfiguration.reportfolder));
            if(!fs.exists(reportfolder)) fs.mkdirs(reportfolder);
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
            log.error("Cannot create report folder");
        }
    }

    protected String getTempOutputDir(String outputfolder){
        return outputfolder + REPORT_TMP + "/" + getOutputFileName();
    }

    public String getOutputFileName(){
        return this.conf.get(AnalysisProcessorConfiguration.outputfilename);
    }

    

    static protected List<String> String2List(String src, String seperator){
        List<String> results = null;
        if(src == null || src.equals("") || src.equals("null") || src.equals("*")){
            return results;
        }
        else {
            results = new ArrayList<String>();
            if(src.indexOf(seperator)!=-1){

                for (String item : src.split(seperator)){
                    results.add(item);
                }
            }
            else {
                results.add(src);
            }
        }
        return results;
    }

    static protected String List2String(List<String> list, String seperator){
        StringBuilder result = new StringBuilder();
        if(list==null || list.size()<=0){
            return "";
        }
        for(String item : list){
            result.append(item).append(seperator);
        }
        return result.toString().substring(0, result.length()+ 0-seperator.length());
    }



    public void start(){
        if(!init()){
            log.error("AnalyzerProcessor: " + this.getClass().getSimpleName() + "'s output: " + getOutputFileName() + " intializing failed");
        }
        if(thread!=null ){
            thread.start();
            log.info("AnalyzerProcessor: " + this.getClass().getSimpleName() + "'s output: " + getOutputFileName() + " started...");
        }

    }

    public void join() throws InterruptedException{
        if(thread!=null){
            thread.join();
            if(getStatus()){
                log.info("AnalyzerProcessor: " + this.getClass().getSimpleName() + "'s output: " + getOutputFileName() + " SUCCESS!");
            }
            else {
                log.info("AnalyzerProcessor: " + this.getClass().getSimpleName() + "'s output: " + getOutputFileName() + " FAILED!");
            }
        }else{
            log.info("AnalyzerProcessor: " + this.getClass().getSimpleName() + "'s output: " + getOutputFileName() + " FAILED!");
        }

    }


    

    public boolean getStatus(){
        return MOVE_DONE;
    }

    

    /**
     * Merge the output file into one, and only emit the header(field name) once.
     * 
     * @param <K>
     * @param <V>
     */
    public static class NullKeyIdentityReducer<K, V> extends MapReduceBase implements Reducer<K, V, K, V> {
        static boolean isHeader = true;
        public void reduce(K key, Iterator<V> values,
                OutputCollector<K, V> output, Reporter reporter)
        throws IOException { 
            while (values.hasNext()) {
                output.collect(null, values.next());
                if(isHeader){
                    isHeader = false;
                    break;
                }
            }
        }
    }


    /**
     * Merge multiple output file into one file and only emit the header of csv once.
     * 
     *
     */
    class MergeOutput  extends Configured implements Tool {


        Configuration configure = null;
        public MergeOutput(Configuration conf){
            this.configure=conf;
        }
        @Override
        public int run(String[] args) throws Exception {
            // TODO Auto-generated method stub
            JobConf conf = new JobConf(this.configure, AnalysisProcessor.class);

            conf.setJobName("MergeOutputFile");

            conf.setInputFormat(TextInputFormat.class);
            conf.setMapperClass(IdentityMapper.class);
            conf.setReducerClass(NullKeyIdentityReducer.class);

            conf.setMapOutputKeyClass(LongWritable.class);
            conf.setMapOutputValueClass(Text.class);

            conf.setOutputKeyClass(Text.class);
            conf.setOutputValueClass(Text.class);
            conf.setOutputFormat(CSVFileOutputFormat.class);

            conf.setNumReduceTasks(1);
            FileInputFormat.setInputPaths(conf, args[0]);
            FileOutputFormat.setOutputPath(conf, new Path(args[1]));

            JobClient.runJob(conf);
            return 0;
        }


    }

    /**
     * Move the TEMP output folder to final one(user defined one);
     * If there are multiple files under one job's output folder, it should merge the output into one file.
     * Then rename the folder to the final one.
     * @param job
     * @param output
     * @param result
     */
    protected void moveResults(JobConf job,String output, String result){
        try {
            FileSystem fs = FileSystem.get(job);
            log.debug("move results: " +result);
            Path src = new Path(result+"/"+"*.csv*");
            Path dst = new Path(output);
            if(!fs.exists(dst)){
                fs.mkdirs(dst);
            }
            FileStatus[] matches = fs.globStatus(src, new PathFilter(){
                @Override
                public boolean accept(Path path) {
                    // TODO Auto-generated method stub
                    return true;

                }});
            if(matches!=null && matches.length!=0){
                if(matches.length > 1){
                    //multiple output files
                    String []args = new String[2];
                    args[0]= result;
                    args[1]= "_"+ result;
                    fs.delete(new Path("_"+ result));
                    //merge multiple output files into one file
                    ToolRunner.run(new MergeOutput(this.conf), args);
                    fs.delete(new Path(result));
                    fs.rename(new Path("_"+ result), new Path(result));
                }

                matches = fs.globStatus(src,new PathFilter(){
                    @Override
                    public boolean accept(Path path) {
                        // TODO Auto-generated method stub
                        return true;                     
                    }});

                for(FileStatus file : matches){
                    String filename = file.getPath().getName();
                    filename = filename.substring(0,filename.indexOf("-"));
                    log.debug("move file:" + filename);
                    Path toFile = new Path(output+"/"+filename);
                    if(fs.exists(toFile)){
                        fs.delete(toFile);
                    }
                    fs.rename(file.getPath(), toFile);
                    fs.delete(file.getPath().getParent(), true);
                    FileStatus[] tmpDirs = fs.listStatus(file.getPath().getParent().getParent());
                    if(tmpDirs == null || tmpDirs.length == 0){
                        fs.delete(file.getPath().getParent().getParent(), true);
                    }
                    break;
                }
            }
            else{
                MOVE_DONE = false;
            }
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
            MOVE_DONE = false;
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        MOVE_DONE = true;
    }

    static protected boolean isMatched(List filterlist, String target){
        return (filterlist==null || filterlist.isEmpty() || filterlist.contains(target));
    }

    /* (non-Javadoc)
     * @see java.lang.Runnable#run()
     */
    @Override
    abstract public void run();

    public boolean init(){
        return parsingInputPath();
    }


    protected boolean parsingInputPath(){
        if(conf!=null){
            String filterclass = conf.get(AnalysisProcessorConfiguration.filefilter);
            if(filterclass ==null || filterclass.equals("")){
                filterclass = "hitune.analysis.mapreduce.processor.FileFilter.DefaultFileFilter";
            }

            String [] paths = conf.getStrings(AnalysisProcessorConfiguration.datasource);
            String pattern = conf.get(AnalysisProcessorConfiguration.filefilter_pattern, null);
            StringBuilder str = new StringBuilder();

            for(String path : paths){
                log.debug("path to scan: " + path);
                FileFilter filter = null;
                try {
                    filter = (FileFilter)Class.forName(filterclass).getConstructor(new Class[] { Configuration.class, String.class }).newInstance(new Object[] {conf, pattern});

                    if(str.length()!=0){
                        str.append(FileFilter.SEPARATOR);
                    }
                    str.append(filter.filter(new Path(path)));

                    
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
                } catch (ClassNotFoundException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
            }
            if(str == null || str.equals("") || str.length() == 0){
                log.error("No input file is met the filtering requirments");
                return false;
            }
            else{
                inputfiles = str.toString();
                return true;
            }
        }
        else{
            return false;
        }
        
    }


    /**
     * @param args
     */
    public static void main(String[] args) {
        // TODO Auto-generated method stub

    }

}
