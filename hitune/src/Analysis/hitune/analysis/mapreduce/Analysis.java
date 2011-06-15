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
package hitune.analysis.mapreduce;


import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.chukwa.conf.ChukwaConfiguration;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

import hitune.analysis.mapreduce.processor.AnalysisProcessorManager;



/**
 * Analysis executable class to run the analysis Map/Reduce job
 *
 */
public class Analysis {

    static Logger log = Logger.getLogger(Analysis.class);
    String starttime = null;
    String endtime = null;
    String jobid = null;
    String testname = null;
    String analysisConf = null;
    static ChukwaConfiguration chukwaconf = null;
    /**
     * 
     */
    public Analysis() {
        // TODO Auto-generated constructor stub
    }

    private void printUsage(String prompts){
        if(prompts !=null && !prompts.equals("")){
            System.out.println(prompts);
        }
        System.out.println("  Please configure either the [ANALYSIS PARAMS] or the [ ANALYSISCONFFILE ]");
        System.out.println("  CMD [-id JOBID]|[-s START_TIME -e END_TIME -id JOBID -n TESTNAME ] | [ -f ANALYSISCONFFILE ]");
        
        System.out.println("      -s START_TIME:        Start time in seconds. " + 
                "a) You can get this value from the your job's historylog file, copy the X part in SUBMIT_TIME=\"{XXXXXXXXXX}YYY\"" +
                "b) Or you can get the start time from the Hadoop web page, for example: \"Started at: Wed Mar 23 08:00:58 CST 2011\". " +
        "And then use a shell command to get its timestamp \"$> date +%s -d 'Wed Mar 23 08:00:58 CST 2011'\"");
        
        System.out.println("      -e END_TIME:          End time in seconds. " + 
                "a) You can get this value from the your job's historylog file, copy the X part in FINISH_TIME=\"{XXXXXXXXXX}YYY\"" +
                "b) Or you can get the start time from the Hadoop web page, for example: \"Finished at: Wed Mar 23 08:00:58 CST 2011\". " +
        "And then use a shell command to get its timestamp \"$> date +%s -d 'Wed Mar 23 08:00:58 CST 2011'\"");
        System.out.println("      -n TESTNAME:          Test name." +
        "The analysis engine will create a sub folder under ${HiTune.output.reportfolder.home} named as this value");
        
        System.out.println("      -id JOBID:            The base id for single Hadoop job." +
        "Just extract the number part of job_{XXXXXXXXXXXX_XXXX}");
        
        System.out.println("      -f ANALYSISCONFFILE:  The analysis configuration xml file. You can find the analysis-conf.xml.template under conf/");
        System.out.println("");
        System.out.println("  For example: to analysis job_id=201101031416_0001, the result is named as 201101031416_0001, if there exists a file named as 201101031416_0001.xml under /.JOBS");
        System.out.println("      CMD -id 201101031416_0001");
        System.out.println("  For example: to analysis job_id=201101031416_0001 which starts from 1293850871 and ends at 1293851371 and store the final reports under my_first_try folder ");
        System.out.println("      CMD -id 201101031416_0001 -s 1293850871 -e 1293851371 -n my_first_try");
    }

    private boolean parseArgs(String[] args){
        if(args.length == 0){
            log.error("No argument found!Please add the analysis configuration file or certain parameters.");
            printUsage("[ERROR]No argument found!Please add the analysis configuration file or certain parameters.");
            return false;
        }
        else {
            for (int i = 0; i< args.length - 1; i++){
                if(args[i].equals("-s")){
                    starttime = args[++i];
                }else if(args[i].equals("-e")){
                    endtime = args[++i];
                }else if(args[i].equals("-id")){
                    jobid = args[++i];
                }else if(args[i].equals("-n")){
                    testname = args[++i];
                }else if(args[i].equals("-f")){
                    analysisConf = args[++i];
                }else{
                    log.error("UNKNOWN arguments: " + args[i]);
                    printUsage("[ERROR]UNKNOWN arguments: " + args[i]);
                    return false;
                }
            }
            if(( jobid==null )&& analysisConf==null){
                log.error("Missing arguments!");
                printUsage("[ERROR]Missing arguments!");
                return false;
            }else {
                chukwaconf = new ChukwaConfiguration(true);
                if(analysisConf==null){
                    //set job id
                    chukwaconf.set(AnalysisProcessorConfiguration.baseid, jobid);
                    try {
                        Path jobconf = new Path("/.JOBS/" + jobid + ".xml");
                        FileSystem fs = jobconf.getFileSystem(chukwaconf);
                        
                        if(fs.exists(jobconf)){
                            ChukwaConfiguration tempconf = new ChukwaConfiguration(false);
                            tempconf.addResource(fs.open(jobconf));
                            if(testname==null || testname.equals("")) testname = tempconf.get(AnalysisProcessorConfiguration.reportfile);
                            if(starttime==null || starttime.equals(""))starttime = tempconf.get(AnalysisProcessorConfiguration.basestart);
                            if(endtime==null || endtime.equals(""))endtime = tempconf.get(AnalysisProcessorConfiguration.baseend);
                        }else {
                            log.warn("The analysis conf file for " + jobid + " (" + jobconf.toString() + ") doesn't exist." );
                            System.out.println("[WARN]The analysis conf file for " + jobid + " (" + jobconf.toString() + ") doesn't exist." );
                            if(starttime == null || endtime==null || testname==null){
                                log.error("Missing arguments!");
                                printUsage("[ERROR]Missing arguments!");
                                return false;
                            }
                        }
                    } catch (IOException e) {
                        // TODO Auto-generated catch block
                        e.printStackTrace();
                    }
                    if(jobid!=null)chukwaconf.set(AnalysisProcessorConfiguration.baseid, jobid);
                    if(testname!=null)chukwaconf.set(AnalysisProcessorConfiguration.reportfile, testname);
                    if(starttime!=null)chukwaconf.set(AnalysisProcessorConfiguration.basestart, starttime);
                    if(endtime!=null)chukwaconf.set(AnalysisProcessorConfiguration.baseend, endtime);
                    
                }else {
                    //to load the configuration file at first
                    File confFile = new File(analysisConf);
                    if(!confFile.exists()||!confFile.isFile()){
                        log.error("Analysis configuration file " +analysisConf +" doesn't exists or isn't a file!");
                        return false;
                    }
                    confFile=null;
                    chukwaconf.addResource(new Path(analysisConf));
                    
                    //to overwrite the certain params
                    if(starttime!=null)chukwaconf.set(AnalysisProcessorConfiguration.basestart, starttime);
                    if(endtime!=null)chukwaconf.set(AnalysisProcessorConfiguration.baseend, endtime);
                    if(jobid!=null)chukwaconf.set(AnalysisProcessorConfiguration.baseid, jobid);
                    if(testname!=null)chukwaconf.set(AnalysisProcessorConfiguration.reportfile, testname);
                }
                return true;
            }
        }
    }

    public void run(String[] args){
        //check & configure the arguments
        if(parseArgs(args)){
            String reportConfFolder = null;
            //Get the report configuration folder from chukwa-configuration file
            reportConfFolder = chukwaconf.get(AnalysisProcessorConfiguration.reportconf, System.getenv("HITUNE_HOME") + "/" + "conf" + "/" + "report");
            if(!new File(reportConfFolder).exists()){
                log.error("Cannot parse the configuration files! Report Configuration Folder: " + reportConfFolder + " doesn't exist.");
                return;
            }
            chukwaconf.set(AnalysisProcessorConfiguration.reportconf, reportConfFolder);
            if(reportConfFolder!=null){
                //pass down high level system properties
                Iterator iterator=System.getProperties().entrySet().iterator();
                while(iterator.hasNext()){
                    Map.Entry<String,String> entry=(Map.Entry)iterator.next();
                    chukwaconf.set(entry.getKey(), entry.getValue());
                    log.debug(entry.getKey()+","+ entry.getValue());
                }
                
                AnalysisConfiguration conf = new AnalysisConfiguration(reportConfFolder, chukwaconf);
                //According to source data to start processor
                Map <String, List <Configuration>> conflist = conf.getConfList();
                AnalysisProcessorManager processorManager = new AnalysisProcessorManager(conflist);
                processorManager.start();
                //Join here
                processorManager.join();
                //Move TMP to Error
                
                //Done
                log.info("HiTune analysis done");
            }
        }
    }

    /**
     * @param args
     */
    public static void main(String[] args) {
        // TODO Auto-generated method stub
        new Analysis().run(args);
    }

}
