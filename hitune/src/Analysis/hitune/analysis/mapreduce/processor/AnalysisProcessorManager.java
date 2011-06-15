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

import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;



/**
 * To load all MapReduce analyzers' configuration and initialize corresponding processor to do analysis.
 * 
 *
 */
public class AnalysisProcessorManager {

    static Logger log = Logger.getLogger(AnalysisProcessorManager.class);
    static final String HiTuneAnalysis = "hitune.analysis.mapreduce.processor";
    List<AnalysisProcessor> engineList = new ArrayList<AnalysisProcessor>();
    /**
     * 
     */
    public AnalysisProcessorManager(Map <String, List <Configuration>> conflist) {
        // TODO Auto-generated constructor stub
        for(String sourcedata : conflist.keySet()){
            log.info("Data source: " + sourcedata);
            List <Configuration> confs = conflist.get(sourcedata);
            for (Object item: confs.toArray()){
                Configuration conf = (Configuration)item;
                log.debug(conf.toString());
                String processor = HiTuneAnalysis + "." + conf.get(AnalysisProcessorConfiguration.reportengine);
                log.info("AnalysisProcessor" + processor);
                try {
                    AnalysisProcessor engine = (AnalysisProcessor)Class.forName(processor).getConstructor(new Class[] { Configuration.class }).newInstance(new Object[] {conf});
                    engineList.add(engine);
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
        }
    }

    public void start(){
        for (AnalysisProcessor engine : engineList){
            //log.info("AnalyzerProcessor: " + engine.getClass().getSimpleName() + "'s output: " + engine.getOutputFileName() + " START");
            engine.start();
        }
    }

    public void join(){
        for (AnalysisProcessor engine : engineList){
            try {
                engine.join();
            } catch (InterruptedException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }
    }



    /**
     * @param args
     */
    public static void main(String[] args) {
        // TODO Auto-generated method stub

    }

}
