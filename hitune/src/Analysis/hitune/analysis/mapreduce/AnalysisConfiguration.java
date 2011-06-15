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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;





/**
 * To load the analysis configuration folder. The folder is built with a hierarchy structure.
 * <br>1. Under each of the stem folder, there exists 2 xml file, one is conf.xml, and the other is list.xml.
 * conf.xml is the configuration file of options for MapReduce analysis job; list.xml file is only the list of folders which should be processed by the analyzer.
 * <br>2. Under the leaf folder, there only contains 1 conf.xml file.
 * 
 *
 */
public class AnalysisConfiguration extends Configuration {

    static Logger log = Logger.getLogger(AnalysisConfiguration.class);
   
    /**
     * Map <sourcedata -> its corresponding configuration list>
     */
    private Map <String, List <Configuration>> conflist = new HashMap<String, List <Configuration>>();

    public Map<String,List <Configuration>> getConfList() {
        return this.conflist;
    }

    public AnalysisConfiguration(String folder, Configuration init){
        LoadConfiguration(folder, init);
    }


 

    /**
     * Load the current configuration folder recursively
     * 
     * if existing list.xml, go step forward
     * 
     * @param folder
     */
    public void LoadConfiguration(String folder, Configuration conf){
        log.debug("scan folder: " + folder);
        File listfile = new File(folder + "/list.xml");

        File conffile = new File(folder+ "/conf.xml");
        Configuration newconf = new Configuration(conf);
        newconf.addResource(new Path(conffile.getAbsolutePath()));
        
        try {
            
            if(listfile.exists()){
                Configuration tempconf = new Configuration(newconf);
                tempconf.addResource(new Path(listfile.getAbsolutePath()));

                Configuration _conf = new Configuration(false);
                _conf.addResource(new Path(listfile.getAbsolutePath()));
                Iterator<Map.Entry<String, String>>iter = _conf.iterator();
                while(iter.hasNext()){
                    Map.Entry<String, String> pairs = (Map.Entry<String, String>)iter.next();
                    String key = pairs.getKey();
                    LoadConfiguration(tempconf.get(key), newconf);
                }
            }
            else {
                String datasource = newconf.get(AnalysisProcessorConfiguration.datasource);
                log.debug("datasource: " + datasource);
                List <Configuration> cflist = null;
                if(conflist.containsKey(datasource)){
                    cflist = conflist.get(datasource);
                }
                else {
                    cflist = new ArrayList<Configuration> ();
                }
                cflist.add(newconf);
                log.debug("add conf: " + newconf);
                conflist.put(datasource, cflist);
            }
        } catch (Exception e) {
            // TODO Auto-generated catch block
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
