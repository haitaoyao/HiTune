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
package jvm.instrument.util;

import java.util.Properties;

/**
 * AgentConf is the configure class for all monitor threads
 * 
 *
 */
public class AgentConf {
    static Properties properties = new Properties();


    /**
     * Initialize all properties with default value
     */
    private void Init(){
        properties.setProperty("traceoutput", "");
        properties.setProperty("traceinterval", Integer.toString(20));
        properties.setProperty("tracedepth", Integer.toString(-1));
        properties.setProperty("traceon", "true");
        properties.setProperty("buffersize", "4096");
        properties.setProperty("style","line");
        properties.setProperty("duration", Integer.toString(-1));
        //TODO:
        properties.setProperty("taskid", "instrumentagent");
    }

    /**
     * Get each item of the properties based on input arguments
     * @param arguments A string with multiple properties separated by COMMA mark. Each property and its value is connected with EQUALMARK;  
     */
    private void parseOptions(String arguments){
        if (arguments != null) {
            String[] args = arguments.split(InstrumentThread.COMMA);
            for (int i = 0; i < args.length; i++){
                String[] subargs = args[i].split(InstrumentThread.EQUALMARK);
                if(subargs.length > 1) {
                    if(!this.setProperty(subargs[0], subargs[1])){
                        //TODO: Unkonw option
                    }
                }
            }
        }
    }

    /**
     * To initialize the property list and parse the arguments
     * @param args
     */
    public AgentConf(String args){
        Init();
        parseOptions(args);
    }

    /**
     * Get the corresponding value for certain field
     * @param field The property name
     * @return The corresponding value
     */
    public String getProperty(String field){
        if(properties.containsKey(field)){
            return (String)properties.get(field);
        }
        else {
            System.out.println("[ERR]Field:"+field + " doesn't exist.");
            return null;
        }
    }

    /**
     * Set the corresponding value for certain filed. 
     * If the property field doesn't exist, no action will be done. 
     * It return "false" to tell set action is failed.
     * @param field The property name
     * @param value The value of that configuration item
     * @return True or False to tell if the property and its value is set successfully
     */
    public boolean setProperty(String field, String value){
        if(properties.containsKey(field)){
            properties.setProperty(field, value);
            return true;
        }
        else {
            System.out.println("[ERR]Field:"+field + " doesn't exist.");
            return false;
        }
    }


}
