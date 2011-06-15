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

/**
 * InstrumentThread is an interface for all MonitorAgentThread.
 * 
 *
 */
public abstract class InstrumentThread extends Thread{

    static String SPACE = " ";
    static String LINEBREAKE = "\n";
    static String SEPERATE_MARK = "#";
    static String DOT = ".";
    static String COMMA = ",";
    static String COLON = ":";
    static String EQUALMARK = "=";
    static String TAB = "\t";
    /**
     * Initialize the InstrumentThread. 
     * <br><br>
     * In the Initialize function, it set the property of the thread, such as thread name, isDaemon, priority and etc.
     * 
     * @return to show if the initialization is successful.
     */
    abstract public boolean Init(AgentConf conf);

    /**
     * Cleanup function must be called before JVM death.
     * All the InstrumentThread must implement this function to Cleanup all its related environment.
     * <br><br>
     * This function will be called by the InstrumentThreadFactory.Cleanup. And the InstrumentThreadFactory.Cleanup function 
     * will be invoked during the agent's shutdown hook.
     * 
     * @return to show if the Cleanup is done
     */
    abstract public boolean Cleanup();

    abstract public boolean isClosed();
   
}
