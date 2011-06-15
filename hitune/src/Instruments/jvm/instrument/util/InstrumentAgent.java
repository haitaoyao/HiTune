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

import java.lang.instrument.Instrumentation;


/**
 * InstrumentAgent is a java agent, which will be invoked before the main() function.
 * In this agent, it will instrument the current java process and get some status periodically.
 * 
 * 
 *
 */
public class InstrumentAgent {
    static InstrumentThreadFactory instrumentthreads = null;  
    static AgentConf conf = null;						

    /**
     * premain function will be called before the main function.
     * @param agentArguments
     * @param instrumentation
     */
    public static void premain(String agentArguments, Instrumentation instrumentation) {

        conf = new AgentConf(agentArguments);

        //new all instrument threads
        instrumentthreads = new InstrumentThreadFactory(conf);
        //start all instrument threads
        instrumentthreads.start();

        //A shutdown-hook to clean up all instrument threads
        Runtime.getRuntime().addShutdownHook(new Thread(){
            public void run(){
                //long start = System.currentTimeMillis();
                instrumentthreads.Cleanup();
                //long end = System.currentTimeMillis();
                //System.out.println("ClosedTime: " + (end - start));
            }
        });



    }
    
    public static void main(String[] args) {
    }

}
