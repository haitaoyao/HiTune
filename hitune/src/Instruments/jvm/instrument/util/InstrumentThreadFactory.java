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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class InstrumentThreadFactory  {
    List <InstrumentThread> instrumentlist = new ArrayList<InstrumentThread>();


    public InstrumentThreadFactory(AgentConf conf){
        Init(conf);
    }

    /**
     * Add all instrument thread into threads factory according to the configuration
     */
    public boolean Init(AgentConf conf) {
        if( conf.getProperty("traceon").equals("true")) {
            instrumentlist.add(new TracingThread(conf));
        }
        return true;
    }

    /**
     * start all instrument threads
     */
    public void start() {
        for(Iterator<InstrumentThread> i = instrumentlist.iterator(); i.hasNext();)
            i.next().start();
    }

    /**
     * cleanup all instrument threads
     */
    public boolean Cleanup() {
        //System.out.println("cleanup in the hook");
        for(Iterator<InstrumentThread> i = instrumentlist.iterator(); i.hasNext();){
            InstrumentThread thread = i.next();
            if(!thread.isClosed()){
                try{
                    //System.out.println("interrupted here");
                    thread.interrupt();
                    //System.out.println("join here");
                    thread.join();
                }
                catch(Exception e) {
                    e.printStackTrace();
                    return false;
                }
                return thread.Cleanup();
            }
            else {
                return true;
            }

        }

        return true;
    }
}
