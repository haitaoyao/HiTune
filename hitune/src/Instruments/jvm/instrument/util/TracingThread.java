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

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;



/**
 * Tracing thread is one of the instrument thread. It aims to get all java threads' status periodically, and print them out.
 * Current collected status includes: thread name, id, status, callstacks, isDaemon and etc.
 * 
 *
 */
public class TracingThread extends InstrumentThread {
    //private String traceoutput = null; //output folder for @taskid@.log
    private String traceoutputfile = null;		//traceoutputfile
    private int traceinterval = 20;				//traceinterval
    private int tracedepth = -1;					//tracedepth in millisecond
    private AgentOutput output = null;
    private boolean closed = false;
    private String style = "line";
    private String taskid = "";
   


    //Not enabled yet
    private int duration = -1; 					//This duration is to control the running time of thie java agent thread

    

    public void setTaskID(String id){
        this.taskid = id;
    }

    public void setDuration(int duration){
        this.duration = duration;
    }

    public void setTraceOutputFile(String traceoutputfile) {
        this.traceoutputfile = traceoutputfile;
    }

    public void setTraceInterval(int traceinterval) {
        this.traceinterval = traceinterval;
    }

    public void setTraceDepth (int tracedepth) {
        this.tracedepth = tracedepth;
    }

    public void setStyle (String style){
        this.style = style;
    }


    public TracingThread(AgentConf conf) {
        if (this.Init(conf)) {
            output = new FileOutput(this.traceoutputfile);
            
            //output = new AgentOutput(this.traceoutputfile);
            output.setBuffersize(Integer.parseInt(conf.getProperty("buffersize")));
            if(this.style.equals("line")){
                try {
                    output.write("TimeStamp"+InstrumentThread.COMMA);
                    output.write("ThreadID" + InstrumentThread.COMMA);
                    output.write("ThreadName" + InstrumentThread.COMMA );
                    output.write("isDaemon" + InstrumentThread.COMMA);			
                    output.write("ThreadPriority" + InstrumentThread.COMMA);
                    output.write("ThreadState" + InstrumentThread.COMMA);
                    if(!taskid.equals(""))
                        output.write("TaskID" + InstrumentThread.COMMA);
                    output.write("CallStack"+InstrumentThread.LINEBREAKE);
                } catch (IOException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                    this.Cleanup();
                }

            }
        }

    }


    public boolean Init(AgentConf conf){
        boolean done = true;
        this.setDaemon(true);
        this.setName("MonitorThreadTrace");
        if(conf.getProperty("traceoutput")!=null && !conf.getProperty("traceoutput").equals(""))
            this.setTraceOutputFile(conf.getProperty("traceoutput")+"/"+conf.getProperty("taskid")+".log");
        this.setTraceInterval(Integer.parseInt(conf.getProperty("traceinterval")));
        this.setTraceDepth(Integer.parseInt(conf.getProperty("tracedepth")));
        this.setStyle(conf.getProperty("style"));
        this.setTaskID(conf.getProperty("taskid"));
        
        return done;
    }



    /**
     * Get the stack trace of all threads except itself
     * @param thread
     * @param stackInfo
     */
    private void PrintStackTrace(Thread thread, StackTraceElement[] stackInfo)throws IOException  {
        String name = thread.getName(); 		//Get the thread name
        long id = thread.getId(); 		//Get the thread id
        boolean isDaemon = thread.isDaemon(); 		//Get the thread is daemon or not
        int priority = thread.getPriority(); 	//Get the thread priority
        State state = thread.getState(); 		//Get the thread state

        if(this.style.equals("blk")){
            output.write("\"" + name + "\"" + InstrumentThread.SPACE );
            if(!taskid.equals(""))
                output.write("taskid=" + taskid + InstrumentThread.SPACE);
            output.write("tid=" + id + InstrumentThread.SPACE);
            output.write("daemon=" + isDaemon + InstrumentThread.SPACE);
            output.write("priority=" + priority + InstrumentThread.SPACE);
            output.write("state=" + state.toString() + InstrumentThread.LINEBREAKE);

        }

        else if (this.style.equals("line")){
            output.write(id + InstrumentThread.COMMA);
            output.write("\"" + name + "\"" + InstrumentThread.COMMA );
            output.write(isDaemon + InstrumentThread.COMMA);			
            output.write(priority + InstrumentThread.COMMA);
            output.write(state.toString() + InstrumentThread.COMMA);
            if(!taskid.equals(""))
                output.write(taskid + InstrumentThread.COMMA);

        }

        if ( stackInfo.length > 0 ) {
            //trace for each thread
            int depth = stackInfo.length;
            if(this.tracedepth > 0){
                depth = stackInfo.length < this.tracedepth ? stackInfo.length: this.tracedepth;
            }
            //int depth = stackInfo.length < this.tracedepth ? stackInfo.length: this.tracedepth;
            for ( int i = 0; i < depth; i++) {
                String classname = stackInfo[i].getClassName();
                String methodname = stackInfo[i].getMethodName();
                if(this.style.equals("blk")){
                    output.write(InstrumentThread.TAB +"at" + InstrumentThread.SPACE );
                    output.write(classname + InstrumentThread.DOT + methodname + InstrumentThread.SPACE );
                }
                else if (this.style.equals("line")){	
                    output.write(classname + InstrumentThread.DOT + methodname + InstrumentThread.SPACE );
                    if (i < (depth -1)){
                        output.write(InstrumentThread.SEPERATE_MARK);
                    }
                }
                if(this.style.equals("blk")){
                    output.write(InstrumentThread.LINEBREAKE);
                }
            }
        }
        output.write(InstrumentThread.LINEBREAKE);
    }


    /**
     * Dump the thread tracing information periodically
     */
    public void run() {
        try{

            while(output!=null && !Thread.interrupted()) {
                long timestamp = System.currentTimeMillis();
                if(this.style.equals("blk")){
                    output.write("Timestamp:" + InstrumentThread.SPACE + timestamp + InstrumentThread.LINEBREAKE);
                    output.write("=== Get All Thread Stack Traces ===" + InstrumentThread.LINEBREAKE);
                }
                //getAllStackTraces

                Map<Thread,StackTraceElement[]> traceinfo = Thread.getAllStackTraces();
                if(traceinfo.size() > 0){
                    //If the map is not empty
                    Iterator<Entry<Thread, StackTraceElement[]>>  iter = traceinfo.entrySet().iterator();  
                    while (iter.hasNext()) {  
                        Map.Entry<Thread, StackTraceElement[]>  entry   =   (Map.Entry<Thread, StackTraceElement[]>) iter.next() ;  
                        Thread   key   =  (Thread) entry.getKey()   ;
                        if(key!=this){
                            StackTraceElement[]   value   =   (StackTraceElement[]) entry.getValue();
                            if (this.style.equals("line")){
                                output.write(timestamp+InstrumentThread.COMMA);
                            }
                            PrintStackTrace(key, value);
                        }
                    }
                }
                if(this.style.equals("blk")){
                    output.write("=== End of All Thread Stack Traces ===" + InstrumentThread.LINEBREAKE);
                    output.write(InstrumentThread.LINEBREAKE);
                }
                try {
                    if(this.traceinterval > 0 && !Thread.interrupted()){
                        Thread.sleep(this.traceinterval);
                    }
                    else {
                        break;
                    }
                } catch (InterruptedException e) {
                    // TODO Auto-generated catch block
                    //e.printStackTrace();
                    break;
                }
            }

        }
        catch (Exception e) {
            // TODO Auto-generated catch block
            this.Cleanup();
            //e.printStackTrace();
        }

    }

    public boolean isClosed(){
        return this.closed;
    }



    public boolean Cleanup(){
        //close Agentoutput
        if(closed)return closed;
        try{
            if(output!=null)output.close();
        }
        catch(Exception e){
            e.printStackTrace();
            return closed;
        }
        output = null;
        closed = true;
        return closed;
    }
}
