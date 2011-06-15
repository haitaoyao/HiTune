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

import java.io.BufferedOutputStream;
import java.io.IOException;

/**
 * AgentOutput is the common output class for the Agent.
 * If there is not output file configured, the debug information will be output into STDOUT
 * 
 *
 */
public class AgentOutput {

    protected BufferedOutputStream dest = null;
    
    protected int buffersize = 1024 * 8;

    /**
     * If AgentOutput is closed, there won't be any output into file or stdout
     * 
     */
    protected boolean closed = false;			

    public void setBuffersize(int buffersize) {
        this.buffersize = buffersize;
    }

    public AgentOutput(){

    }


    public void write(String output) throws IOException {
        if(dest != null){
            dest.write(output.getBytes());
        }
        else if(!closed){
            System.out.print(output);
        }
    }


    public void flush() {
        if(dest != null){
            try {
                dest.flush();
            } catch (IOException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }

    }

    public void close() {
        if(dest != null) {
            try {
                dest.close();
                dest=null;
                closed = true;
            } catch (IOException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }
        closed = true;
    }
}
