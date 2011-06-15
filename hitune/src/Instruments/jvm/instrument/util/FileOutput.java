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
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;

/**
 * A file output channel.
 * The output data will be buffered in the memory space at first.
 * If the buffer space is full, the data will be flushed into local file.
 * Or if there is no more output appended, the data will be flushed out as well
 *
 */
public class FileOutput extends AgentOutput {
    File file = null;
    FileOutputStream fos = null;

    public FileOutput(String filename) {
        if(filename != "" && filename != null) {
            try {
                file = new File(filename);
                try {
                    fos = new FileOutputStream(file, true);
                    dest = new BufferedOutputStream(fos, buffersize);
                } catch (IOException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
            }
            catch(NullPointerException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }
    }

    public void close(){
        closed = true;
        super.close();
        try {
            if(fos!=null)fos.close();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
            closed = false;
        }
        fos=null;
        file=null;
    }
   
}
