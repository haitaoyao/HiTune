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
package org.apache.hadoop.chukwa.datacollection.adaptor.HiTune;

import org.apache.hadoop.chukwa.ChunkImpl;
import org.apache.hadoop.chukwa.datacollection.ChunkReceiver;
import org.apache.hadoop.chukwa.datacollection.adaptor.Adaptor;
import org.apache.log4j.Logger;

/**
 * Send the output data to collectors.
 * 
 *
 */
public abstract class EmitLineThread implements Runnable{
    static Logger log = Logger.getLogger(EmitLineThread.class);

    static final int LINE_BUF_LENGTH = 50;
    static final int FORCE_FLUSH_DELAY = 30000; // in million-second

    protected long last_flush = 0;
    protected StringBuilder output = null;
    protected int line_count = 0;
    protected long sendOffset = 0;

    protected Adaptor source = null;
    protected String adaptorParams = null;
    protected ChunkReceiver dest = null;

    Thread thread = new Thread((Runnable) this);

    public void join() throws InterruptedException{
        thread.join();
    }


    public long getSendOffset(){
        return this.sendOffset;
    }

    public void setAdaptor(Adaptor source) {
        // TODO Auto-generated method stub
        this.source = source;
    }


    public void setReceiver(ChunkReceiver dest) {
        // TODO Auto-generated method stub
        this.dest = dest;
    }


    public void setSendOffset(long offset) {
        // TODO Auto-generated method stub
        this.sendOffset = offset;
    }

    public void setAdaptorParams(String params){
        this.adaptorParams = params;
    }

    public boolean start(){
        log.debug("start thread");
        if (source == null || adaptorParams == null || dest == null ){
            log.error("NULL pointer for member variables");
            return false;
        }
        thread.setDaemon(true);
        thread.setName(this.getClass().getSimpleName());
        log.debug("quit start");
        thread.start();
        return true;
    }

    /**
     * This function buffered the STDOUT line content, and flush it in time
     */
    protected void send(String line){
        if (!line.equals("")){
            if(output == null){
                output = new StringBuilder();
            }
            log.debug("STDOUT: " + line);
            //output.append(System.currentTimeMillis()).append(" ");
            output.append(line).append("\n");
            line_count++;
            log.debug("line buffer count: " +line_count );
            if (line_count == LINE_BUF_LENGTH){
                log.debug("flush because of ups to line buffer length");
                flush();
            }else if((System.currentTimeMillis()- last_flush) > FORCE_FLUSH_DELAY ){
                log.debug("flush because of longer than the flush delay");
                flush();
            }
        }

    }

    protected long flush(){
        if (output == null){
            return sendOffset;
        }
        log.debug("flush out: " + output);
        byte[] data;
        data = output.toString().getBytes();
        sendOffset += data.length;
        ChunkImpl c = new ChunkImpl(source.getType(),"results from "
                + adaptorParams,sendOffset, data, source);
        try {
            log.debug("add one chunk");
            //control.reportCommit(ExecAdaptor.this, sendOffset);
            dest.add(c);
        } catch (InterruptedException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        output = null;
        line_count = 0;

        last_flush = System.currentTimeMillis();
        log.debug("last flush at: " + last_flush);
        return sendOffset;
    }


    /**
     * @param args
     */
    public static void main(String[] args) {
        // TODO Auto-generated method stub

    }

    @Override
    abstract public void run();

}
