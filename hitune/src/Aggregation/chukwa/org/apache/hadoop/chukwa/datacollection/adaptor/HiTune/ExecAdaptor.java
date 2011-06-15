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

import org.apache.hadoop.chukwa.datacollection.adaptor.AbstractAdaptor;
import org.apache.hadoop.chukwa.datacollection.adaptor.AdaptorException;
import org.apache.hadoop.chukwa.datacollection.adaptor.AdaptorShutdownPolicy;


import org.apache.log4j.Logger;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

/**

 * This ExecAdaptor aims to start a thread which invokes a executable processes. 
 * The executable command line can be shell command with "pipe" as well.
 * And meanwhile, it will create one stdOut ExecOutputSender to emit the ChunkImpl based on line entries
 * <BR>
 * 1) If the adaptor is stopped, all these threads must be stopped<BR>
 * 2) If the process is quit, all these threads must be stopped<BR>
 * 
 */
public class ExecAdaptor extends AbstractAdaptor implements Runnable{
    /**
     * The user's command line 
     */
    String cmd = "";
    /**
     * The external process to run user's command line
     */
    Process process = null;
    /**
     * The sender thread to listen all STD output and send them out to the collector
     */
    ExecOutputSender sender = null;

    long sendOffset = 0;
    /**
     * The monitoring thread to see if the process is running
     */
    Thread ExecThread = null; 

    /**
     * Mark if the process is terminated by shutdown() function 
     */
    volatile boolean running = false;

    static Logger log = Logger.getLogger(ExecAdaptor.class);

    public ExecAdaptor() {
        // TODO Auto-generated constructor stub
    }

    /* (non-Javadoc)
     * @see org.apache.hadoop.chukwa.datacollection.adaptor.AbstractAdaptor#parseArgs(java.lang.String)
     */
    @Override
    public String parseArgs(String s) {
        // TODO Auto-generated method stub
        log.debug("Adaptor's parameters: " + s);
        cmd=s.trim();
        log.debug("RUN command: " + cmd);
        return cmd;
    }

    /* (non-Javadoc)
     * @see org.apache.hadoop.chukwa.datacollection.adaptor.AbstractAdaptor#start(long)
     */
    @Override
    public void start(long offset) throws AdaptorException {
        // TODO Auto-generated method stub
        sendOffset = offset;

        //Runtime runtime = Runtime.getRuntime();
        ProcessBuilder pb = new ProcessBuilder("sh", "-c", cmd);
        try {
            process = pb.start();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
            deregisterAndStop();
            throw new AdaptorException("Cannot execute the command line!");
        }

        if (process != null){
            log.debug("start sender");
            running = true;
            sender = new ExecOutputSender(process);
            sender.start();
        }
        this.ExecThread = new Thread(this);
        this.ExecThread.start();
    }

    /* (non-Javadoc)
     * @see org.apache.hadoop.chukwa.datacollection.adaptor.Adaptor#getCurrentStatus()
     */
    @Override
    public String getCurrentStatus() {
        // TODO Auto-generated method stub
        if(sender!=null)
            sendOffset = sender.getSendOffset();
        return ExecAdaptor.this.type + " " + cmd;
    }

    /* (non-Javadoc)
     * @see org.apache.hadoop.chukwa.datacollection.adaptor.Adaptor#shutdown(org.apache.hadoop.chukwa.datacollection.adaptor.AdaptorShutdownPolicy)
     */
    @Override
    public long shutdown(AdaptorShutdownPolicy shutdownPolicy)
    throws AdaptorException {
        // TODO Auto-generated method stub
        if(sender!=null) 
            sendOffset = sender.getSendOffset();
        close();
        log.info("Destroy the execution process in any shutdown Policy");
        return sendOffset;
    }




    /**
     * The monitoring thread to get all std output of the running process(i.e., user's command line) and send them to the collectors
     * <br> If "running" mark is false, which means the process is terminated, this thread should exit ASAP.
     * <br> before the current thread quit, it will flush all buffered content.
     *
     */
    class ExecOutputSender extends EmitLineThread {
        private Process process = null;
        public ExecOutputSender(Process process){
            if (process == null){
                log.error("process in NULL");
            }
            else {
                this.process=process;
                setAdaptor(ExecAdaptor.this);
                setReceiver(ExecAdaptor.this.dest);
                setSendOffset(ExecAdaptor.this.sendOffset);
                setAdaptorParams(ExecAdaptor.this.cmd);
            }
        }

        public void run(){
            log.debug("Thread is running...");
            String line = null;
            //Get process's stdout
            if (process == null){
                log.error("process in NULL");
                return;
            }

            InputStream input = process.getInputStream();
            InputStreamReader reader = new InputStreamReader(input);
            BufferedReader bufferreader = new BufferedReader(reader);
            last_flush = System.currentTimeMillis();
            log.debug("Get STDOUT here");
            try {
                while ( running ){
                    //log.debug("stdout_line: " + line);
                    if((line = bufferreader.readLine()) != null)send(line);
                }
            } catch (IOException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }

            finally{
                flush();
                try {
                    bufferreader.close();
                    reader.close();
                    input.close();
                } catch (IOException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
            }
        }


    }

    /**
     * Close all corresponding process or thread
     * <br>1. Mark "running" as false, tell the ExecThread not to do deregisterAndStop();
     * <br>2. destroy the external process
     * <br>3. wait for sender thread to exit
     */
    public void close(){
        running = false;
        if (process != null){
            process.destroy();
        }
        if(sender!=null){
            try {
                sender.join();
            } catch (InterruptedException except) {
                // TODO Auto-generated catch block
                except.printStackTrace();
            }
            sender = null;
        }
    }



    /**
     * The listening thread to monitor the process is running or not
     * <br>1. if this thread is interrupted, it will log the stack and deregisterAndStop() the related Adaptor
     * <br>2. if the process is terminated, it will deregisterAndStop() the related Adaptor
     * <br>3. if Chukwa agent shutdown current Adaptor, it won't do deregisterAndStop()
     */
    @Override
    public void run() {
        // TODO Auto-generated method stub
        try {
            // wait for the process to stop
            if(process != null){
                int exitValue = process.waitFor();
                if (exitValue != 0){
                    log.error(cmd + "'s return value: " + exitValue);
                }
            }
        } catch (InterruptedException e) { 
            log.error(e.getMessage());
            e.printStackTrace();
        }
        finally{
            //if someone shutdowns the adaptor after checking "running" status(which is true at that time)
            //deregisterAndStop will warn that there is no adaptor to stop
            if(running)deregisterAndStop();
        }

    }
    /**
     * @param args
     */
    public static void main(String[] args) {
        // TODO Auto-generated method stub


    }



}
