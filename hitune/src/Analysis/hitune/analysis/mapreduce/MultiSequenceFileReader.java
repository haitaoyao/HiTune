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

import java.io.IOException;


import org.apache.hadoop.chukwa.extraction.engine.ChukwaRecord;
import org.apache.hadoop.chukwa.extraction.engine.ChukwaRecordKey;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MultiFileSplit;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.SequenceFileRecordReader;
import org.apache.log4j.Logger;

/**
 * The reader will read the records one by one from multiple files. 
 * <br>If the next record is in the same file, it just move on and gets the key and value promptly;
 * <br>If it comes to the end of current file, it will open a new file handler and read the first record.
 */
public class MultiSequenceFileReader implements RecordReader<ChukwaRecordKey, ChukwaRecord> {

    static Logger log = Logger.getLogger(MultiSequenceFileReader.class);
    MultiFileSplit split = null;
    long pos = 0;
    long totalLength = 0;
    FileSystem fs;
    int count = 0;
    Path [] paths = null;
    SequenceFileRecordReader<ChukwaRecordKey, ChukwaRecord> reader = null;
    private boolean more = true;

    Configuration conf = null;
    /**
     * 
     */
    public MultiSequenceFileReader(Configuration conf, MultiFileSplit split)throws IOException  {
        // TODO Auto-generated constructor stub
        this.split = split;
        this.conf = conf;
        paths = this.split.getPaths();
        fs = FileSystem.get(conf);
        totalLength = split.getLength();
        pos = 0;
        log.debug("total split number:" + split.getNumPaths());
        log.debug("open split:" + paths[0].toString());
        FileSplit filesplit = new FileSplit(paths[0],0,split.getLength(0),(JobConf) conf);
        reader = new SequenceFileRecordReader<ChukwaRecordKey, ChukwaRecord>(conf,filesplit);
        if(reader == null){
            log.warn("open split failed!");
        }
    }

    /* (non-Javadoc)
     * @see org.apache.hadoop.mapred.RecordReader#close()
     */
    @Override
    public void close() throws IOException {
        // TODO Auto-generated method stub

    }

    /* (non-Javadoc)
     * @see org.apache.hadoop.mapred.RecordReader#createKey()
     */
    @Override
    public ChukwaRecordKey createKey() {
        // TODO Auto-generated method stub
        return reader.createKey();
    }

    /* (non-Javadoc)
     * @see org.apache.hadoop.mapred.RecordReader#createValue()
     */
    @Override
    public ChukwaRecord createValue() {
        // TODO Auto-generated method stub
        return reader.createValue();
    }

    /* (non-Javadoc)
     * @see org.apache.hadoop.mapred.RecordReader#getPos()
     */
    @Override
    public long getPos() throws IOException {
        // TODO Auto-generated method stub
        if(pos==totalLength){
            return pos;
        }
        else{
            long currentpos = reader.getPos();
            return pos + currentpos;
        }

    }

    /* (non-Javadoc)
     * @see org.apache.hadoop.mapred.RecordReader#getProgress()
     */
    @Override
    public float getProgress() throws IOException {
        // TODO Auto-generated method stub	
        return ((float)getPos())/this.totalLength;
    }



    /**
     * @param args
     */
    public static void main(String[] args) {
        // TODO Auto-generated method stub

    }

    @Override
    public boolean next(ChukwaRecordKey key, ChukwaRecord value)
    throws IOException {
        // TODO Auto-generated method stub
        if(reader==null){
            throw new IOException("reader is empty");
        }
        else{
            more = reader.next(key, value);
        }
        while(!more){
            if(reader!=null){
                log.debug("close previous reader:" + count);
                reader.close();
                reader = null;
            }
            pos += split.getLength(count);

            if(pos < totalLength){
                count++;
                log.debug("current split number:" + count);
                log.debug("open slit: " + paths[count]);
                FileSplit filesplit = new FileSplit(paths[count],0,split.getLength(count),(JobConf) conf);
                reader = new SequenceFileRecordReader<ChukwaRecordKey, ChukwaRecord>(conf,filesplit);
                if(reader==null){
                    throw new IOException("reader is empty");
                }else{
                    more = reader.next(key, value);
                }
            }
            else{
                break;
            }

        }
        return more;
    }

}
