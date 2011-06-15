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

import org.apache.hadoop.fs.FileSystem;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.util.Progressable;

/**
 * Define CSF file output format. Each line of the file represents one record, and field contents are connected by COMMA mark.
 *
 */
public class CSVFileOutputFormat<K, V> extends TextOutputFormat<K, V>{

    static final String SEPEARATOR_COMMA =",";
    static final String TEXT_SEPEARATOR_COMMA=";";
    static class CSVFileWriter<K, V> implements RecordWriter<K, V>{
        RecordWriter newWriter = null;

        public CSVFileWriter(RecordWriter writer){
            newWriter = writer;
        }

        private Text array2Text(ArrayWritable array){
            String [] contents = array.toStrings();
            StringBuilder sb = new StringBuilder();
            for (String content : contents){
                sb.append(content.replaceAll(SEPEARATOR_COMMA, TEXT_SEPEARATOR_COMMA)).append(SEPEARATOR_COMMA);
            }
            return new Text(sb.toString().substring(0,sb.length()-1));
        }

        @Override
        public void close(Reporter reporter) throws IOException {
            // TODO Auto-generated method stub
            newWriter.close(reporter);
        }

        @Override
        public void write(Object key, Object value) throws IOException {
            // TODO Auto-generated method stub
            if(value instanceof ArrayWritable){
                newWriter.write(key,array2Text((ArrayWritable) value));
            }
            else {
                newWriter.write(key,value);
            }

        }

    }
    /**
     * 
     */
    public CSVFileOutputFormat() {
        // TODO Auto-generated constructor stub

    }

    public RecordWriter<K, V> getRecordWriter(FileSystem fs,
            JobConf job,
            String name,
            Progressable progress)
            throws IOException {

        job.set("mapred.textoutputformat.separator",SEPEARATOR_COMMA);
        String filename = job.get(AnalysisProcessorConfiguration.outputfilename);
        if(filename ==null || filename.equals("")){
            filename = name;
        }else{
            filename += "-" + name;
            //FileOutputFormat.setWorkOutputPath(job,new Path(job.get("mapred.output.dir")+"/" + filename));
        }

        return new CSVFileWriter(super.getRecordWriter(fs, job, filename, progress));
    }
    /**
     * @param args
     */
    public static void main(String[] args) {
        // TODO Auto-generated method stub

    }

}
