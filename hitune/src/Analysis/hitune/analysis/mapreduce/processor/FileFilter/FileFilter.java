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
package hitune.analysis.mapreduce.processor.FileFilter;


import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.log4j.Logger;

/**
 * To filter the files with certain/user-defined criteria
 *
 * input: target folder
 * output: file list(separated by comma) to process 
 */
public abstract class FileFilter {
    static Logger log = Logger.getLogger(FileFilter.class);
    public final static String SEPARATOR = ",";
    String pattern = null;
    Configuration conf = null;

    public class regpatternFilter implements PathFilter{

        @Override
        public boolean accept(Path path) {
            // TODO Auto-generated method stub
            //log.debug("path in filter: " + path.toString());
            String pathname =  path.toString();
            Pattern p = Pattern.compile(pattern);
            Matcher matcher = p.matcher(pathname);
            return matcher.find();
        }

    }

    public FileFilter(Configuration conf, String pattern){
        this.conf = conf;
        this.pattern = pattern;
    }



    /**
     * Simply scan all those files under the path recursively
     * @param path
     * @param files
     */
    public void scan(Path path , StringBuilder files){
        //log.debug("parentpath: " + path.toString());
        if(files == null){
            log.error("The files[StringBuilder] object isn't initialized");
            return;
        }
        try {
            //log.debug("pattern: " + pattern);
            FileSystem fs = path.getFileSystem(conf);
            FileStatus[] fstats = null;
            fstats = fs.globStatus(new Path(path.toString()+"/*"));

            for (FileStatus fstat: fstats){
                //log.debug("current file/folder: "+ fstat.getPath().toString());
                if(fstat.isDir()){
                    scan(fstat.getPath(),files); 
                }else {
                    FileStatus[] rst= null;
                    if(pattern==null || pattern.equals("") || pattern.length() ==0){
                        rst = fs.globStatus(fstat.getPath());
                    }else {
                        rst = fs.globStatus(fstat.getPath(),new regpatternFilter());
                    }
                    if(rst != null && rst.length != 0){                    
                        String filepath = rst[0].getPath().toString();

                        if(files.length()==0){
                            files.append(filepath);
                        }else {
                            files.append(SEPARATOR).append(filepath);
                        }
                    }
                }
            }
        } catch (IOException e) {
            // TODO Auto-generated catch block
            log.error("Cannot do the file system operation: " + path.toString());
            e.printStackTrace();
        }
    }

    abstract public String filter(Path toScan);

}
