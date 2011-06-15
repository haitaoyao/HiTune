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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.util.StringUtils;

/**
 * 
 *
 */
public abstract class ChukwaFileFilter extends FileFilter {

   
    
    
    protected boolean inputValidation(Configuration job,String dir, PathFilter filter) {
        boolean result = false;
        if(filter==null){
            filter = new PathFilter(){
                @Override
                public boolean accept(Path path) {
                    // TODO Auto-generated method stub
                    return true;
                }

            };
        }
        Path[] p = StringUtils.stringToPath(new String[]{dir});
        try {
            FileSystem fs = p[0].getFileSystem(job);
            FileStatus[] matches = fs.globStatus(p[0], filter);
            if(matches!=null && matches.length!=0){
                result = true;
            }
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        return result;
    }
    /**
     * @param conf
     */
    public ChukwaFileFilter(Configuration conf, String pattern) {
        super(conf, pattern);
        // TODO Auto-generated constructor stub
    }

   
    abstract public String filter(Path toScan) ;
    /**
     * @param args
     */
    public static void main(String[] args) {
        // TODO Auto-generated method stub

    }

}
