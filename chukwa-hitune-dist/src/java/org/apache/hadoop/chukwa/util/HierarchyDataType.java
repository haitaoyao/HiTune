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
package org.apache.hadoop.chukwa.util;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.log4j.Logger;


public class HierarchyDataType {
  static Logger log = Logger.getLogger(HierarchyDataType.class);

  /**
   * 
   * @param path
   * @param filter
   * @return
   */
  public static List<FileStatus> globStatus(FileSystem fs, Path path,PathFilter filter, boolean recursive){
    List<FileStatus> results = new ArrayList<FileStatus>();
    try {
      FileStatus[] candidates = fs.globStatus(path);
      for(FileStatus candidate : candidates){
        log.debug("candidate is:"+candidate);
        Path p = candidate.getPath();
        if(candidate.isDir() && recursive){
          StringBuilder subpath = new StringBuilder(p.toString());
          subpath.append("/*");
          log.debug("subfolder is:"+p);
          results.addAll(globStatus(fs, new Path(subpath.toString()), filter, recursive)); 					
        }
        else{
          log.debug("Eventfile is:"+p);
          FileStatus[] qualifiedfiles = fs.globStatus(p,filter);
          if (qualifiedfiles != null && qualifiedfiles.length > 0){
            log.debug("qualified Eventfile is:"+p);
            Collections.addAll(results, qualifiedfiles);
          }
        }
      }
    } catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    log.debug("results.length: " + results.size());
    return results;
  }

  public static List<FileStatus> globStatus(FileSystem fs, Path path, boolean recursive){
    List<FileStatus> results = new ArrayList<FileStatus>();
    try {
      FileStatus[] candidates = fs.listStatus(path);
      if( candidates.length > 0 ) {
        for(FileStatus candidate : candidates){
          log.debug("candidate is:"+candidate);
          Path p = candidate.getPath();
          if(candidate.isDir() && recursive){
            results.addAll(globStatus(fs, p, recursive)); 					
          }
        }
      }
      else {
        log.debug("path is:"+path);
        results.add(fs.globStatus(path)[0]);
      }
    } catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }

    return results;
  }


  public static String getDataType(Path path, Path cluster){
    log.debug("datasource path: " + path + " cluster path: " + cluster);
    String Cluster = cluster.toString();
    if (!Cluster.endsWith("/")){
      Cluster = Cluster + "/";
    }
    String dataType = path.toString().replaceFirst(Cluster, "");
    log.debug("The datatype is: " + dataType);
    return dataType;
  }

  public static String trimSlash(String datasource){
    String results = datasource;
    if(datasource.startsWith("/")){
      results = datasource.replaceFirst("/", "");
    }
    if(results.endsWith("/")){
      results = results.substring(0, -1);
    }
    return results;
  }



  /**
   * @param args
   */
  public static void main(String[] args) {
    // TODO Auto-generated method stub

  }

}
