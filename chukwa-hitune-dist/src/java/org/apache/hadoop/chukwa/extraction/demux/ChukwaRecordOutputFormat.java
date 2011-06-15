/*
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

package org.apache.hadoop.chukwa.extraction.demux;


import org.apache.hadoop.chukwa.extraction.demux.processor.Util;
import org.apache.hadoop.chukwa.extraction.engine.ChukwaRecord;
import org.apache.hadoop.chukwa.extraction.engine.ChukwaRecordKey;
import org.apache.hadoop.chukwa.extraction.engine.RecordUtil;
import org.apache.hadoop.mapred.lib.MultipleSequenceFileOutputFormat;
import org.apache.log4j.Logger;
import org.apache.hadoop.chukwa.extraction.CHUKWA_CONSTANT;

//Allow the user define multiple level string separated by slash in the ReduceType
public class ChukwaRecordOutputFormat extends
  MultipleSequenceFileOutputFormat<ChukwaRecordKey, ChukwaRecord> implements CHUKWA_CONSTANT{
  static Logger log = Logger.getLogger(ChukwaRecordOutputFormat.class);

  @Override
  protected String generateFileNameForKeyValue(ChukwaRecordKey key,
      ChukwaRecord record, String name) {
    //To support Hierarchy datatype
    String output = RecordUtil.getClusterName(record) + "/"
        + key.getReduceType() + "/" + key.getReduceType().replace("/", HIERARCHY_CONNECTOR)
        + Util.generateTimeOutput(record.getTime());

    // {log.info("ChukwaOutputFormat.fileName: [" + output +"]");}

    return output;
  }
}
