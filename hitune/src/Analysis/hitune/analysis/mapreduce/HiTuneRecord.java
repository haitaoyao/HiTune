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


import java.lang.reflect.InvocationTargetException;
import java.util.Map;

import hitune.analysis.mapreduce.proxy.HiTuneRecordProxy;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.record.Record;
import org.apache.log4j.Logger;

public class HiTuneRecord<T extends  Record> extends HiTuneRecordProxy <T>{
    static Logger log = Logger.getLogger(HiTuneRecord.class);
    
    HiTuneRecordProxy proxy = null;
    String proxypackage = "hitune.analysis.mapreduce.proxy";
    
    public HiTuneRecord(T record) {
        super(record);
        String proxyclass = proxypackage + "."+ this.record.getClass().getSimpleName() ;
        //log.debug("proxyclass: " + proxyclass);
        //log.debug("record class: " + record.getClass());
        try {
            proxy = (HiTuneRecordProxy)Class.forName(proxyclass).getConstructor(new Class[] { record.getClass() }).newInstance(new Object[]{record});
        } catch (IllegalArgumentException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (SecurityException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (InstantiationException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (InvocationTargetException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (NoSuchMethodException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    
    @Override
    public void add(String field, String value){
        proxy.add(field, value);
    }

    @Override
    public T copyCommonFields(T record) {
        // TODO Auto-generated method stub
        return (T) proxy.copyCommonFields(record);
    }

   

    @Override
    public Long getTime() {
        // TODO Auto-generated method stub
        return proxy.getTime();
    }

    @Override
    public String getValue(String field) {
        // TODO Auto-generated method stub
        return proxy.getValue(field);
    }

    @Override
    public void setTime(long timestamp) {
        // TODO Auto-generated method stub
        proxy.setTime(timestamp);
    }

    @Override
    public Map<String, String> getCommonFields() {
        // TODO Auto-generated method stub
        return proxy.getCommonFields();
    }


    @Override
    public String[] getFields() {
        // TODO Auto-generated method stub
        return proxy.getFields();
    }
    
    public String toString(){
        return proxy.toString();
    }


    @Override
    public String getHost() {
        // TODO Auto-generated method stub
        return proxy.getHost();
    }


    @Override
    public void setHost(String hostname) {
        // TODO Auto-generated method stub
        proxy.setHost(hostname);
    }
    



    
}
