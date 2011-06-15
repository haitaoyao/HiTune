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

import hitune.analysis.mapreduce.proxy.HiTuneKeyProxy;
import hitune.analysis.mapreduce.proxy.HiTuneRecordProxy;

import org.apache.hadoop.record.Record;
import org.apache.log4j.Logger;

/**
 * 
 *
 */
public class HiTuneKey<T extends Record> extends HiTuneKeyProxy<T> {
    //T key = null;
    static Logger log = Logger.getLogger(HiTuneKey.class);
    HiTuneKeyProxy proxy = null;
    String proxypackage = "hitune.analysis.mapreduce.proxy";

    public HiTuneKey(T key){
        super(key);
        String proxyclass = proxypackage + "."+ this.key.getClass().getSimpleName() ;
        log.debug("proxyclass: " + proxyclass);
        log.debug("key class: " + key.getClass());
        try {
            proxy = (HiTuneKeyProxy)Class.forName(proxyclass).getConstructor(new Class[] { key.getClass() }).newInstance(new Object[]{key});
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
    public String getDataType() {
        // TODO Auto-generated method stub
        return proxy.getDataType();
    }

    @Override
    public String getKey() {
        // TODO Auto-generated method stub
        return proxy.getKey();
    }

    @Override
    public void setDataType(String datatype) {
        // TODO Auto-generated method stub
        proxy.setDataType(datatype);
    }

    @Override
    public void setKey(String key) {
        // TODO Auto-generated method stub
        proxy.setKey(key);
    }
    
    public String toString(){
        return proxy.toString();
    }
    
    

}
