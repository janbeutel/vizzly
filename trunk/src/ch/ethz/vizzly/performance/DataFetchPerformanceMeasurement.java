/*
 * Copyright 2012 ETH Zurich, Computer Engineering and Networks Laboratory
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package ch.ethz.vizzly.performance;

/**
 * This class implements a data type that represents the measurement of the time
 * needed to fetch data from a data source.
 * @author Matthias Keller
 *
 */
public class DataFetchPerformanceMeasurement {

    public enum DataBackend {
        DIRECTACCESS, MEMCACHE, MYSQLDBCACHE, LIVEAGGREGATION, UNDEFINED
    }

    /* Time of measurement in millisec */
    public long timestamp;
    
    /* Identifier of requested signal */
    public String signalName;

    /* Last time when the signal was updated */
    public long lastUpdate;
    
    /* Used data backend */
    public DataBackend dataBackend;

    /* Window length used for aggregating data */
    public int windowLengthSec;

    /* Number of elements in result */
    public int resultSize;

    /* Time that was spent for data access in millisec */
    public long elapsedTime;

    public DataFetchPerformanceMeasurement(long timestamp, String signalName, long lastUpdate, DataBackend dataBackend, 
            int windowLengthSec, int resultSize, long elapsedTime) {
        this.timestamp = timestamp;
        this.signalName = signalName;
        this.lastUpdate = lastUpdate;
        this.dataBackend = dataBackend;
        this.windowLengthSec = windowLengthSec;
        this.resultSize = resultSize;
        this.elapsedTime = elapsedTime;
    }    

    public String getDataBackendString() {
        String ret = null;
        switch(dataBackend) {
        case DIRECTACCESS:
            ret = "directaccess";
            break;
        case MEMCACHE:
            ret = "memcache";
            break;
        case MYSQLDBCACHE:
            ret = "mysqldbcache";
            break;
        case LIVEAGGREGATION:
            ret = "liveaggregation";
            break;
        default:
            ret = "notset";
        }
        return ret; 
    }

}
