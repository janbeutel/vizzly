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
 * This class implements a data type that represents an end-to-end measurement
 * of the overall time needed for serving a request.
 * @author Matthias Keller
 *
 */
public class UserRequestPerformanceMeasurement {
    
    private long lastMeasurement = 0L;
    
    private long dataFetchTime = 0L;
    
    private long requestPrepareTime = 0L;
    
    private long requestFinalizeTime = 0L;
    
    private long timestamp = 0L;
    
    private int numRequestedSignals = 0;
    
    private int numReturnedLines = 0;
    
    public UserRequestPerformanceMeasurement() {
        lastMeasurement = System.currentTimeMillis();
        timestamp = System.currentTimeMillis();
    }
    
    public void setDataFetchStart() {
        requestPrepareTime = System.currentTimeMillis()-lastMeasurement;
        lastMeasurement = System.currentTimeMillis();
    }
    
    public void setDataFetchEnd() {
        dataFetchTime = System.currentTimeMillis()-lastMeasurement;
        lastMeasurement = System.currentTimeMillis();
    }
    
    public void setNumRequestedSignals(int numRequestedSignals) {
        this.numRequestedSignals = numRequestedSignals;
    }
    
    public void setNumReturnedLines(int numReturnedLines) {
        this.numReturnedLines = numReturnedLines;
    }
    
    public void setEnd() {
        requestFinalizeTime = System.currentTimeMillis()-lastMeasurement;
    }
    
    public long getTimestamp() {
        return timestamp;
    }
    
    public long getDataFetchTime() {
        return dataFetchTime;
    }
    
    public long getRequestPrepareTime() {
        return requestPrepareTime;
    }
    
    public long getRequestFinalizeTime() {
        return requestFinalizeTime;
    }
    
    public int getNumReturnedLines() {
        return numReturnedLines;
    }
    
    public int getNumRequestedSignals() {
        return numRequestedSignals;
    }
    

}
