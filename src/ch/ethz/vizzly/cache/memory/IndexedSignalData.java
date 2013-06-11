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

package ch.ethz.vizzly.cache.memory;

import java.util.Calendar;
import java.util.Date;
import java.util.Vector;

import org.apache.log4j.Logger;

import ch.ethz.vizzly.datatype.VizzlySignal;
import ch.ethz.vizzly.datatype.readings.TimedLocationValue;
import ch.ethz.vizzly.datatype.readings.TimedValue;
import ch.ethz.vizzly.util.DataAggregationUtil;
import ch.ethz.vizzly.util.TimestampTruncateUtil;

/**
 * This class implements a data structure that holds measurements with time information
 * that have been aggregated to a certain level of detail.
 * @author Matthias Keller
 *
 */
public class IndexedSignalData {
    
    protected static final double NULL_VALUE = -9999;

    protected long indexStartTimeMilli;
    protected int windowLengthSec;
    protected long windowLengthMilli;
    protected Calendar cal = null;
    protected Date lastUpdate = null;

    protected Long firstPacketTimestamp = null;
    protected Long lastPacketTimestamp = null;

    private double[] cachedData = null;
    private int maxDataIdxUsed;

    protected VizzlySignal signal = null;

    // Count how often the data was accessed
    protected int hits = 1;

    private static Logger log = Logger.getLogger(IndexedSignalData.class);

    protected IndexedSignalData(VizzlySignal signal, long firstPacketTimestamp, int windowLengthSec) {
        this.signal = signal;
        this.windowLengthSec = windowLengthSec;
        this.firstPacketTimestamp = firstPacketTimestamp;
        windowLengthMilli = windowLengthSec*1000;
        indexStartTimeMilli = truncateTimestamp(firstPacketTimestamp);
        cal = Calendar.getInstance();
        lastUpdate = cal.getTime();
    }

    public IndexedSignalData(VizzlySignal signal, int size, long firstPacketTimestamp, int windowLengthSec) {
        this(signal, firstPacketTimestamp, windowLengthSec);
        cachedData = new double[size];
        for(int i = 0; i < size; i++) {
            cachedData[i] = NULL_VALUE;
        }
        maxDataIdxUsed = -1;
    }

    private int getIdx(long timeMilli, Boolean noLimit) {
        int idx = (int)((timeMilli-indexStartTimeMilli)/windowLengthMilli);
        if(idx < 0) {
            return -1;
        }
        if(idx > (cachedData.length-1) && !noLimit) {
            return -1;
        }
        return idx;
    }

    protected long truncateTimestamp(long ts) {
        return TimestampTruncateUtil.truncate(ts, windowLengthMilli);
    }

    public void updateValues(Vector<TimedLocationValue> data) {
        if(data.size() == 0) {
            return;
        }
        // First step: Pre-aggregate new data
        Vector<TimedValue> aggregatedData = DataAggregationUtil.aggregateData(data, windowLengthSec);    
        if(aggregatedData.size() == 0) {
            log.warn("Empty aggregated data.");
            return;
        }
        
        // Second step: Increase size of data structure, if needed
        int maxIdx = getIdx(aggregatedData.lastElement().timestamp, true);
        if(maxIdx > cachedData.length) {
            resizeDataArray(maxIdx); 
        }

        // Third step: Add new data
        for(int i = 0; i < aggregatedData.size(); i++) {
            TimedValue v = aggregatedData.get(i);
            int idx = getIdx(v.timestamp, false);
            if(idx != -1) {
                cachedData[idx] = v.value;
            } else {
                log.error("Invalid index " + idx);
            }
            maxDataIdxUsed = idx;
        }

        lastPacketTimestamp = data.get(data.size()-1).timestamp;
        cal = Calendar.getInstance();
        lastUpdate = cal.getTime();
    }

    public Vector<TimedLocationValue> getData(Long timeFilterStart, Long timeFilterEnd) {
        Vector<TimedLocationValue> data = new Vector<TimedLocationValue>();
        long curTime = (timeFilterStart != null) ? truncateTimestamp(timeFilterStart) : getStartTime();
        long endTime = (timeFilterEnd != null) ? truncateTimestamp(timeFilterEnd) : getEndTime();
        if(curTime < getStartTime()) {
            curTime = getStartTime();
        }
        if(endTime > getEndTime()) {
            endTime = getEndTime();
        }
        while(curTime <= endTime) {
            int idx = getIdx(curTime, false);
            if(idx != -1) {
                if(cachedData[idx] != NULL_VALUE) {
                    data.add(new TimedLocationValue(curTime, cachedData[idx]));
                }

            }
            curTime += getAvgIntervalMilli();
        }
        return data;
    }

    private void resizeDataArray(int minSize) {
        // Add space for roughly one more month of data
        int incr = (int)Math.round(2678400/windowLengthSec);
        int newSize = minSize + ((incr < 10) ? 10 : incr);
        //log.debug("Increasing data array size to " + newSize + ", minSize = " + minSize + ", incr = " + incr);
        double[] newData = new double[newSize];
        for(int i = 0; i < cachedData.length; i++) {
            newData[i] = cachedData[i];
        }
        for(int i = cachedData.length; i < newData.length; i++) {
            newData[i] = NULL_VALUE;
        }
        cachedData = newData;
    }

    public long getStartTime() {
        return indexStartTimeMilli;
    }

    public long getEndTime() {
        return indexStartTimeMilli+(maxDataIdxUsed*windowLengthMilli);
    }

    public int getAvgInterval() {
        return windowLengthSec;
    }

    public long getAvgIntervalMilli() {
        return windowLengthMilli;
    }

    public VizzlySignal getSignal() {
        return signal;
    }

    // Not so clean, fix at some point
    public String getUniqueIdentifier() {
        return signal.getUniqueIdentifier() + "_" + Integer.toString(windowLengthSec);
    }

    public String getFilename() {
        return this.getUniqueIdentifier().replace(":", "_").replace(".", "_");
    }

    public int getNumElements() {
        if(cachedData == null) {
            return -1;
        }
        // Change for seeing how many elements there really are
        return maxDataIdxUsed; //_data.length;
    }

    public Date getLastUpdate() {
        return lastUpdate;
    }

    public Long getFirstPacketTimestamp() {
        return firstPacketTimestamp;
    }

    public Long getLastPacketTimestamp() {
        return lastPacketTimestamp;
    }

    public synchronized void incrHits() {
        hits++;
    }

    public int getHits() {
        return hits;
    }

}
