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

import java.io.Serializable;
import java.util.Calendar;
import java.util.Vector;

import org.apache.log4j.Logger;

import ch.ethz.vizzly.datatype.VizzlySignal;
import ch.ethz.vizzly.datatype.readings.LocationValue;
import ch.ethz.vizzly.datatype.readings.TimedLocationValue;
import ch.ethz.vizzly.util.DataAggregationUtil;

/**
 * This class implements a data structure that holds measurements with time and
 * location information that have been aggregated to a certain level of detail.
 * @author Matthias Keller
 *
 */
public class IndexedSignalLocationData extends IndexedSignalData implements Serializable {

    private static final long serialVersionUID = 1L;

    private int maxTimeTransIdxUsed = 0;
    private int nextDataArrayIdx = 0;
    private int[][] timeTranslation = null;
    private LocationValue[] cachedData = null;
  
    private static Logger log = Logger.getLogger(IndexedSignalLocationData.class);

    public IndexedSignalLocationData(VizzlySignal signal, int size, long firstPacketTimestamp, int windowLengthSec) {
        super(signal, firstPacketTimestamp, windowLengthSec);
        cachedData = new LocationValue[size];
        maxTimeTransIdxUsed = -1;
        nextDataArrayIdx = 0;
        timeTranslation = new int[size][2];
        for(int i = 0; i < size; i++) {
            timeTranslation[i][0] = -1;
            timeTranslation[i][1] = -1;
        }
    }

    private int getTransTimeIdx(long timeMilli, Boolean readOnly) {
        int idx = (int)((timeMilli-indexStartTimeMilli)/windowLengthMilli);
        if(idx < 0) {
            return -1;
        }
        if(idx > (timeTranslation.length-1)) {
            if(!readOnly) {
                resizeTimeTranslation(idx);
            } else {
                return -1;
            }
        }
        return idx;
    }

    public int getStartIndex(long timeMilli) {
        int idx = getTransTimeIdx(timeMilli, true);
        if(idx == -1) {
            return -1;
        }
        return timeTranslation[idx][0];
    }

    public int getEndIndex(long timeMilli) {
        int idx = getTransTimeIdx(timeMilli, true);
        if(idx == -1) {
            return -1;
        }
        return timeTranslation[idx][1];
    }

    public void updateValues(Vector<TimedLocationValue> data) {
        if(data.size() == 0) {
            return;
        }
        // First step: Pre-aggregate new data
        Vector<TimedLocationValue> aggregatedData = DataAggregationUtil.aggregateDataWithLocation(data, windowLengthSec);
        if(aggregatedData.size() == 0) {
            log.warn("Empty aggregated data.");
            return;
        }

        // Second step: Clean-up previously filled, overlapping data
        if(aggregatedData.size() > 0) {
            int timeTransIdx = getTransTimeIdx(aggregatedData.get(0).timestamp, false);
            if(timeTransIdx >= 0 && timeTransIdx <= maxTimeTransIdxUsed) {
                int startIdx = timeTranslation[timeTransIdx][0];
                if(startIdx != -1) {
                    //log.debug("Decremented _nextDataArrayIdx from " + _nextDataArrayIdx + " to " + startIdx);
                    nextDataArrayIdx = startIdx;
                    for(int i = timeTransIdx; i <= maxTimeTransIdxUsed; i++) {
                        timeTranslation[i][0] = -1;
                        timeTranslation[i][1] = -1;
                    }
                }
            }
        }

        // Third step: Increase size of data structure, if needed
        if((nextDataArrayIdx+aggregatedData.size()) > cachedData.length) {
            resizeDataArray(nextDataArrayIdx+aggregatedData.size()); 
        }

        // Fourth step: Add new data
        int curTimeIdx = -1;
        for(int i = 0; i < aggregatedData.size(); i++) {
            TimedLocationValue v = aggregatedData.get(i);
            int timeTransIdx = getTransTimeIdx(v.timestamp, false);

            // First entry
            if(curTimeIdx == -1) {
                timeTranslation[timeTransIdx][0] = nextDataArrayIdx;
            }

            // Update when time changes
            if(curTimeIdx != -1 && timeTransIdx != curTimeIdx) {
                timeTranslation[curTimeIdx][1] = nextDataArrayIdx-1;
                timeTranslation[timeTransIdx][0] = nextDataArrayIdx;
            }

            cachedData[nextDataArrayIdx] = new LocationValue(v.value, v.location); 
            nextDataArrayIdx++;
            curTimeIdx = timeTransIdx;
        }
        timeTranslation[curTimeIdx][1] = nextDataArrayIdx-1;
        maxTimeTransIdxUsed = curTimeIdx;

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
            int startIdx = getStartIndex(curTime);
            int endIdx = getEndIndex(curTime);
            if(startIdx == -1 || endIdx == -1) {
                curTime += getAvgIntervalMilli();
                continue;
            }
            for(int i = startIdx; i <= endIdx; i++) {
                data.add(new TimedLocationValue(curTime, cachedData[i]));
            }
            curTime += getAvgIntervalMilli();
        }
        return data;
    }

    private void resizeTimeTranslation(int minSize) {
        // Add space for roughly one more month of data
        int incr = (int)Math.round(2678400/windowLengthSec);
        int newSize = minSize + ((incr < 10) ? 10 : incr); 
        //log.debug("Increasing time translation array size to " + newSize + ", minSize = " + minSize + ", incr = " + incr);

        int[][] newTimeTranslation = new int[newSize][2];

        for(int i = 0; i < timeTranslation.length; i++) {
            newTimeTranslation[i][0] = timeTranslation[i][0];
            newTimeTranslation[i][1] = timeTranslation[i][1];
        }

        if(newTimeTranslation.length > timeTranslation.length) {
            for(int i = timeTranslation.length; i < newTimeTranslation.length; i++) {
                newTimeTranslation[i][0] = -1;
                newTimeTranslation[i][1] = -1;
            }
        }

        timeTranslation = newTimeTranslation;
    }

    private void resizeDataArray(int minSize) {
        // Add space for roughly one more month of data
        int incr = (int)Math.round(2678400/windowLengthSec);
        int newSize = minSize + ((incr < 10) ? 10 : incr);
        //log.debug("Increasing data array size to " + newSize + ", minSize = " + minSize + ", incr = " + incr);
        LocationValue[] newData = new LocationValue[newSize];

        for(int i = 0; i < cachedData.length; i++) {
            newData[i] = cachedData[i];
        }
        cachedData = newData;
    }

    public long getEndTime() {
        return indexStartTimeMilli+(maxTimeTransIdxUsed*windowLengthMilli);
    }
    
    public int getNumElements() {
        if(cachedData == null) {
            return -1;
        }
        return cachedData.length;
    }

}
