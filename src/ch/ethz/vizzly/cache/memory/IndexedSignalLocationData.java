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
public class IndexedSignalLocationData extends IndexedSignalData {

    private int maxTimeTransIdxUsed = 0;
    private int nextDataArrayIdx = 0;
    private int[][] timeTranslation = null;
    
    private double[] cachedDataVal = null;
    private double[] cachedDataLat = null;
    private double[] cachedDataLng = null;
    
    private static Logger log = Logger.getLogger(IndexedSignalLocationData.class);

    public IndexedSignalLocationData(VizzlySignal signal, long firstPacketTimestamp, int windowLengthSec) {
        initMetadata(signal, firstPacketTimestamp, windowLengthSec);
        maxTimeTransIdxUsed = -1;
        nextDataArrayIdx = 0;
    }

    private int getTransTimeIdx(long timeMilli, Boolean readOnly) {
        if(readOnly && timeTranslation == null) {
            return -1;
        }
        int idx = (int)((timeMilli-indexStartTimeMilli)/windowLengthMilli);
        if(idx < 0) {
            return -1;
        }
        if(timeTranslation == null || idx > (timeTranslation.length-1)) {
            if(!readOnly) {
                createOrResizeTimeTranslation(idx+1);
            } else {
                return -1;
            }
        }
        return idx;
    }

    public int getStartIndex(long timeMilli) {
        if(timeTranslation == null) {
            return -1;
        }
        int idx = getTransTimeIdx(timeMilli, true);
        if(idx == -1) {
            return -1;
        }
        return timeTranslation[idx][0];
    }

    public int getEndIndex(long timeMilli) {
        if(timeTranslation == null) {
            return -1;
        }
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

        // timeTranslation != null after getTransTimeIdx()
        int timeTransIdx = getTransTimeIdx(aggregatedData.get(0).timestamp, false);

        // Second step: Clean-up previously filled, overlapping data
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

        // Third step: Increase size of data structures, if needed
        if(cachedDataVal == null || (nextDataArrayIdx+aggregatedData.size()) > cachedDataVal.length-1) {
            createOrResizeDataArray(nextDataArrayIdx+aggregatedData.size()+1); 
        }

        // Fourth step: Add new data
        int curTimeIdx = -1;
        for(int i = 0; i < aggregatedData.size(); i++) {
            TimedLocationValue v = aggregatedData.get(i);
            timeTransIdx = getTransTimeIdx(v.timestamp, false);

            // First entry
            if(curTimeIdx == -1) {
                timeTranslation[timeTransIdx][0] = nextDataArrayIdx;
            }

            // Update when time changes
            if(curTimeIdx != -1 && timeTransIdx != curTimeIdx) {
                timeTranslation[curTimeIdx][1] = nextDataArrayIdx-1;
                timeTranslation[timeTransIdx][0] = nextDataArrayIdx;
            }

            cachedDataVal[nextDataArrayIdx] = v.value;
            cachedDataLat[nextDataArrayIdx] = v.location.latitude;
            cachedDataLng[nextDataArrayIdx] = v.location.longitude;
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
        if(lastPacketTimestamp == null) {
            return null;
        }
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
                data.add(new TimedLocationValue(curTime, new LocationValue(cachedDataVal[i], cachedDataLat[i], cachedDataLng[i])));
            }
            curTime += getAvgIntervalMilli();
        }
        return data;
    }

    private void createOrResizeTimeTranslation(int minSize) {
        // Add space for roughly one more month of data
        int incr = (int)Math.round(2678400/windowLengthSec);
        int newSize = minSize + ((incr < 10) ? 10 : incr); 
        //log.debug("Increasing time translation array size to " + newSize + ", minSize = " + minSize + ", incr = " + incr);

        int[][] newTimeTranslation = new int[newSize][2];

        if(timeTranslation != null) {
            // Copy data of existing array
            for(int i = 0; i < timeTranslation.length; i++) {
                newTimeTranslation[i][0] = timeTranslation[i][0];
                newTimeTranslation[i][1] = timeTranslation[i][1];
            }
            for(int i = timeTranslation.length; i < newTimeTranslation.length; i++) {
                newTimeTranslation[i][0] = -1;
                newTimeTranslation[i][1] = -1;
            }
        } else {
            // Initialize new array with -1
            for(int i = 0; i < newTimeTranslation.length; i++) {
                newTimeTranslation[i][0] = -1;
                newTimeTranslation[i][1] = -1;
            }
        }
        timeTranslation = newTimeTranslation;
    }

    private void createOrResizeDataArray(int minSize) {
        // Add space for roughly one more month of data
        int incr = (int)Math.round(2678400/windowLengthSec);
        int newSize = minSize + ((incr < 10) ? 10 : incr);
        //log.debug("Increasing data array size to " + newSize + ", minSize = " + minSize + ", incr = " + incr);
        double[] newDataVal = new double[newSize];
        double[] newDataLat = new double[newSize];
        double[] newDataLng = new double[newSize];

        if(cachedDataVal != null) {
            // Copy data of existing arrays
            for(int i = 0; i < cachedDataVal.length; i++) {
                newDataVal[i] = cachedDataVal[i];
                newDataLat[i] = cachedDataLat[i];
                newDataLng[i] = cachedDataLng[i];
            }
            for(int i = cachedDataVal.length; i < newDataVal.length; i++) {
                newDataVal[i] = NULL_VALUE;
            }
        } else {
            // Initialization of new data structures
            for(int i = 0; i < newDataVal.length; i++) {
                newDataVal[i] = NULL_VALUE;
            }  
        }
        cachedDataVal = newDataVal;
        cachedDataLat = newDataLat;
        cachedDataLng = newDataLng;
    }

    public long getEndTime() {
        return indexStartTimeMilli+(maxTimeTransIdxUsed*windowLengthMilli);
    }
    
    public int getNumElements() {
        if(cachedDataVal == null) {
            return -1;
        }
        return cachedDataVal.length;
    }

}
