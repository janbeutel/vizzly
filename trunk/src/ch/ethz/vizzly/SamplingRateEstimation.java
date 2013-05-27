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

package ch.ethz.vizzly;

import java.io.Serializable;
import java.util.Calendar;
import java.util.Date;
import java.util.Vector;

import org.apache.log4j.Logger;

import ch.ethz.vizzly.datatype.VizzlyException;
import ch.ethz.vizzly.datatype.readings.TimedLocationValue;
import ch.ethz.vizzly.datatype.readings.ValueAggregate;

/**
 * This class implements a data structure that maintains monthly estimates of the
 * unknown (mean) sampling interval of a signal. We assume that data is addded sequentially
 * and without large gaps.
 * @author Matthias Keller
 *
 */
public class SamplingRateEstimation implements Serializable {
    
    private static final long serialVersionUID = 1L;

    private ValueAggregate[] monthlyEstimates = null;
    
    private Date firstTimestamp = null;
    
    private static Logger log = Logger.getLogger(SamplingRateEstimation.class);
    
    /**
     * This variable is used for determining if it is worthwhile to update
     * the corresponding database entry.
     */
    private long lastSignificantUpdateTimestamp = 0L;
    
    /**
     * Minimum number of samples required for including a monthly value in an estimation. 
     * Used to filter weird outlier values
     */
    private final int MIN_NUM_SAMPLES = 100;
    
    /**
     * Minimal relative change caused by adding a new sample so that
     * data is marked as dirty.
     */
    private final double MIN_REL_CHANGE_DIRTY = 0.05;
    
    public SamplingRateEstimation(long firstTimestamp) {
        monthlyEstimates = new ValueAggregate[12];
        for(int i=0; i<monthlyEstimates.length; i++) {
            monthlyEstimates[i] = null;
        }
        Calendar cal = Calendar.getInstance();
        cal.setTimeInMillis(firstTimestamp);
        cal.set(Calendar.DAY_OF_MONTH, 0);
        cal.set(Calendar.HOUR, 0);
        cal.set(Calendar.MINUTE, 0);
        cal.set(Calendar.SECOND, 0);
        cal.set(Calendar.MILLISECOND, 0);
        this.firstTimestamp = cal.getTime();
    }
    
    // Only used for debugging
    private void showEstimatorContents() {
        Calendar cal = Calendar.getInstance();
        String estimateStr = null;
        String numSamplesStr = null;
        for(int i=0; i<monthlyEstimates.length; i++) {
            cal.setTime(firstTimestamp);
            cal.add(Calendar.MONTH, i);
            if(monthlyEstimates[i] == null) {
                estimateStr = "null";
                numSamplesStr = "";
            } else if(monthlyEstimates[i].getNumSamples() == 0) {
                estimateStr = "no samples";
                numSamplesStr = "";
            } else {
                estimateStr = Double.valueOf(monthlyEstimates[i].getAggregatedValue()).toString();
                numSamplesStr = ", " + Integer.valueOf(monthlyEstimates[i].getNumSamples()).toString() + " samples";
            }
            log.debug((cal.get(Calendar.MONTH)+1)+"/"+cal.get(Calendar.YEAR)+numSamplesStr+": "+estimateStr);
        }
    }
    
    private int getMonthsDiff(long timestamp) throws VizzlyException {
        Calendar cal = Calendar.getInstance();
        cal.setTimeInMillis(timestamp);
        int thisMonth = cal.get(Calendar.MONTH);
        int thisYear = cal.get(Calendar.YEAR);
        cal.setTime(firstTimestamp);
        int startMonth = cal.get(Calendar.MONTH);
        int startYear = cal.get(Calendar.YEAR);
        int monthsDiff = 0;
        if(thisYear > startYear) {
            monthsDiff += (thisYear-startYear-1)*12;
            monthsDiff += thisMonth;
            monthsDiff += 12-startMonth;
        } else if(thisYear == startYear) {
            monthsDiff = thisMonth-startMonth; 
        } else {
            log.debug("Requested: " + startYear + " " + thisYear + " " + thisMonth);
            showEstimatorContents();
            throw new VizzlyException("startYear cannot be larger than thisYear");
        }
        
        return monthsDiff;
    }
    
    private void updateEstimate(int monthIdx, double timeDiff) throws VizzlyException {
        double oldValue = 0.0;
        if(monthIdx < 0) {
            throw new VizzlyException("monthIdx must be >= 0");
        }
        // Resize array, if needed
        if(monthIdx > monthlyEstimates.length-1) {
            ValueAggregate[] newArray = new ValueAggregate[monthIdx+12];
            for(int i=0; i<newArray.length; i++) {
                newArray[i] = null;
            }
            System.arraycopy(monthlyEstimates, 0, newArray, 0, monthlyEstimates.length);
            monthlyEstimates = newArray;
        }
        // Create aggregation object, if needed
        if(monthlyEstimates[monthIdx] == null) {
            monthlyEstimates[monthIdx] = new ValueAggregate();
            // Database entry should be updated after adding an entry
            lastSignificantUpdateTimestamp = System.currentTimeMillis();
        }
        
        if(monthlyEstimates[monthIdx].getNumSamples() > 0) {
            oldValue = monthlyEstimates[monthIdx].getAggregatedValue();
        }
        
        monthlyEstimates[monthIdx].addValue(timeDiff);
        
        // Check if the added value made the current estimation change significantly
        // If yes, set the data as dirty
        if(monthlyEstimates[monthIdx].getNumSamples() > 1 &&
                Math.abs((monthlyEstimates[monthIdx].getAggregatedValue()/oldValue)-1) >= MIN_REL_CHANGE_DIRTY) {
            lastSignificantUpdateTimestamp = System.currentTimeMillis();
        }
    }

    public void updateEstimation(Vector<TimedLocationValue> data) {
        try {
            if(data.size() < 2) {
                return;
            }
            for(int i = 2; i < data.size(); i++) {
                updateEstimate(getMonthsDiff(data.get(i).timestamp), data.get(i).timestamp-data.get(i-1).timestamp);
            }
        } catch(VizzlyException e) {
            log.error(e);
        }
    }

    public double getSamplingRate(Long timeStart, Long timeEnd) throws VizzlyException {
        double ret = 0.1; // very high value as fall-back if no estimation data is available
        if(timeStart == null) {
            throw new VizzlyException("timeStart cannot be null");
        }
        if(timeEnd == null) {
            throw new VizzlyException("timeEnd cannot be null");
        }
        double sum = 0.0;
        int numElements = 0;

        int monthStart = getMonthsDiff(timeStart);
        int monthEnd = getMonthsDiff(timeEnd);

        for(int i = monthStart; i <= monthEnd; i++) {
            if(i < monthlyEstimates.length && monthlyEstimates[i] != null && monthlyEstimates[i].getNumSamples() >= MIN_NUM_SAMPLES) {
                sum += monthlyEstimates[i].getAggregatedValue();
                numElements++;
            }
        }
        if(numElements > 0) {
            ret = 1.0/(sum/(double)numElements);
        }
        return ret;
    }

    public long getLastSignificantUpdateTimestamp() {
        return lastSignificantUpdateTimestamp;
    }
    
}
