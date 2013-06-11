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

package ch.ethz.vizzly.util;

import java.util.HashMap;
import java.util.Vector;

import ch.ethz.vizzly.datatype.readings.Location;
import ch.ethz.vizzly.datatype.readings.TimedLocationValue;
import ch.ethz.vizzly.datatype.readings.TimedValue;
import ch.ethz.vizzly.datatype.readings.ValueAggregate;

/**
 * This class provides various functions for aggregating time series data. Functions differ
 * on how location information should be handled (is not provided, is used, is to be ignored).
 * @author Matthias Keller
 *
 */
public class DataAggregationUtil {

    public static Vector<TimedValue> aggregateData(Vector<TimedLocationValue> unaggregatedData, int windowLengthSec) {
        Vector<TimedValue> aggregatedData = new Vector<TimedValue>();
        ValueAggregate vAgg = new ValueAggregate();
        long curTime = -1;
        for(int i = 0; i < unaggregatedData.size(); i++) {
            TimedLocationValue v = unaggregatedData.get(i);
            long truncTime = TimestampTruncateUtil.truncate(v.timestamp, windowLengthSec*1000);
            if(curTime != -1 && truncTime != curTime) {
                if(vAgg.getNumSamples() > 0) {
                    aggregatedData.add(new TimedValue(curTime, vAgg.getAggregatedValue()));
                    vAgg.reset();
                }
            }
            vAgg.addValue(v.value);
            curTime = truncTime;
        }
        if(vAgg.getNumSamples() > 0) {
            aggregatedData.add(new TimedValue(curTime, vAgg.getAggregatedValue()));
            vAgg.reset();
        }
        return aggregatedData;
    }
    
    public static Vector<TimedLocationValue> aggregateDataNullLocation(Vector<TimedLocationValue> unaggregatedData, int windowLengthSec) {
        Vector<TimedLocationValue> aggregatedData = new Vector<TimedLocationValue>();
        ValueAggregate vAgg = new ValueAggregate();
        long curTime = -1;
        for(int i = 0; i < unaggregatedData.size(); i++) {
            TimedLocationValue v = unaggregatedData.get(i);
            long truncTime = TimestampTruncateUtil.truncate(v.timestamp, windowLengthSec*1000);
            if(curTime != -1 && truncTime != curTime) {
                if(vAgg.getNumSamples() > 0) {
                    aggregatedData.add(new TimedLocationValue(curTime, vAgg.getAggregatedValue(), null));
                    vAgg.reset();
                }
            }
            vAgg.addValue(v.value);
            curTime = truncTime;
        }
        if(vAgg.getNumSamples() > 0) {
            aggregatedData.add(new TimedLocationValue(curTime, vAgg.getAggregatedValue(), null));
            vAgg.reset();
        }
        return aggregatedData;
    }
    
    public static Vector<TimedLocationValue> aggregateDataWithLocation(Vector<TimedLocationValue> unaggregatedData, int windowLengthSec) {
        HashMap<Location,ValueAggregate> locationAgg = new HashMap<Location,ValueAggregate>();
        Vector<TimedLocationValue> aggregatedData = new Vector<TimedLocationValue>();
        long curTime = -1;
        for(int i = 0; i < unaggregatedData.size(); i++) {
            TimedLocationValue v = unaggregatedData.get(i);
            long truncTime = TimestampTruncateUtil.truncate(v.timestamp, windowLengthSec*1000);
            if(curTime != -1 && truncTime != curTime) {
                if(locationAgg.size() > 0) {
                    for(Location l : locationAgg.keySet()) {
                        aggregatedData.add(new TimedLocationValue(curTime, locationAgg.get(l).getAggregatedValue(), l));
                    }
                    locationAgg.clear();
                }
            }

            // Aggregate values that fall into the same time slot, location-aware
            ValueAggregate vAgg = locationAgg.get(v.location);
            if(vAgg == null) {
                vAgg = new ValueAggregate();
                locationAgg.put(DataAggregationUtil.decreaseLocationAccuracy(v.location, 4), vAgg);
            }
            vAgg.addValue(v.value);
            curTime = truncTime;
        }
        if(locationAgg.size() > 0) {
            for(Location l : locationAgg.keySet()) {
                aggregatedData.add(new TimedLocationValue(curTime, locationAgg.get(l).getAggregatedValue(), l));
            }
            locationAgg.clear();
        }
        return aggregatedData;
    }

    public static Location decreaseLocationAccuracy(Location l, int numDecimals) {
        double div = Math.pow(10, numDecimals);
        return new Location(
                (double)Math.round(l.latitude*div)/div,
                (double)Math.round(l.longitude*div)/div);
    }
    
}
