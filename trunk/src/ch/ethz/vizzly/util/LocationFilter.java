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

import java.util.Vector;

import ch.ethz.vizzly.datatype.TimedLocationValue;
import ch.ethz.vizzly.datatype.ValueAggregate;

/**
 * This class provides two functions for filtering and aggregating time series by
 * its included location information.
 * @author Matthias Keller
 *
 */
public class LocationFilter {

    public static Vector<TimedLocationValue> filterByLocation(Vector<TimedLocationValue> input, Double latSW, 
            Double lngSW, Double latNE, Double lngNE) {
        // Do more filtering and aggregation by location
        Vector<TimedLocationValue> n = new Vector<TimedLocationValue>();
        TimedLocationValue v = null;
        for(int i = 1; i < input.size(); i++) {
            v = input.get(i);
            if(v.location.latitude >= latNE || v.location.latitude < latSW) {
                continue;
            }
            if(v.location.longitude >= lngNE || v.location.longitude < lngSW) {
                continue;
            }
            n.add(v);
        }
        return n;
    }

    public static Vector<TimedLocationValue> filterAndAggregateByLocation(Vector<TimedLocationValue> input, Double latSW, 
            Double lngSW, Double latNE, Double lngNE) {
        // Do more filtering and aggregation by location
        Vector<TimedLocationValue> n = new Vector<TimedLocationValue>();
        ValueAggregate vAgg = new ValueAggregate();
        vAgg.addValue(input.get(0).value);
        long curTime = input.get(0).timestamp;
        TimedLocationValue v = null;
        for(int i = 1; i < input.size(); i++) {
            v = input.get(i);
            if(v.location.latitude >= latNE || v.location.latitude < latSW) {
                continue;
            }
            if(v.location.longitude >= lngNE || v.location.longitude < lngSW) {
                continue;
            }
            if(v.timestamp != curTime) {
                n.add(new TimedLocationValue(v.timestamp, vAgg.getAggregatedValue(), v.location));
                curTime = v.timestamp;
                vAgg.reset();
            }
            vAgg.addValue(v.value);
        }
        if(vAgg.getNumSamples() > 0) {
            n.add(new TimedLocationValue(v.timestamp, vAgg.getAggregatedValue(), v.location));
            vAgg.reset();
        }
        return n;
    }


}
