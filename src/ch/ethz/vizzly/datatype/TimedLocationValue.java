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

package ch.ethz.vizzly.datatype;

/**
 * This class implements a data type that represents a measurement including
 * time and location information.
 * @author Matthias Keller
 *
 */
public class TimedLocationValue extends LocationValue {

    private static final long serialVersionUID = 1L;
    
    public long timestamp;
    
    public TimedLocationValue() {
        super();
        timestamp = 0L;
    }
    
    public TimedLocationValue(long timestamp, double value) {
        super(value);
        this.timestamp = timestamp;
    }
    
    public TimedLocationValue(long timestamp, double value, double locationLat, double locationLon) {
        super(value, locationLat, locationLon);
        this.timestamp = timestamp;
    }
    
    public TimedLocationValue(long timestamp, double value, Location location) {
        super(value, location);
        this.timestamp = timestamp;
    }
    
    public TimedLocationValue(long timestamp, LocationValue v) {
        super(v.value, v.location);
        this.timestamp = timestamp;
    }
    
       
}
