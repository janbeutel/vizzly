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
 * This class implements a data type that represents a measurements with attached
 * location information.
 * @author Matthias Keller
 *
 */
public class LocationValue extends Value {

    static final long serialVersionUID = 1L;
    
    public Location location;
    
    /* Derived from com.google.visualization.datasource.datatable.value.NumberValue */
    protected static final double NULL_VALUE = -9999;
    
    public LocationValue() {
        super();
        location = null;
    }
    
    public LocationValue(double value) {
        super(value);
        this.location = null;
    }
    
    
    public LocationValue(double value, Location location) {
        super(value);
        this.location = location;
    }
    
    public LocationValue(double value, double locationLat, double locationLon) {
        super(value);
        this.location = new Location(locationLat, locationLon);
    }
    
}
