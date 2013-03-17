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

package ch.ethz.vizzly.datatype.readings;

import java.io.Serializable;

/**
 * This class implements a data type that represents a location.
 * @author Matthias Keller
 *
 */
public class Location implements Serializable {

    private static final long serialVersionUID = 1L;

    public double latitude;
    public double longitude;
    
    public Location(double latitude, double longitude) {
        this.latitude = latitude;
        this.longitude = longitude;
    }
    
    public boolean equals(Object other) {
        if (this == other) return true;
        if (!(other instanceof Location)) return false;
        Location otherValue = (Location)other;
        
        return this.latitude == otherValue.latitude 
                && this.longitude == otherValue.longitude;
    }
    
    public int hashCode() {
        return (int)(latitude*100)+(int)(longitude*10000);
    }
    
}
