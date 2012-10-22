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

/**
 * This class provides functions for converting geo coordinates as specified in GSN
 * data streams to coordinates used by Google Maps.
 * @author Matthias Keller
 *
 */
public class GeoCoordConverter {

    static public double convertLongitude(double lon) {
        double absdlon = Math.abs(Math.round(Math.round(lon/100) * 1000000.));
        double absmlon = Math.abs(Math.round((lon-absdlon/10000) * 1000000.));
        return (absdlon + (absmlon/60.)) * 1/1000000;
    }
    
    static public double convertLatitude(double lat) {
        double absdlat = Math.abs(Math.round(Math.round(lat/100) * 1000000.));
        double absmlat = Math.abs(Math.round((lat-absdlat/10000) * 1000000.));
        return (absdlat + (absmlat/60.)) * 1/1000000;
    }
    
}
