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

import org.apache.log4j.Logger;

import ch.ethz.vizzly.datatype.Location;
import ch.ethz.vizzly.datatype.LocationValue;
import ch.ethz.vizzly.datatype.LocationValueAggregate;
import ch.ethz.vizzly.datatype.ValueAggregate;

/**
 * This class implements a grid that is used for generating aggregated 2D maps.
 * @author Matthias Keller
 *
 */
public class LocationAggregationGrid {
    
    /* Grid dimensions */
    private int numRows = 0; // latitude
    private int numCols = 0; // longitude
    
    /* Geographical boundaries */
    private double latSW = 0.0;
    private double lngSW = 0.0;
    private double latNE = 0.0;
    private double lngNE = 0.0;
    
    /* Grid cell spacing */
    private double distLng = 0.0;
    private double distLat = 0.0;
    
    /* Reference point so that panning looks good */
    private double refLat = 0.0;
    private double refLng = 0.0;
    
    ValueAggregate[][] aggregatedValues = null;
    ValueAggregate[][] aggregatedLat = null;
    ValueAggregate[][] aggregatedLng = null;
    
    @SuppressWarnings("unused")
    private static Logger log = Logger.getLogger(LocationAggregationGrid.class);
    
    public LocationAggregationGrid(double latSW, double lngSW, double latNE, double lngNE, int numRows, int numCols) {
        this.latSW = latSW;
        this.lngSW = lngSW;
        this.latNE = latNE;
        this.lngNE = lngNE;
        this.numRows = numRows;
        this.numCols = numCols;
        aggregatedValues = new ValueAggregate[numRows][numCols];
        aggregatedLat = new ValueAggregate[numRows][numCols];
        aggregatedLng = new ValueAggregate[numRows][numCols];
        
        for(int i=0; i < numRows; i++) {
            for(int j=0; j < numCols; j++) {
                aggregatedValues[i][j] = new ValueAggregate();
                aggregatedLat[i][j] = new ValueAggregate();
                aggregatedLng[i][j] = new ValueAggregate();
            }
        }
        distLat = (latNE-latSW)/numRows;
        distLng = (lngNE-lngSW)/numCols;
        refLat = Math.floor(latSW/distLat)*distLat;
        refLng = Math.floor(lngSW/distLng)*distLng;
    }
    
    public void addValue(LocationValue v) {
        if(v.location.latitude >= latNE || v.location.latitude < latSW) {
            return;
        }
        if(v.location.longitude >= lngNE || v.location.longitude < lngSW) {
            return;
        }
        int row = (int)Math.floor((v.location.latitude-refLat)/distLat);
        int col = (int)Math.floor((v.location.longitude-refLng)/distLng);
        if(row < numRows && col < numCols) {
            aggregatedValues[row][col].addValue(v.value);
            aggregatedLat[row][col].addValue(v.location.latitude);
            aggregatedLng[row][col].addValue(v.location.longitude);
        }
    }
    
    public LocationValueAggregate[] getAggregatedData() {
        LocationValueAggregate[] ret = new LocationValueAggregate[numRows*numCols];
        for(int i=0; i < numRows; i++) {
            for(int j=0; j < numCols; j++) {
                int idx = (i*numCols)+j;
                if(aggregatedValues[i][j].getNumSamples() == 0) {
                    ret[idx] = null;
                    continue;
                }
                Location l = new Location(aggregatedLat[i][j].getAggregatedValue(), aggregatedLng[i][j].getAggregatedValue());
                ret[idx] = new LocationValueAggregate(aggregatedValues[i][j], l);
            }
        }
        return ret;
    }
    

}
