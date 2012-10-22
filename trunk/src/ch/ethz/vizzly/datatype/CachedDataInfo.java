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

import java.util.Comparator;
import java.util.Date;

/**
 * This class implements a data type that describes the properties and contents of cached data.
 * @author Matthias Keller
 *
 */
public class CachedDataInfo {

    public VizzlySignal signal;
    public int windowLength;
    public int numElements;
    public Boolean hasLocationData;
    public Date lastUpdate;
    public Date lastPacketTimestamp;
    public int hits;
    
    public CachedDataInfo(VizzlySignal signal, int windowLength, int numElements, Boolean hasLocationData, Date lastUpdate, Date lastPacketTimestamp, int hits) {
        this.signal = signal;
        this.windowLength = windowLength;
        this.numElements = numElements;
        this.hasLocationData = hasLocationData;
        this.lastUpdate = lastUpdate;
        this.lastPacketTimestamp = lastPacketTimestamp;
        this.hits = hits;
    }
    
    public static Comparator<CachedDataInfo> getComparator(SortParameter... sortParameters) {
        return new CachedDataInfoComparator(sortParameters);
    }

    public enum SortParameter {
        NAME_ASCENDING, NAME_DESCENDING, WINDOW_LENGTH_ASCENDING, WINDOW_LENGTH_DESCENDING,
        NUM_ELEMENTS_ASCENDING, NUM_ELEMENTS_DESCENDING, HAS_LOCATION_DATA_ASCENDING, 
        HAS_LOCATION_DATA_DESCENDING, LAST_UPDATE_ASCENDING, LAST_UPDATE_DESCENDING, 
        LAST_PACKET_TIMESTAMP_ASCENDING, LAST_PACKET_TIMESTAMP_DESCENDING, HITS_ASCENDING, HITS_DESCENDING
    }
   
    private static class CachedDataInfoComparator implements Comparator<CachedDataInfo> {
        private SortParameter[] parameters;

        private CachedDataInfoComparator(SortParameter[] parameters) {
            this.parameters = parameters;
        }

        public int compare(CachedDataInfo o1, CachedDataInfo o2) {
            int comparison;
            for (SortParameter parameter : parameters) {
                switch (parameter) {
                case NAME_ASCENDING:
                    comparison = o1.signal.getUniqueIdentifier().compareTo(o2.signal.getUniqueIdentifier());
                    if (comparison != 0) return comparison;
                    break;
                case NAME_DESCENDING:
                    comparison = o2.signal.getUniqueIdentifier().compareTo(o1.signal.getUniqueIdentifier());
                    if (comparison != 0) return comparison;
                    break;
                case WINDOW_LENGTH_ASCENDING:
                    comparison = o1.windowLength - o2.windowLength;
                    if (comparison != 0) return comparison;
                    break;
                case WINDOW_LENGTH_DESCENDING:
                    comparison = o2.windowLength - o1.windowLength;
                    if (comparison != 0) return comparison;
                    break;
                case NUM_ELEMENTS_ASCENDING:
                    comparison = o1.numElements - o2.numElements;
                    if (comparison != 0) return comparison;
                    break;
                case NUM_ELEMENTS_DESCENDING:
                    comparison = o2.numElements - o1.numElements;
                    if (comparison != 0) return comparison;
                    break;
                case HAS_LOCATION_DATA_ASCENDING:
                    comparison = o1.hasLocationData.compareTo(o2.hasLocationData);
                    if (comparison != 0) return comparison;
                    break;
                case HAS_LOCATION_DATA_DESCENDING:
                    comparison = o2.hasLocationData.compareTo(o1.hasLocationData);
                    if (comparison != 0) return comparison;
                    break;
                case LAST_UPDATE_ASCENDING:
                    // Can be null when the cache entry is initialized/filled for the first time
                    if(o1 == null || o1.lastUpdate == null) {
                        return 1;
                    }
                    if(o2 == null || o2.lastUpdate == null) {
                        return -1;
                    }
                    comparison = o1.lastUpdate.compareTo(o2.lastUpdate);
                    if (comparison != 0) return comparison;
                    break;
                case LAST_UPDATE_DESCENDING:
                    // Can be null when the cache entry is initialized/filled for the first time
                    if(o1 == null || o1.lastUpdate == null) {
                        return -1;
                    }
                    if(o2 == null || o2.lastUpdate == null) {
                        return 1;
                    }
                    comparison = o2.lastUpdate.compareTo(o1.lastUpdate);
                    if (comparison != 0) return comparison;
                    break;
                case LAST_PACKET_TIMESTAMP_ASCENDING:
                    // Can be null when the cache entry is initialized/filled for the first time
                    if(o1 == null || o1.lastPacketTimestamp == null) {
                        return 1;
                    }
                    if(o2 == null || o2.lastPacketTimestamp == null) {
                        return -1;
                    }
                    comparison = o1.lastPacketTimestamp.compareTo(o2.lastPacketTimestamp);
                    if (comparison != 0) return comparison;
                    break;
                case LAST_PACKET_TIMESTAMP_DESCENDING:
                    // Can be null when the cache entry is initialized/filled for the first time
                    if(o1 == null || o1.lastPacketTimestamp == null) {
                        return -1;
                    }
                    if(o2 == null || o2.lastPacketTimestamp == null) {
                        return 1;
                    }
                    comparison = o2.lastPacketTimestamp.compareTo(o1.lastPacketTimestamp);
                    if (comparison != 0) return comparison;
                    break;
                case HITS_ASCENDING:
                    comparison = o1.hits - o2.hits;
                    if (comparison != 0) return comparison;
                    break;
                case HITS_DESCENDING:
                    comparison = o2.hits - o1.hits;
                    if (comparison != 0) return comparison;
                    break;
                }
            }
            return 0;
        }
    }
    
}
