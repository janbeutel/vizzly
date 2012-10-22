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

package ch.ethz.vizzly.cache;

import java.util.Calendar;
import java.util.Date;
import java.util.Vector;

import ch.ethz.vizzly.datatype.CachedDataInfo;
import ch.ethz.vizzly.datatype.TimedLocationValue;
import ch.ethz.vizzly.datatype.VizzlySignal;
import ch.ethz.vizzly.performance.DataFetchPerformanceMeasurement.DataBackend;

/**
 * This class defines the interfaces that any cache implementation must follow.
 * @author Matthias Keller
 *
 */
public abstract class AbstractCache {

    private Date bootTime = null;
    
    protected Boolean isInitialized = false;
    
    protected long cacheHits = 0L;

    protected long cacheMisses = 0L;

    protected long cacheRequests = 0L;
    
    protected DataBackend dataBackend = DataBackend.UNDEFINED;
    
    public AbstractCache() {
        bootTime = Calendar.getInstance().getTime();
    }
    
    public abstract void updateCacheEntry(VizzlySignal signal, int windowLengthSec, Vector<TimedLocationValue> r);

    public abstract Vector<CachedDataInfo> getCachedDataInfo();
    
    public abstract Vector<VizzlySignal> getSignals();
    
    public abstract Vector<TimedLocationValue> getSignalData(VizzlySignal signal, int windowLengthSec, Long timeFilterStart, Long timeFilterEnd, Boolean updateStats);

    public abstract Boolean isInCache(VizzlySignal signal, int windowLengthSec);
    
    public abstract Long getStartTime(VizzlySignal signal, int windowLengthSec);
    
    public abstract Long getEndTime(VizzlySignal signal, int windowLengthSec);
    
    public abstract Long getFirstPacketTimestamp(VizzlySignal signal, int windowLengthSec);
    
    public abstract Long getLastPacketTimestamp(VizzlySignal signal, int windowLengthSec);
    
    public abstract Date getLastUpdate(VizzlySignal signal, int windowLengthSec);
    
    public abstract void addSignal(VizzlySignal signal);
    
    public abstract Boolean removeSignal(VizzlySignal signal);
    
    public abstract String getCacheDescription();
    
    public long getUptime() {
        return Calendar.getInstance().getTime().getTime()-bootTime.getTime();
    }

    public abstract long getCacheSize();

    public abstract int getNumberOfSeenSignals();

    public abstract int getNumberOfCacheEntries();
    
    public Boolean isInitialized() {
        return isInitialized;
    }
    
    public long getNumberOfCacheRequests() {
        return cacheRequests;
    }

    public long getNumberOfCacheHits() {
        return cacheHits;
    }

    public long getNumberOfCacheMisses() {
        return cacheMisses;
    }
    
    public DataBackend getDataBackend() {
        return dataBackend;
    }
    
}
