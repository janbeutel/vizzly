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

package ch.ethz.vizzly.cache.memory;

import java.util.Calendar;
import java.util.Date;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.log4j.Logger;

import ch.ethz.vizzly.cache.AbstractCache;
import ch.ethz.vizzly.datatype.CachedDataInfo;
import ch.ethz.vizzly.datatype.TimedLocationValue;
import ch.ethz.vizzly.datatype.VizzlySignal;
import ch.ethz.vizzly.performance.DataFetchPerformanceMeasurement.DataBackend;

/**
 * This class implement a cache that stores all data in memory.
 * @author Matthias Keller
 *
 */
public class MemCache extends AbstractCache {

    @SuppressWarnings("unused")
    private static Logger log = Logger.getLogger(MemCache.class);

    private ConcurrentHashMap<String, IndexedSignalData> cacheMap = null;

    private Vector<VizzlySignal> seenSignals = null;

    private final String description = "MemCache";
    
    /*
     * Init memory cache
     */
    public MemCache() {
        cacheMap = new ConcurrentHashMap<String, IndexedSignalData>();
        seenSignals = new Vector<VizzlySignal>();
        isInitialized = true;
        dataBackend = DataBackend.MEMCACHE;
    }

    public String getCacheDescription() {
        return description;
    }
    
    private IndexedSignalData getCacheEntry(VizzlySignal signal, int windowLengthSec, Boolean updateStats) {
        String identifier = signal.getUniqueIdentifier() + '_' + Integer.valueOf(windowLengthSec).toString();
        IndexedSignalData s = cacheMap.get(identifier);
        if(s == null) {
            if(updateStats) {
                cacheRequests++;
                cacheMisses++;
            }
            return null;
        }
        if(updateStats) {
            s.incrHits();
            cacheRequests++;
            cacheHits++;
        }
        return s;
    }
    
    public void updateCacheEntry(VizzlySignal signal, int windowLengthSec, Vector<TimedLocationValue> r) {
        String identifier = signal.getUniqueIdentifier() + '_' + Integer.valueOf(windowLengthSec).toString();
        IndexedSignalData d = cacheMap.get(identifier);
        if(d == null) {
            // Create cache entry
            if(!signal.hasLocation()) {
                d = new IndexedSignalData(signal, r.size(), r.firstElement().timestamp, windowLengthSec);
                addCacheEntry(signal, windowLengthSec, d);    
            } else {
                d = new IndexedSignalLocationData(signal, r.size(), r.firstElement().timestamp, windowLengthSec);
                addCacheEntry(signal, windowLengthSec, d);    
            }
        }
        if(r.size() > 0) {
            d.updateValues(r);
        }
    }

    private synchronized void addCacheEntry(VizzlySignal signal, int windowLengthSec, IndexedSignalData d) {
        String identifier = signal.getUniqueIdentifier() + '_' + Integer.valueOf(windowLengthSec).toString();
        IndexedSignalData c = cacheMap.get(identifier);
        if(c == null) {
            if(!cacheMap.containsKey(identifier)) {
                cacheMap.put(identifier, d);
            }
            addSignal(signal);
        }
    }
    
    public Boolean isInCache(VizzlySignal signal, int windowLengthSec) {
        IndexedSignalData d = getCacheEntry(signal, windowLengthSec, false);
        if(d != null) {
            return true;
        }
        return false;
    }
    
    public Long getStartTime(VizzlySignal signal, int windowLengthSec) {
        IndexedSignalData d = getCacheEntry(signal, windowLengthSec, false);
        if(d != null) {
            return d.getStartTime();
        }
        return null;
    }
    
    public Long getEndTime(VizzlySignal signal, int windowLengthSec) {
        IndexedSignalData d = getCacheEntry(signal, windowLengthSec, false);
        if(d != null) {
            return d.getEndTime();
        }
        return null;
    }
    
    public Long getFirstPacketTimestamp(VizzlySignal signal, int windowLengthSec) {
        IndexedSignalData d = getCacheEntry(signal, windowLengthSec, false);
        if(d != null) {
            return d.getFirstPacketTimestamp();
        }
        return null;   
    }
    
    public Long getLastPacketTimestamp(VizzlySignal signal, int windowLengthSec) {
        IndexedSignalData d = getCacheEntry(signal, windowLengthSec, false);
        if(d != null) {
            return d.getLastPacketTimestamp();
        }
        return null;
    }
    
    public Date getLastUpdate(VizzlySignal signal, int windowLengthSec) {
        IndexedSignalData d = getCacheEntry(signal, windowLengthSec, false);
        if(d != null) {
            return d.getLastUpdate();
        }
        return null;
    }
    
    public void addSignal(VizzlySignal signal) {
        synchronized(seenSignals) {
            if(!seenSignals.contains(signal)) {
                seenSignals.add(signal);
            }
        }
    }

    public Boolean removeSignal(VizzlySignal signal) {
        if(!isInitialized) {
            return false;
        }
        synchronized(seenSignals) {
            seenSignals.remove(signal);
        }
        for(String k : cacheMap.keySet()) {
            if(cacheMap.get(k).getSignal().equals(signal)) {
                cacheMap.remove(k);
            }
        }
        return true;
    }

    public Vector<TimedLocationValue> getSignalData(VizzlySignal signal, int windowLengthSec, Long timeFilterStart, Long timeFilterEnd, Boolean updateStats) {
        IndexedSignalData d = getCacheEntry(signal, windowLengthSec, updateStats);
        if(d != null) {
            if(d instanceof ch.ethz.vizzly.cache.memory.IndexedSignalLocationData) {
                return ((IndexedSignalLocationData)d).getData(timeFilterStart, timeFilterEnd);
            }   
            return d.getData(timeFilterStart, timeFilterEnd);
        }
        return null;
    }
    
    public Vector<CachedDataInfo> getCachedDataInfo() {
        Vector<CachedDataInfo> ret = new Vector<CachedDataInfo>();
        for(IndexedSignalData d : cacheMap.values()) {
            Boolean hasLocationData = false;
            if(d instanceof ch.ethz.vizzly.cache.memory.IndexedSignalLocationData) {
                hasLocationData = true;
            }
            Date lastPacketTimestamp = null;
            if(d.getLastPacketTimestamp() != null) {
                Calendar cal = Calendar.getInstance();
                cal.setTimeInMillis(d.getLastPacketTimestamp());
                lastPacketTimestamp = cal.getTime();    
            }
            CachedDataInfo i = new CachedDataInfo(d.getSignal(), d.getAvgInterval(), d.getNumElements(), 
                    hasLocationData, d.getLastUpdate(), lastPacketTimestamp, d.getHits());
            ret.add(i);
        }
        return ret;
    }

    @SuppressWarnings("unchecked")
    public Vector<VizzlySignal> getSignals() {
        return (Vector<VizzlySignal>)seenSignals.clone();
    }
    
    public long getCacheSize() {
        long total = 0;
        for(IndexedSignalData d : cacheMap.values()) {
            if(d instanceof ch.ethz.vizzly.cache.memory.IndexedSignalLocationData) {
                total += d.getNumElements() * 12;
            } else {
                total += d.getNumElements() * 4;
            }
        }
        return total;
    }

    public int getNumberOfSeenSignals() {
        return seenSignals.size();
    }

    public int getNumberOfCacheEntries() {
        return cacheMap.size();
    }

}