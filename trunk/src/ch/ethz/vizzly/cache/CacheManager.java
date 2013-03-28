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
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.log4j.Logger;

import ch.ethz.vizzly.AggregationLevelLookup;
import ch.ethz.vizzly.datareader.AbstractDataReader;
import ch.ethz.vizzly.datareader.DataReaderRegistry;
import ch.ethz.vizzly.datatype.CacheConfiguration;
import ch.ethz.vizzly.datatype.CachedDataInfo;
import ch.ethz.vizzly.datatype.VizzlyException;
import ch.ethz.vizzly.datatype.VizzlySignal;
import ch.ethz.vizzly.datatype.VizzlySignalCurrentness;
import ch.ethz.vizzly.datatype.readings.TimedLocationValue;
import ch.ethz.vizzly.performance.AbstractPerformanceTracker;
import ch.ethz.vizzly.performance.DataFetchPerformanceMeasurement;
import ch.ethz.vizzly.performance.DataFetchPerformanceMeasurement.DataBackend;
import ch.ethz.vizzly.util.DataAggregationUtil;

/**
 * This class implements the actual caching strategy. Currently each
 * configured cache contains data of a certain temporal resolution. 
 * @author Matthias Keller
 *
 */
public class CacheManager {

    /**
     * Log.
     */
    private static Logger log = Logger.getLogger(CacheManager.class);
    
    private Vector<CacheConfiguration> caches = null;
    
    private Calendar cal = null;
    
    private AbstractPerformanceTracker perfTracker = null;
    
    /* List that is temporarily used to mark which signals should be removed in the next update rounds */
    private Vector<VizzlySignal> signalsToRemove = null;

    /* Do not update again if there has just been an update */
    private final long UPDATE_DISTANCE_MSEC = 120000L;
    
    private DataReaderRegistry dataReaderRegistry = null;
    
    /**
     * Keep track when we tried to update a signal most recently
     */
    private ConcurrentHashMap<VizzlySignal,Long> signalLastUpdateAttempt = null;
    
    public CacheManager(Vector<CacheConfiguration> caches, DataReaderRegistry dataReaderRegistry, AbstractPerformanceTracker perfTracker) {
        this.caches = caches;
        this.dataReaderRegistry = dataReaderRegistry;
        this.perfTracker = perfTracker;
        signalsToRemove = new Vector<VizzlySignal>();
        signalLastUpdateAttempt = new ConcurrentHashMap<VizzlySignal,Long>();
        // Try to populate last update information from cached information
        // First seen signals, if data is also cached the currentness is refined in the second loop
        for(VizzlySignal s : caches.lastElement().cache.getSignals()) {
            signalLastUpdateAttempt.put(s, 0L);
        }
        for(CachedDataInfo d : caches.lastElement().cache.getCachedDataInfo()) {
            signalLastUpdateAttempt.put(d.signal, d.lastUpdate.getTime());
        }
    }
  
    public Vector<TimedLocationValue> getSignalData(VizzlySignal signal, int windowLengthSec, 
            Long timeFilterStart, Long timeFilterEnd, Boolean ignoreLocation) throws VizzlyException {
        Vector<TimedLocationValue> r = null;
        
        // Iterate through list of available caches. Assumes that faster/smaller caches are 
        // checked before slower/larger caches are polled.
        CacheConfiguration cacheConfigUsed = null;
        for(CacheConfiguration cc : caches) {
            if(windowLengthSec < cc.windowLength) {
                // Desired granularity is not found in this cache
                continue;
            }
            long dataFetchStart = System.currentTimeMillis();
            r = cc.cache.getSignalData(signal, cc.windowLength, 
                    timeFilterStart, timeFilterEnd, true);
            long dataFetchEnd = System.currentTimeMillis();
            if(r != null) {
                // Log successful access
                DataFetchPerformanceMeasurement s = new DataFetchPerformanceMeasurement(dataFetchStart, signal.getUniqueIdentifier(), 
                        cc.cache.getLastUpdate(signal, cc.windowLength).getTime(), 
                        cc.cache.getDataBackend(), cc.windowLength, r.size(), dataFetchEnd-dataFetchStart);
                perfTracker.addDataFetchMeasurement(s);
                cacheConfigUsed = cc;
                break;
            }
        }
        
        if(r == null) {
            // None of the cached had the requested data available
            throw new VizzlyException("Client requested an unknown signal.");
        }
        
        if(r.size() == 0) {
            // There is a result, but it is empty. No need for further processing.
            return r;
        }
        
        if(!ignoreLocation && windowLengthSec == cacheConfigUsed.windowLength) {
            // Result found, temporal detail level matches already, do not group by equal location
            return r;
        }
        
        // Data is not yet on desired detail level but needs to be further aggregated on-the-fly 
        Vector<TimedLocationValue> agg = null;
        long dataFetchStart = System.currentTimeMillis();
        if(r.get(0).location != null && !ignoreLocation) {
            agg = DataAggregationUtil.aggregateDataWithLocation(r,  windowLengthSec);
        } else {
            agg = DataAggregationUtil.aggregateDataNullLocation(r,  windowLengthSec);
        }
        long dataFetchEnd = System.currentTimeMillis();
        DataFetchPerformanceMeasurement s = new DataFetchPerformanceMeasurement(dataFetchStart, signal.getUniqueIdentifier(), 
                cacheConfigUsed.cache.getLastUpdate(signal, cacheConfigUsed.windowLength).getTime(), 
                DataBackend.LIVEAGGREGATION, windowLengthSec, agg.size(), dataFetchEnd-dataFetchStart);
        perfTracker.addDataFetchMeasurement(s);
        
        return agg;
    }
    
    public int getNumberOfCaches() {
        return caches.size();
    }
    
    public void updateCachedSignal(VizzlySignal signal) {
        // Under the assumed order of cache configurations (largest cache last), first
        // update last cache configuration and then all others
        if(updateCachedSignalFromSource(caches.lastElement().cache, signal, 
                caches.lastElement().windowLength) && caches.size() > 1) {
            for(int i = 0; i < caches.size()-1; i++) {
                AbstractCache toCopy = caches.get(i).cache;
                int windowLengthNew = caches.get(i).windowLength;
                if(!toCopy.isInCache(signal, windowLengthNew)) {
                    toCopy.addSignal(signal);
                }
                updateCachedSignalByAggregation(caches.lastElement().cache, toCopy, signal, 
                        caches.lastElement().windowLength, windowLengthNew);
            }
        }
    }

    // The lowest supported aggregation level must be fetched from 
    // the DB. Here, we cannot aggregate anything because either time
    // or location information would be lost
    private Boolean updateCachedSignalFromSource(AbstractCache cache, VizzlySignal signal, int windowLengthSec) {
        Long timeFilterStart = null;
        // Check if the respective cache entry is not already fresh enough
        if(cache.isInCache(signal, windowLengthSec)) {
            cal = Calendar.getInstance();
            // Do not update again if the signal has just been updated
            if(cache.getLastUpdate(signal, windowLengthSec).getTime() > (cal.getTimeInMillis()-UPDATE_DISTANCE_MSEC)) {
                //log.debug("Not updating " + signal.getUniqueIdentifier() + ", now = " + _cal.getTimeInMillis() + ", last update = " + d.getLastUpdate().getTime());
                return true;
            }
            timeFilterStart = cache.getEndTime(signal, windowLengthSec)-windowLengthSec;
            log.debug("timeFilterStart = " + timeFilterStart);
        }
        
        try {
            Calendar cal = Calendar.getInstance();
            signalLastUpdateAttempt.put(signal, cal.getTime().getTime());
            AbstractDataReader reader = dataReaderRegistry.getDataReader(signal.dataSource.type);
            Vector<TimedLocationValue> r = reader.getSignalData(signal, timeFilterStart, null, 0);
            if(r != null && r.size() > 0) {
                AggregationLevelLookup.getInstance().updateSamplingRateEstimation(signal, r);
                cache.updateCacheEntry(signal, windowLengthSec, r);
                return true;
            }
        } catch(VizzlyException e) {
            log.error(e);
        }
        
        return false;
    }
    
    @SuppressWarnings("unused")
    private void updateByCopyFromOtherCache(AbstractCache fromCache, AbstractCache toCache, VizzlySignal signal, int windowLengthSec) {
        Long timeFilterStart = null;
        if(toCache.isInCache(signal, windowLengthSec)) {
            cal = Calendar.getInstance();
            // Do not update again if the signal has just been updated
            if(toCache.getLastUpdate(signal, windowLengthSec).getTime() > (cal.getTimeInMillis()-UPDATE_DISTANCE_MSEC)) {
                return;
            }
            timeFilterStart = toCache.getEndTime(signal, windowLengthSec)-windowLengthSec;
            //log.debug("timeFilterStart = " + timeFilterStart);
        }
        
        Vector<TimedLocationValue> data = fromCache.getSignalData(signal, windowLengthSec, timeFilterStart, null, false);
        if(data == null) {
            log.error("Data is not available in other cache?!");
            return;
        }
        toCache.updateCacheEntry(signal, windowLengthSec, data);
    }

    // All but the lowest supported aggregation levels are derived
    // from further aggregating cached contents
    private void updateCachedSignalByAggregation(AbstractCache fromCache, AbstractCache toCache, VizzlySignal signal, 
            int windowLengthSecFrom, int windowLengthSecTo) {
        Long timeFilterStart = null;
        if(toCache.isInCache(signal, windowLengthSecTo)) {
            cal = Calendar.getInstance();
            // Do not update again if the signal has just been updated
            if(toCache.getLastUpdate(signal, windowLengthSecTo).getTime() > (cal.getTimeInMillis()-UPDATE_DISTANCE_MSEC)) {
                return;
            }
            timeFilterStart = toCache.getEndTime(signal, windowLengthSecTo)-windowLengthSecTo;
            //log.debug("timeFilterStart = " + timeFilterStart);
        } 
        
        Vector<TimedLocationValue> inputData = fromCache.getSignalData(signal, windowLengthSecFrom, timeFilterStart, null, false);
        if(inputData == null) {
            log.error("Data of lower resolution is not available?!");
            return;
        }
        toCache.updateCacheEntry(signal, windowLengthSecTo, inputData);
    }
    
    public void scheduleSignalForRemoval(VizzlySignal signal) {
        synchronized(signalsToRemove) {
            if(!signalsToRemove.contains(signal)) {
                signalsToRemove.add(signal);
            }
        }
    }
    
    public void performSignalRemoval(VizzlySignal signal) {
        Boolean removeSuccessful = true;
        for(CacheConfiguration cc : caches) {
            removeSuccessful = removeSuccessful && cc.cache.removeSignal(signal);
        }
        AggregationLevelLookup.getInstance().deleteSignalEstimation(signal);
        if(removeSuccessful) {
            synchronized(signalsToRemove) {
                signalsToRemove.remove(signal);
            }
        }
        signalLastUpdateAttempt.remove(signal);
    }
    
    // Called from web page to show the users that there are pending requests
    @SuppressWarnings("unchecked")
    public Vector<VizzlySignal> getSignalsToRemove() {
        return (Vector<VizzlySignal>)(signalsToRemove.clone());
    }

    public Long getFirstPacketTimestamp(VizzlySignal signal) {
        return caches.lastElement().cache.getFirstPacketTimestamp(signal, caches.lastElement().windowLength);
    }

    public Long getLastPacketTimestamp(VizzlySignal signal) {
        return caches.lastElement().cache.getLastPacketTimestamp(signal, caches.lastElement().windowLength);
    }

    public Boolean isInitialized() {
        Boolean ret = true;
        for(CacheConfiguration cc : caches) {
            ret = ret && cc.cache.isInitialized();
        }
        return ret;
    }

    public Vector<CachedDataInfo> getCachedDataInfo(int cacheIdx) {
        return caches.get(cacheIdx).cache.getCachedDataInfo();
    }

    public Vector<VizzlySignal> getSignals(int cacheIdx) {
        return caches.get(cacheIdx).cache.getSignals();
    }

    public long getUptime(int cacheIdx) {
        return caches.get(cacheIdx).cache.getUptime();
    }

    public long getCacheSize(int cacheIdx) {
        return caches.get(cacheIdx).cache.getCacheSize();
    }

    public int getNumberOfSeenSignals(int cacheIdx) {
        return caches.get(cacheIdx).cache.getNumberOfSeenSignals();
    }

    public int getNumberOfCacheEntries(int cacheIdx) {
        return caches.get(cacheIdx).cache.getNumberOfCacheEntries();
    }

    public long getNumberOfCacheRequests(int cacheIdx) {
        return caches.get(cacheIdx).cache.getNumberOfCacheRequests();
    }

    public long getNumberOfCacheHits(int cacheIdx) {
        return caches.get(cacheIdx).cache.getNumberOfCacheHits();
    }

    public long getNumberOfCacheMisses(int cacheIdx) {
        return caches.get(cacheIdx).cache.getNumberOfCacheMisses();
    }
    
    public String getCacheDescription(int cacheIdx) {
        return caches.get(cacheIdx).cache.getCacheDescription();
    }
    
    public Boolean isInCache(VizzlySignal signal) {
        // The last/largest cache is filled first
        if(!caches.lastElement().cache.getSignals().contains(signal)) {
            caches.lastElement().cache.addSignal(signal);
            signalLastUpdateAttempt.put(signal, 0L);
        }
        return caches.lastElement().cache.isInCache(signal, caches.lastElement().windowLength);
    }
    
    public Vector<VizzlySignalCurrentness> getSignalsWithCurrentness() {
        Vector<VizzlySignalCurrentness> ret = new Vector<VizzlySignalCurrentness>();
        for(VizzlySignal s : signalLastUpdateAttempt.keySet()) {
            ret.add(new VizzlySignalCurrentness(s, signalLastUpdateAttempt.get(s)));
        }
        return ret;
    }
  

}
