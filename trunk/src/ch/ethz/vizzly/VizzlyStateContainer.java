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

package ch.ethz.vizzly;

import java.util.Vector;

import ch.ethz.vizzly.cache.CacheConfiguration;
import ch.ethz.vizzly.cache.CacheManager;
import ch.ethz.vizzly.cache.memory.MemCache;
import ch.ethz.vizzly.datareader.CsvDataReader;
import ch.ethz.vizzly.datareader.DataReaderRegistry;
import ch.ethz.vizzly.datareader.gsn.GsnDataReader;
import ch.ethz.vizzly.performance.AbstractPerformanceTracker;
import ch.ethz.vizzly.performance.DummyPerformanceTracker;

/**
 * This class contains all state that is needed to run the application. For example,
 * the memory cache is initialized here. During runtime, this object is stored
 * in the servlet context.
 * @author Matthias Keller
 *
 */
public class VizzlyStateContainer {
    
    public static final String SERVLET_ATTRIB_KEY = "vizzlyState";
    
    private Boolean stateInitialized = false;
    
    private long numberOfRequests = 0L;
    
    /**
     * All data readers must be registered here
     */
    private DataReaderRegistry dataReaderRegistry = null;
    
    /**
     * This variable provides access to all cache activities
     */
    private CacheManager cacheManager = null;
    
    /**
     * Performance tracker for logging the data access performance.
     */
    private AbstractPerformanceTracker perfTracker = null;
    

    public VizzlyStateContainer() {
       initStateContainer(); 
    }
    
    private synchronized void initStateContainer() {
        if(!stateInitialized) {
            dataReaderRegistry = new DataReaderRegistry();
            
            // Initialize GSN data source handling
            GsnDataReader gsnDataReader = new GsnDataReader();
            dataReaderRegistry.addDataReader("gsn", gsnDataReader);
            
            // Initialize and register CSV file reader
            CsvDataReader csvDataReader = new CsvDataReader();
            dataReaderRegistry.addDataReader("csv", csvDataReader);
            
            // Initialize performance tracker
            perfTracker = new DummyPerformanceTracker();
            //perfTracker = new DbPerformanceTracker();
            
            // Initialize caches
            Vector<CacheConfiguration> caches = new Vector<CacheConfiguration>();
            MemCache memCache = new MemCache();
            caches.add(new CacheConfiguration(memCache, AggregationLevelLookup.MIN_WINDOW_LENGTH_SEC));
            
            // Can only be activated when proper data source has been configured beforehand
            //SqlDbCache sqlDbCache = new SqlDbCache();
            //caches.add(new CacheConfiguration(sqlDbCache, AggregationLevelLookup.MIN_WINDOW_LENGTH_SEC));

            // TODO: Order caches by ascending window length
            cacheManager = new CacheManager(caches, dataReaderRegistry, perfTracker);

            stateInitialized = true;
        }
    }
    
    public CacheManager getCacheManager() {
        return cacheManager;
    }
    
    public DataReaderRegistry getDataReaderRegistry() {
        return dataReaderRegistry;
    }
    
    public AbstractPerformanceTracker getPerformanceTracker() {
        return perfTracker;
    }
    
    public void incrNumberOfRequests() {
        numberOfRequests++;
    }
    
    public long getNumberOfRequests() {
        return numberOfRequests;
    }

}
