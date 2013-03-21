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

import java.util.Collections;
import java.util.Vector;

import ch.ethz.vizzly.cache.CacheFactory;
import ch.ethz.vizzly.cache.CacheManager;
import ch.ethz.vizzly.cache.memory.MemCache;
import ch.ethz.vizzly.datareader.CsvDataReader;
import ch.ethz.vizzly.datareader.DataReaderRegistry;
import ch.ethz.vizzly.datareader.gsn.GsnDataReader;
import ch.ethz.vizzly.datatype.CacheConfiguration;
import ch.ethz.vizzly.datatype.CacheSpec;
import ch.ethz.vizzly.datatype.VizzlyException;
import ch.ethz.vizzly.performance.AbstractPerformanceTracker;
import ch.ethz.vizzly.performance.DbPerformanceTracker;
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
    

    public VizzlyStateContainer(VizzlyConfiguration config) throws VizzlyException {
       initStateContainer(config); 
    }
    
    private synchronized void initStateContainer(VizzlyConfiguration config) throws VizzlyException {
        if(!stateInitialized) {
            dataReaderRegistry = new DataReaderRegistry();
            
            // Initialize GSN data source handling
            GsnDataReader gsnDataReader = new GsnDataReader();
            dataReaderRegistry.addDataReader("gsn", gsnDataReader);
            
            // Initialize and register CSV file reader
            CsvDataReader csvDataReader = new CsvDataReader();
            dataReaderRegistry.addDataReader("csv", csvDataReader);
            
            // Initialize performance tracker
            if(config.useSqlDatabase() && config.isPerformanceTrackerEnabled()) {
                perfTracker = new DbPerformanceTracker();
            } else {
                perfTracker = new DummyPerformanceTracker();
            }
            
            // Initialize caches
            Vector<CacheSpec> configCaches = config.getCacheList();
          
            // Caches are assumed to be sorted by its ascending window length
            Collections.sort(configCaches);
          
            // Create caches as specified in configuration file
            Vector<CacheConfiguration> caches = new Vector<CacheConfiguration>();
            for(CacheSpec s : configCaches) {
                caches.add(new CacheConfiguration(CacheFactory.createCache(s), s.windowLength));
            }
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
