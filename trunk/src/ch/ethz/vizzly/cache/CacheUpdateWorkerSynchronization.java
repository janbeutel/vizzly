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

import java.util.Collections;
import java.util.Comparator;
import java.util.Vector;

import org.apache.log4j.Logger;

import ch.ethz.vizzly.VizzlyStateContainer;
import ch.ethz.vizzly.datatype.CachedDataInfo;
import ch.ethz.vizzly.datatype.VizzlySignal;

/**
 * This class synchronizes multiple, concurrently running worker
 * threads that are used for continuously pulling new data.
 * @author Matthias Keller
 *
 */
public class CacheUpdateWorkerSynchronization {

    public static final String SERVLET_ATTRIB_KEY = "workerSync";
    
    private CacheManager cache = null;

    private VizzlySignal[] workerSignal = null;

    private CacheUpdateWorkerThread[] workers = null;

    private Boolean threadsStarted = false;
  
    private int numWorkers = 0;
    
    private Object workerSyncLock = new Object();
    
    /**
     * Log.
     */
    private static Logger log = Logger.getLogger(CacheUpdateWorkerSynchronization.class);

    public CacheUpdateWorkerSynchronization(VizzlyStateContainer vizzlyState) {
        cache = vizzlyState.getCacheManager();
    }

    public void startUpdaterThreads(int numWorkers) {
        synchronized(threadsStarted) {
            if(!threadsStarted) {
                this.numWorkers = numWorkers;
                workers = new CacheUpdateWorkerThread[numWorkers];
                workerSignal = new VizzlySignal[numWorkers];
                for(int i = 0; i < workerSignal.length; i++) {
                    workerSignal[i] = null;
                }
                for(int i = 0; i < numWorkers; i++) {
                    if(workers[i] == null || !workers[i].isAlive()) {
                        workers[i] = new CacheUpdateWorkerThread(i, this, cache);
                        workers[i].start();
                    }
                }
                log.info("Initialization of " + numWorkers + " updater threads done.");
                threadsStarted = true;
            }
        }
    }

    public VizzlySignal getNextPendingRemoval(int workerId) {
        if(cache.isInitialized() && cache.getSignalsToRemove().size() > 0) {
            VizzlySignal toProcess = null;
            // After successful removal, the CacheManager removes the element from the list
            toProcess = cache.getSignalsToRemove().firstElement();
            synchronized(workerSyncLock) {
                for(int i = 0; i < workerSignal.length; i++) {
                    // Check if the next signal is currently updated by another thread
                    if(workerSignal[i] != null && workerSignal[i].equals(toProcess)) {
                        log.debug("Worker " + workerId + ": Next signal for removal is still in processing.");
                        return null;
                    }
                }
                workerSignal[workerId] = toProcess;
            }
            return toProcess;
        }
        return null;
    }

    public VizzlySignal getNextSignal(int workerId) {
        if(cache.isInitialized() && cache.getNumberOfSeenSignals(cache.getNumberOfCaches()-1) > 0) {
            VizzlySignal toProcess = null;
            
            Vector<CachedDataInfo> cacheInfo = cache.getCachedDataInfo(cache.getNumberOfCaches()-1);
            Comparator<CachedDataInfo> comp = CachedDataInfo.getComparator(CachedDataInfo.SortParameter.LAST_UPDATE_ASCENDING);
            Collections.sort(cacheInfo, comp);
            // Iterate through all available signals until a conflict-free signal is found
            synchronized(workerSyncLock) {
                for(int j=0; j < cacheInfo.size(); j++) {
                    toProcess = cacheInfo.get(j).signal;
                    for(int i=0; i < workerSignal.length; i++) {
                        // Check if the next signal is currently updated by another thread
                        // Additionally avoid accessing the same data source (e.g., a MySQL table) 
                        // in parallel as this is often slower
                        if(workerSignal[i] != null) {
                            if(workerSignal[i].equals(toProcess)) {
                                log.debug("Worker " + workerId + ": Next signal is already updated by another worker.");
                                toProcess = null;
                                break;
                            } else if(workerSignal[i].dataSource.equals(toProcess.dataSource)) {
                                log.debug("Worker " + workerId + ": Data source of next signal is already being accessed by another worker.");
                                toProcess = null;
                                break;
                            }
                        }
                    }
                    if(toProcess != null) {
                        workerSignal[workerId] = toProcess;
                        return toProcess;
                    }
                }
            }
        }
        return null;
    }

    public void signalWorkerFinished(int workerId) {
        if(workerId >= 0 && workerId < workerSignal.length) {
            synchronized(workerSyncLock) {
                workerSignal[workerId] = null;
            }
        } else {
            log.error("Incorrect worker id: " + workerId + ", " + workerSignal.length);
        }
    }

    public void terminateThreads() {
        for(int i = 0; i < numWorkers; i++) {
            workers[i].setRunning(false);
            workers[i].interrupt();
        }
    }
    
    public VizzlySignal[] getWorkerSignals() {
        return (VizzlySignal[])workerSignal.clone();
    }

}
