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

import java.util.Vector;
import java.util.concurrent.Semaphore;

import org.apache.log4j.Logger;

import ch.ethz.vizzly.VizzlyStateContainer;
import ch.ethz.vizzly.datatype.VizzlySignal;

/**
 * This class synchronizes multiple, concurrently running worker
 * threads that are used for continuously pulling new data.
 * @author Matthias Keller
 *
 */
public class CacheUpdateWorkerSynchronization {

    private CacheManager cache = null;

    private VizzlySignal[] workerSignal = null;

    private int currentSignalIdx = -1;

    private final Semaphore access = new Semaphore(1);
    
    private CacheUpdateWorkerThread[] workers = null;

    private Boolean threadsStarted = false;
  
    private int numWorkers = 0;
    
    /**
     * Log.
     */
    private static Logger log = Logger.getLogger(CacheUpdateWorkerSynchronization.class);

    public CacheUpdateWorkerSynchronization(VizzlyStateContainer vizzlyState) {
        cache = vizzlyState.getCacheManager();
    }
    
    public synchronized void startUpdaterThreads(int numWorkers) {
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

    public VizzlySignal getNextPendingRemoval(int workerId) {
        if(cache.isInitialized() && cache.getSignalsToRemove().size() > 0) {
            VizzlySignal toProcess = null;
            try {
                access.acquire();
                // After successful removal, the CacheManager removes the element from the list
                toProcess = cache.getSignalsToRemove().firstElement();
                for(int i = 0; i < workerSignal.length; i++) {
                    // Check if the next signal is currently updated by another thread
                    if(workerSignal[i] != null && workerSignal[i].equals(toProcess)) {
                        log.debug("Worker " + workerId + ": Next signal for removal is still in processing.");
                        access.release();
                        return null;
                    }
                }
                workerSignal[workerId] = toProcess;
                access.release();
            } catch(InterruptedException e) {
                log.debug("Interrupted in next signal", e);
            }
            return toProcess;
        }
        return null;
    }
    
    public VizzlySignal getNextSignal(int workerId) {
        if(cache.isInitialized() && cache.getNumberOfSeenSignals(cache.getNumberOfCaches()-1) > 0) {
            VizzlySignal toProcess = null;
            try {
                access.acquire();
                Vector<VizzlySignal> signals = cache.getSignals(cache.getNumberOfCaches()-1);
                currentSignalIdx = (currentSignalIdx+1) % signals.size();
                toProcess = signals.get(currentSignalIdx);
                for(int i = 0; i < workerSignal.length; i++) {
                    // Check if the next signal is currently updated by another thread
                    if(workerSignal[i] != null && workerSignal[i].equals(toProcess)) {
                        log.debug("Worker " + workerId + ": Next signal is still in processing.");
                        access.release();
                        return null;
                    }
                }
                workerSignal[workerId] = toProcess;
                access.release();
            } catch(InterruptedException e) {
                log.debug("Interrupted in next signal", e);
            }
            return toProcess;
        }
        return null;
    }

    public void signalWorkerFinished(int workerId) {
        try {
            access.acquire();
            if(workerId >= 0 && workerId < workerSignal.length) {
                workerSignal[workerId] = null;
                //log.debug("Worker " + workerId + ": Release.");
            } else {
                log.error("Incorrect worker id: " + workerId + ", " + workerSignal.length);
            }
            access.release();
        } catch(InterruptedException e) {
            log.debug("Interrupted in finished", e);
        }
    }
    
    public void terminateThreads() {
        for(int i = 0; i < numWorkers; i++) {
            workers[i].setRunning(false);
            workers[i].interrupt();
        }
    }

}
