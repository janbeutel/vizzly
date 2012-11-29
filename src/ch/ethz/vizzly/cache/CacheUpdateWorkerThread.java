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

import org.apache.log4j.Logger;

import ch.ethz.vizzly.datatype.VizzlySignal;

/**
 * This class implements a thread that updated cached contents.
 * @author Matthias Keller
 *
 */
public class CacheUpdateWorkerThread extends Thread {

    private Boolean running = false;

    private int workerId;

    private final long sleepMsec = 5000;

    private CacheUpdateWorkerSynchronization workerSync = null;
    
    private CacheManager cache = null;

    /**
     * Log.
     */
    private static Logger log = Logger.getLogger(CacheUpdateWorkerThread.class);

    public CacheUpdateWorkerThread(int workerId, CacheUpdateWorkerSynchronization workerSync, CacheManager cache) {
        this.setName("Cache Updater " + workerId);
        this.workerId = workerId;
        this.workerSync = workerSync;
        this.cache = cache;
        running = true;
    }

    public void setRunning(Boolean running) {
        log.debug("Set running to " + running);
        this.running = running;
    }

    public void run() {
        try {
            // Sleep at the beginning so that the server can first start everything
            sleep(sleepMsec*2);
            while(running) {
     
                // Worker 0 is responsible for cleaning
                if(workerId == 0) {
                    while(true) {
                        VizzlySignal nextRemoval = workerSync.getNextPendingRemoval(workerId);
                        if(nextRemoval == null) {
                            break;
                        }
                        log.info("Removing a signal from the cache.");
                        cache.performSignalRemoval(nextRemoval);
                    }
                }

                // All workers are responsible for updating cache contents
                VizzlySignal nextSignal = workerSync.getNextSignal(workerId);
                if(nextSignal != null) {
                    cache.updateCachedSignal(nextSignal);
                    workerSync.signalWorkerFinished(workerId);
                }
                sleep(sleepMsec);
            }
        } catch(InterruptedException e) {
            log.debug("Interrupted", e);
        } catch(Exception e) {
            log.error("Worker dying", e);
        }
    }

}