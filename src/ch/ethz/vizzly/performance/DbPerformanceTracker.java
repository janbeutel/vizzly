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

package ch.ethz.vizzly.performance;

import java.util.Vector;
import java.util.concurrent.Semaphore;

import org.apache.log4j.Logger;

/**
 * This class collects performance measurements that are taken during cache accesses.
 * Measurements are first put into a queue and then stored in a MySQL database.
 * @author Matthias Keller
 *
 */
public class DbPerformanceTracker extends AbstractPerformanceTracker {

    /**
     * Log.
     */
    private static Logger log = Logger.getLogger(DbPerformanceTracker.class);

    private Vector<DataFetchPerformanceMeasurement> dataFetchMeas = null;
    
    private Vector<UserRequestPerformanceMeasurement> userRequestMeas = null;

    private final Semaphore writeAccessDataFetch = new Semaphore(1);
    
    private final Semaphore writeAccessUserRequest = new Semaphore(1);
    
    private DbPerformanceTrackerPersistenceThread persistenceThread = null;

    public DbPerformanceTracker() {
        persistenceThread = new DbPerformanceTrackerPersistenceThread(this);
        dataFetchMeas = new Vector<DataFetchPerformanceMeasurement>();
        userRequestMeas = new Vector<UserRequestPerformanceMeasurement>();
        persistenceThread.start();
    }

    /* Data is added to memory first, and then asynchronously copied to the database */
    public void addDataFetchMeasurement(DataFetchPerformanceMeasurement p) {
        try {
            writeAccessDataFetch.acquire();
            dataFetchMeas.add(p);
            writeAccessDataFetch.release();
        } catch(InterruptedException e) {
            log.error(e);
        }
    }
    
    /* Data is added to memory first, and then asynchronously copied to the database */
    public void addUserRequestMeasurement(UserRequestPerformanceMeasurement p) {
        try {
            writeAccessUserRequest.acquire();
            userRequestMeas.add(p);
            writeAccessUserRequest.release();
        } catch(InterruptedException e) {
            log.error(e);
        }
    }
    
    @SuppressWarnings("unchecked")
    public Vector<DataFetchPerformanceMeasurement> copyAndClearDataFetchSamples() {
        Vector<DataFetchPerformanceMeasurement> ret = null;
        try {
            writeAccessDataFetch.acquire();
             ret = (Vector<DataFetchPerformanceMeasurement>)dataFetchMeas.clone();
            dataFetchMeas.clear();
            writeAccessDataFetch.release();
        } catch(InterruptedException e) {
            log.error(e);
        }
        return ret;
    }
    
    @SuppressWarnings("unchecked")
    public Vector<UserRequestPerformanceMeasurement> copyAndClearUserRequestSamples() {
        Vector<UserRequestPerformanceMeasurement> ret = null;
        try {
            writeAccessUserRequest.acquire();
             ret = (Vector<UserRequestPerformanceMeasurement>)userRequestMeas.clone();
            userRequestMeas.clear();
            writeAccessUserRequest.release();
        } catch(InterruptedException e) {
            log.error(e);
        }
        return ret;
    }

    public void stopApplication() {
        persistenceThread.setRunning(false);
    }

}
