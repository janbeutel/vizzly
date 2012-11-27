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
    @SuppressWarnings("unused")
    private static Logger log = Logger.getLogger(DbPerformanceTracker.class);

    private Vector<DataFetchPerformanceMeasurement> dataFetchMeas = null;
    
    private Vector<UserRequestPerformanceMeasurement> userRequestMeas = null;

    private DbPerformanceTrackerPersistenceThread persistenceThread = null;

    public DbPerformanceTracker() {
        persistenceThread = new DbPerformanceTrackerPersistenceThread(this);
        dataFetchMeas = new Vector<DataFetchPerformanceMeasurement>();
        userRequestMeas = new Vector<UserRequestPerformanceMeasurement>();
        persistenceThread.start();
    }

    /* Data is added to memory first, and then asynchronously copied to the database */
    public void addDataFetchMeasurement(DataFetchPerformanceMeasurement p) {
        synchronized(dataFetchMeas) {
            dataFetchMeas.add(p);
        }
    }

    /* Data is added to memory first, and then asynchronously copied to the database */
    public void addUserRequestMeasurement(UserRequestPerformanceMeasurement p) {
        synchronized(userRequestMeas) {
            userRequestMeas.add(p);
        }
    }
    
    @SuppressWarnings("unchecked")
    public Vector<DataFetchPerformanceMeasurement> copyAndClearDataFetchSamples() {
        Vector<DataFetchPerformanceMeasurement> ret = null;
        synchronized(dataFetchMeas) {
            ret = (Vector<DataFetchPerformanceMeasurement>)dataFetchMeas.clone();
            dataFetchMeas.clear();
        }
        return ret;
    }
    
    @SuppressWarnings("unchecked")
    public Vector<UserRequestPerformanceMeasurement> copyAndClearUserRequestSamples() {
        Vector<UserRequestPerformanceMeasurement> ret = null;
        synchronized(userRequestMeas) {
            ret = (Vector<UserRequestPerformanceMeasurement>)userRequestMeas.clone();
            userRequestMeas.clear();
        }
        return ret;
    }

    public void stopApplication() {
        persistenceThread.setRunning(false);
    }

}
