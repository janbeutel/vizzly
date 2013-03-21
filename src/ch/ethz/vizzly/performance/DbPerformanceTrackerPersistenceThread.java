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

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Vector;

import javax.naming.InitialContext;
import javax.naming.NamingException;
import javax.sql.DataSource;

import org.apache.log4j.Logger;

/**
 * This thread periodically copies performance measurements from the memory to a MySQL database
 * @author Matthias Keller
 *
 */
public class DbPerformanceTrackerPersistenceThread extends Thread {

    /**
     * Log.
     */
    private static Logger log = Logger.getLogger(DbPerformanceTrackerPersistenceThread.class);

    private final long sleepMsec = 60000; /* 10 minutes */

    private DbPerformanceTracker tracker = null;

    private DataSource ds = null;

    private Boolean running = false;

    final private String performanceDataFetchTable = "viz_perf_data_fetch";

    final private String performanceUserRequestTable = "viz_perf_user_request";

    final private String performanceDataFetchTableCreate = "CREATE TABLE IF NOT EXISTS " + performanceDataFetchTable + " (" +
            "`timestamp` bigint(20) NOT NULL," +
            "`signal_name` varchar(255) NOT NULL," +
            "`last_update` bigint(20) NOT NULL," +
            "`data_backend` varchar(20) NOT NULL," +
            "`window_length` int(11) NOT NULL," +
            "`result_size` int(11) NOT NULL," +
            "`elapsed_time` bigint(20) NOT NULL" +
            ") ENGINE=InnoDB DEFAULT CHARSET=latin1;";
    
    
    final private String performanceUserRequestTableCreate = "CREATE TABLE IF NOT EXISTS " + performanceUserRequestTable + " (" +
            "`timestamp` bigint(20) NOT NULL," +
            "`num_requested_signals` smallint(2) NOT NULL," +
            "`num_returned_lines` int(11) NOT NULL," +
            "`request_prepare_time` bigint(20) NOT NULL," +
            "`data_fetch_time` bigint(20) NOT NULL," +
            "`request_finalize_time` bigint(20) NOT NULL" +
            ") ENGINE=InnoDB DEFAULT CHARSET=latin1;";

    public DbPerformanceTrackerPersistenceThread(DbPerformanceTracker tracker) {
        this.setName("Performance Tracker");
        this.tracker = tracker;
        try {
            InitialContext ctx = new InitialContext();
            // Perform JNDI lookup - database connection is configured in contexts/sensorviz.xml
            ds = (DataSource)ctx.lookup("VizzlyDS");

            // Create table, if needed
            Connection conn = ds.getConnection();
            Statement stmt = conn.createStatement();
            stmt.executeUpdate(performanceDataFetchTableCreate);
            stmt.close();
            stmt = null;
            stmt =conn.createStatement();
            stmt.executeUpdate(performanceUserRequestTableCreate);
            stmt.close();
            stmt = null;
            conn.close();
            conn = null;
        } catch(NamingException e) {
            log.error("Failed to connect to database server.", e);
        } catch(SQLException e) {
            log.error(e);
        }
        running = true;
    }

    public void setRunning(Boolean running) {
        this.running = running;
    }
    
    public void run() {
        while(running) {
            try {
                Connection conn = null;
                // Data fetch performance measurements
                Vector<DataFetchPerformanceMeasurement> dataFetchMeas = tracker.copyAndClearDataFetchSamples();
                // Add data to DB
                if(dataFetchMeas.size() > 0) {
                    conn = ds.getConnection();
                    conn.setAutoCommit(false);
                    PreparedStatement p = conn.prepareStatement("INSERT INTO " + performanceDataFetchTable 
                            + " (timestamp, signal_name, last_update, data_backend, window_length," +
                            " result_size, elapsed_time) VALUES (?, ?, ?, ?, ?, ?, ?)");

                    for(DataFetchPerformanceMeasurement m : dataFetchMeas) {
                        p.setLong(1, m.timestamp);
                        p.setString(2, m.signalName);
                        p.setLong(3, m.lastUpdate);
                        p.setString(4, m.getDataBackendString());
                        p.setInt(5, m.windowLengthSec);
                        p.setInt(6, m.resultSize);
                        p.setLong(7, m.elapsedTime);
                        p.addBatch();
                    }
                    p.executeBatch();
                    conn.commit();
                    p.close();
                    p = null;
                }
                
                // User request performance measurements
                Vector<UserRequestPerformanceMeasurement> userRequestMeas = tracker.copyAndClearUserRequestSamples();
                // Add data to DB
                if(userRequestMeas.size() > 0) {
                    if(conn == null) {
                        conn = ds.getConnection(); 
                    }
                    conn.setAutoCommit(false);
                    PreparedStatement p = conn.prepareStatement("INSERT INTO " + performanceUserRequestTable 
                            + " (timestamp, num_requested_signals, num_returned_lines, request_prepare_time, " +
                            "data_fetch_time, request_finalize_time) VALUES (?, ?, ?, ?, ?, ?)");

                    for(UserRequestPerformanceMeasurement m : userRequestMeas) {
                        p.setLong(1, m.getTimestamp());
                        p.setInt(2, m.getNumRequestedSignals());
                        p.setInt(3, m.getNumReturnedLines());
                        p.setLong(4, m.getRequestPrepareTime());
                        p.setLong(5, m.getDataFetchTime());
                        p.setLong(6, m.getRequestFinalizeTime());
                        p.addBatch();
                    }
                    p.executeBatch();
                    conn.commit();
                    p.close();
                    p = null;
                }
                
                if(conn != null) {
                    conn.close();
                    conn = null;
                }
                sleep(sleepMsec);
            } catch(InterruptedException e) {
                log.warn("Interrupted", e);
            } catch(SQLException e) {
                log.error(e);
            }
        }
    }
}
