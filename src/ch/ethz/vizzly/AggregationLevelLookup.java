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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;

import javax.naming.InitialContext;
import javax.naming.NamingException;
import javax.sql.DataSource;

import org.apache.log4j.Logger;

import ch.ethz.vizzly.cache.CacheManager;
import ch.ethz.vizzly.datatype.VizzlyException;
import ch.ethz.vizzly.datatype.VizzlySignal;
import ch.ethz.vizzly.datatype.readings.TimedLocationValue;

/**
 * This class calculates the returned level of detail for a request. Given a requested signal including
 * a time period of interest, it is first decided if unaggregated data can be displayed. If not,
 * the best aggregation interval is determined.
 * @author Matthias Keller
 *
 */
public class AggregationLevelLookup {

    public static final int MIN_WINDOW_LENGTH_SEC = 240;
    
    /**
     * Minimal wait time before again updating the database.
     */
    public final long MIN_DB_UPDATE_WAIT = 120000L;

    private Boolean isInitialized = false;
    
    private DataSource ds = null;
    
    private Boolean useDatabase = false;
    
    private long lastDatabaseUpdate = 0L;
    
    /**
     * Log.
     */
    private static Logger log = Logger.getLogger(AggregationLevelLookup.class);

    final private String rateEstimatorTable = "viz_rate_estimation";

    final private String rateEstimatorTableCreate = "CREATE TABLE IF NOT EXISTS " + rateEstimatorTable + " (" +
            "`id` int(11) NOT NULL AUTO_INCREMENT," +
            "`viz_signal` BLOB NOT NULL," +
            "`rate_estimation` BLOB NOT NULL," +
            "`last_update` TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP," +
            "PRIMARY KEY (`id`)" +
            ") ENGINE=InnoDB DEFAULT CHARSET=latin1;";

    /**
     * Singleton
     */
    private static AggregationLevelLookup instance = null;

    private ConcurrentHashMap<VizzlySignal,SamplingRateEstimation> rateEstimators = null;
    
    private ConcurrentHashMap<VizzlySignal,Integer> dbIdLookupTable = null;

    private AggregationLevelLookup() {}
    
    public void init(Boolean useDatabase) {
        synchronized(isInitialized) {
            if(isInitialized) {
                return;
            }
            rateEstimators = new ConcurrentHashMap<VizzlySignal,SamplingRateEstimation>();
            if(useDatabase) {
                this.useDatabase = true;
                initDatabase();
                dbIdLookupTable = new ConcurrentHashMap<VizzlySignal,Integer>();
                loadFromDatabase();
            }
            isInitialized = true;
        }
    }

    public static synchronized AggregationLevelLookup getInstance() {
        if(instance == null) {
            instance = new AggregationLevelLookup();
        }
        return instance;
    }

    public Boolean canLoadUnaggregatedData(VizzlySignal signal, Long timeFilterStart, 
            Long timeFilterEnd, int maxNumPoints, CacheManager cache) throws VizzlyException {
        Boolean ret = false;
        SamplingRateEstimation estim = rateEstimators.get(signal);
        if(estim == null) {
            return ret;
        }
        Long signalStart = cache.getFirstPacketTimestamp(signal);
        Long signalEnd = cache.getLastPacketTimestamp(signal);
        // Two cases in which eventually no data will be provided
        if(timeFilterStart != null && timeFilterStart > signalEnd) {
            return true;
        }
        if(timeFilterEnd != null && timeFilterEnd < signalStart) {
            return true;
        }
        Long timeStart = (timeFilterStart == null) ? signalStart : ((timeFilterStart < signalStart) ? signalStart : timeFilterStart);
        Long timeEnd = (timeFilterEnd == null) ? signalEnd : ((timeFilterEnd > signalEnd) ? signalEnd : timeFilterEnd);
        Long timeDiffMsec = timeEnd-timeStart;
        Double numPoints = timeDiffMsec.doubleValue()*estim.getSamplingRate(timeStart, timeEnd);
        if(numPoints < maxNumPoints) {
            ret = true;
        }
        return ret;
    }

    public int getWindowLength(VizzlySignal signal, Long timeFilterStart, 
            Long timeFilterEnd, int maxNumPoints, CacheManager cache) throws VizzlyException {
        int multiple = 1;
        int windowLengthSec = 24*365*3600; // initialize with arbitrary chosen, high value
        Long signalStart = cache.getFirstPacketTimestamp(signal);
        Long signalEnd = cache.getLastPacketTimestamp(signal);
        Long timeStart = (timeFilterStart == null) ? signalStart : ((timeFilterStart < signalStart) ? signalStart : timeFilterStart);
        Long timeEnd = (timeFilterEnd == null) ? signalEnd : ((timeFilterEnd > signalEnd) ? signalEnd : timeFilterEnd);
        if(timeStart == null) {
            throw new VizzlyException("timeStart cannot be null at this point");
        }
        if(timeEnd == null) {
            throw new VizzlyException("timeEnd cannot be null at this point");
        }
        Long timeDiffMsec = timeEnd-timeStart;
        while(true) {
            windowLengthSec = MIN_WINDOW_LENGTH_SEC*multiple;
            double numWindows = timeDiffMsec.doubleValue()/((double)windowLengthSec*1000.0);
            if(numWindows <= maxNumPoints) {
                break;
            }
            multiple++;
        }
        return windowLengthSec;
    }

    public void updateSamplingRateEstimation(VizzlySignal signal, Vector<TimedLocationValue> values) {
        SamplingRateEstimation e = rateEstimators.get(signal);
        if(e == null) {
            e = new SamplingRateEstimation(values.firstElement().timestamp);
            rateEstimators.put(signal, e);
        }
        e.updateEstimation(values);
        if(useDatabase) {
            updateDatabase();
        }
    }

    public void deleteSignalEstimation(VizzlySignal signal) {
        rateEstimators.remove(signal);
        deleteFromDatabase(signal);
    }
    
    public Boolean isInitialized() {
        return isInitialized;
    }
    
    private void initDatabase() {
        try {
            InitialContext ctx = new InitialContext();
            // Perform JNDI lookup
            ds = (DataSource)ctx.lookup("VizzlyDS");

            Connection conn = ds.getConnection();
            Statement stmt = conn.createStatement();
            log.info("Create database table (if not existing): " + rateEstimatorTable);
            stmt.executeUpdate(rateEstimatorTableCreate);
            stmt.close();
            stmt = null;
            conn.close();
            conn = null;
        } catch(NamingException e) {
            log.error("Failed to connect to database server.", e);
        } catch(SQLException e) {
            log.error(e);
        }
    }
    
    private void loadFromDatabase() {
        try {
            Connection conn = ds.getConnection();
            Statement s = conn.createStatement();
            ResultSet rs = s.executeQuery("SELECT id, viz_signal, rate_estimation FROM " + 
                    rateEstimatorTable);

            while(rs.next()) {
                int id = rs.getInt("id");
                byte[] buf = rs.getBytes("viz_signal");
                ObjectInputStream objectIn = new ObjectInputStream(new ByteArrayInputStream(buf));
                VizzlySignal sig = (VizzlySignal)objectIn.readObject();
                buf = rs.getBytes("rate_estimation");
                objectIn = new ObjectInputStream(new ByteArrayInputStream(buf));
                SamplingRateEstimation e = (SamplingRateEstimation)objectIn.readObject();
                rateEstimators.put(sig, e);
                dbIdLookupTable.put(sig, id);
            }
            s.close();
            s = null;
            conn.close();
            conn = null;
        } catch(SQLException e) {
            log.error(e);
        } catch(IOException e) {
            log.error(e);
        } catch(ClassNotFoundException e) {
            log.error(e);
        }
    }
    
    private void updateDatabase() {
        synchronized(useDatabase) {
            try {
                
                if((System.currentTimeMillis()-lastDatabaseUpdate) < MIN_DB_UPDATE_WAIT) {
                    return;
                }
                Connection conn = ds.getConnection();
                conn.setAutoCommit(false);

                // Add not yet stored entries
                PreparedStatement pInsert = conn.prepareStatement("INSERT INTO " + rateEstimatorTable + 
                        " (viz_signal, rate_estimation) VALUES (?, ?)", Statement.RETURN_GENERATED_KEYS);
                PreparedStatement pUpdate = conn.prepareStatement("UPDATE " + rateEstimatorTable + 
                        " SET rate_estimation = ? WHERE id = ?");
                // This vector is required to map signals to auto-generated IDs later on
                Vector<VizzlySignal> addedEstimators = new Vector<VizzlySignal>();
                int batchSizeIns = 0, batchSizeUpd = 0;
                long now = System.currentTimeMillis();
                for(VizzlySignal sig : rateEstimators.keySet()) {
                    if(dbIdLookupTable.get(sig) == null) {
                        pInsert.setObject(1, sig);
                        pInsert.setObject(2, rateEstimators.get(sig));
                        pInsert.addBatch();
                        addedEstimators.add(sig);
                        batchSizeIns++;
                    } else {
                        SamplingRateEstimation e = rateEstimators.get(sig);
                        // Also limit the frequency in which single entries are updated
                        if((now-e.getLastSignificantUpdateTimestamp()) < MIN_DB_UPDATE_WAIT) {
                            continue;
                        }
                        pUpdate.setObject(1, e);
                        pUpdate.setInt(2, dbIdLookupTable.get(sig));
                        pUpdate.addBatch();
                        batchSizeUpd++;
                    }
                }
                if(batchSizeIns > 0) {
                    pInsert.executeBatch();
                    conn.commit();
                    // Retrieve newly generated IDs
                    ResultSet rs = pInsert.getGeneratedKeys();
                    int i = 0;
                    while(rs.next()) {
                        dbIdLookupTable.put(addedEstimators.get(i), rs.getInt(1));
                        i++;
                    }
                }
                pInsert.close();
                pInsert = null;
                if(batchSizeUpd > 0) {
                    pUpdate.executeBatch();
                    conn.commit();
                }
                pUpdate.close();
                pUpdate = null;
                conn.close();
                conn = null;
            } catch(SQLException e) {
                log.error(e);
            }
        }
    }
    
    private void deleteFromDatabase(VizzlySignal signal) {
        synchronized(useDatabase) {
            try {
                Connection conn = ds.getConnection();
                PreparedStatement p = conn.prepareStatement("DELETE FROM " + rateEstimatorTable + 
                        " WHERE id = ?");
                p.setInt(1, dbIdLookupTable.get(signal));
                p.executeQuery();
                p.close();
                p = null;
                conn.close();
                conn = null;
                dbIdLookupTable.remove(signal);
            } catch(SQLException e) {
                log.error(e);
            }
        }
    }
    
}
