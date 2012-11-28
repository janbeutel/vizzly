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

package ch.ethz.vizzly.cache.sqldb;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;

import javax.naming.InitialContext;
import javax.naming.NamingException;
import javax.sql.DataSource;

import org.apache.log4j.Logger;

import ch.ethz.vizzly.cache.AbstractCache;
import ch.ethz.vizzly.datatype.CachedDataInfo;
import ch.ethz.vizzly.datatype.TimedLocationValue;
import ch.ethz.vizzly.datatype.TimedValue;
import ch.ethz.vizzly.datatype.VizzlySignal;
import ch.ethz.vizzly.performance.DataFetchPerformanceMeasurement.DataBackend;
import ch.ethz.vizzly.util.DataAggregationUtil;
import ch.ethz.vizzly.util.TimestampTruncateUtil;

/**
 * This class implements a cache that stores all data in a SQL database.
 * For faster access, meta data is also hold in memory.
 * @author Matthias Keller
 *
 */
public class SqlDbCache extends AbstractCache {

    /**
     * Log.
     */
    private static Logger log = Logger.getLogger(SqlDbCache.class);

    private DataSource ds = null;

    private Calendar cal = null;

    // For each signal, there is a map that translates the average interval to the cache entry_id
    private ConcurrentHashMap<VizzlySignal, HashMap<Integer, Integer>> cacheIdLookup = null;

    // For being faster, we keep certain (small) information in memory
    private Vector<VizzlySignal> seenSignals = null;
    private ConcurrentHashMap<VizzlySignal, Integer> seenSignalsEntryIds = null;
    private ConcurrentHashMap<Integer, SqlDbCacheMetaEntry> cacheMeta = null;

    private Integer nextCacheEntryId = 1;
    private int nextSignalId = 1;

    final private String description = "SqlDbCache";

    final private String tablePrefix = "viz_";

    final private String signalsDbTable = tablePrefix + "signals";

    final private String signalsDbTableCreate = "CREATE TABLE IF NOT EXISTS " + signalsDbTable + " (" +
            "`signal_id` int(11) NOT NULL AUTO_INCREMENT," +
            "`ds_type` varchar(30) NOT NULL," +
            "`ds_name` varchar(255) NOT NULL," +
            "`ds_server` varchar(255) DEFAULT NULL," +
            "`data_field` varchar(30) NOT NULL," +
            "`sel_type` varchar(10) NOT NULL," +
            "`sel_field` varchar(30) DEFAULT NULL," +
            "`sel_value` varchar(30) DEFAULT NULL," +
            "`time_field` varchar(30) NOT NULL," +
            "`location_lat_field` varchar(30) DEFAULT NULL," +
            "`location_lng_field` varchar(30) DEFAULT NULL," +
            "`agg_function` varchar(10) DEFAULT NULL," +
            "PRIMARY KEY (`signal_id`)" +
            ") ENGINE=InnoDB DEFAULT CHARSET=latin1;";

    final private String cacheMetaDataTable = tablePrefix + "metadata";

    final private String cacheMetaDataTableCreate = "CREATE TABLE IF NOT EXISTS " + cacheMetaDataTable + " (" +
            "`entry_id` int(11) NOT NULL AUTO_INCREMENT," +
            "`signal_id` int(11) NOT NULL," +
            "`window_length` int(11) NOT NULL," +
            "`has_location_data` tinyint(1) NOT NULL," +
            "`start_time` bigint(20) DEFAULT NULL," +
            "`end_time` bigint(20) DEFAULT NULL," +
            "`first_packet_timestamp` bigint(20) DEFAULT NULL," +
            "`last_packet_timestamp` bigint(20) DEFAULT NULL," +
            "`last_update` bigint(20) DEFAULT NULL," +
            "`num_elements` int(11) DEFAULT 0," +
            "`hits` int(11) DEFAULT 0," +
            "PRIMARY KEY (`entry_id`)" +
            ") ENGINE=InnoDB DEFAULT CHARSET=latin1;";

    public SqlDbCache() {
        seenSignals = new Vector<VizzlySignal>();
        seenSignalsEntryIds = new ConcurrentHashMap<VizzlySignal, Integer>();
        cacheMeta = new ConcurrentHashMap<Integer, SqlDbCacheMetaEntry>();
        cacheIdLookup = new ConcurrentHashMap<VizzlySignal, HashMap<Integer, Integer>>();
        dataBackend = DataBackend.MYSQLDBCACHE;

        try {

            InitialContext ctx = new InitialContext();
            // Perform JNDI lookup - database connection is configured in contexts/sensorviz.xml
            ds = (DataSource)ctx.lookup("java:comp/env/jdbc/VizzlyDS");

            // Create tables for managing structure
            sqlExecuteSimpleQuery(signalsDbTableCreate);
            log.info("Created new database table: " + signalsDbTable);
            sqlExecuteSimpleQuery(cacheMetaDataTableCreate);
            log.info("Created new database table: " + cacheMetaDataTable);

            // Load previous state from DB
            initShadowedDataFromDb();

            isInitialized = true;

        } catch(NamingException e) {
            log.error("Failed to connect to database server.", e);
        } catch(SQLException e) {
            log.error(e);
        }
    }

    public String getCacheDescription() {
        return description;
    }

    private void initShadowedDataFromDb() throws SQLException {
        Connection conn = ds.getConnection();
        Statement s = conn.createStatement();
        ResultSet rs = s.executeQuery("SELECT signal_id, ds_type, ds_name, ds_server, data_field, sel_type," +
                "sel_field, sel_value, time_field, location_lat_field, location_lng_field, agg_function FROM " + 
                signalsDbTable + " ORDER BY signal_id ASC");

        HashMap<Integer,VizzlySignal> signalIdToSignal = new HashMap<Integer,VizzlySignal>();
        while(rs.next()) {
            VizzlySignal sig = new VizzlySignal();
            int signalId = rs.getInt("signal_id");

            sig.dataSource = new VizzlySignal.DataSource(rs.getString("ds_type"), rs.getString("ds_name"), rs.getString("ds_server"));
            sig.dataField = rs.getString("data_field");
            sig.deviceSelect = new VizzlySignal.DeviceSelect(rs.getString("sel_type"), rs.getString("sel_field"), rs.getString("sel_value"));
            sig.timeField = rs.getString("time_field");
            sig.locationLatField = rs.getString("location_lat_field");
            sig.locationLngField = rs.getString("location_lng_field");
            sig.aggFunction = rs.getString("agg_function");
            synchronized(seenSignals) {
                seenSignals.add(sig);
            }
            seenSignalsEntryIds.put(sig, signalId);
            signalIdToSignal.put(signalId, sig);
            nextSignalId = signalId+1;
        }
        s.close();

        s = conn.createStatement();
        rs = s.executeQuery("SELECT entry_id, signal_id, window_length, has_location_data, start_time, end_time, " +
                "first_packet_timestamp, last_packet_timestamp, last_update, num_elements, hits FROM " + 
                cacheMetaDataTable + " ORDER BY entry_id ASC");
        while(rs.next()) {
            int entryId = rs.getInt("entry_id");
            int signalId = rs.getInt("signal_id");
            VizzlySignal sig = signalIdToSignal.get(signalId);
            if(sig == null) {
                log.error("DB inconsistency, signal_id " + signalId + " not found in signals table.");
                continue;
            }
            SqlDbCacheMetaEntry e = new SqlDbCacheMetaEntry();
            e.signal = sig;
            e.windowLengthSec =  rs.getInt("window_length");
            e.hasLocationData = (rs.getInt("has_location_data") == 1) ? true : false;
            e.startTime = rs.getLong("start_time");
            e.endTime = rs.getLong("end_time");
            e.firstPacketTimestamp = rs.getLong("first_packet_timestamp");
            e.lastPacketTimestamp = rs.getLong("last_packet_timestamp");
            e.hits = rs.getInt("hits");
            cal = Calendar.getInstance();
            cal.setTimeInMillis(rs.getLong("last_update"));
            e.lastUpdate = cal.getTime();
            e.numElements = rs.getInt("num_elements");
            cacheMeta.put(entryId, e);
            HashMap<Integer, Integer> lookupTable = cacheIdLookup.get(sig);
            if(lookupTable == null) {
                lookupTable = new HashMap<Integer, Integer>();
                cacheIdLookup.put(sig, lookupTable);
            }
            lookupTable.put(e.windowLengthSec, entryId);

            nextCacheEntryId = entryId+1;
        }
        s.close();
        s = null;
        conn.close();
        conn = null;
    }

    public void updateCacheEntry(VizzlySignal signal, int windowLengthSec,
            Vector<TimedLocationValue> r) {
        String tableName = "";
        if(!isInitialized) {
            return;
        }
        if(r.size() == 0) {
            return;
        }
        SqlDbCacheMetaEntry e = null;
        if(!isInCacheIgnoreData(signal, windowLengthSec)) {
            try {
                int nextEntryId = 0;
                synchronized(nextCacheEntryId) {
                    nextEntryId = nextCacheEntryId;
                    nextCacheEntryId++;
                }

                Boolean hasLocationData = signal.hasLocation();
                String locationColumns = "";
                if(hasLocationData) {
                    locationColumns = "`location_lat` double DEFAULT NULL," +
                            "`location_lng` double DEFAULT NULL,";
                }

                // Create not yet existing table
                tableName = tablePrefix + Integer.valueOf(nextEntryId).toString();
                String sql = "CREATE TABLE " + tableName + " (" +
                        "`id` MEDIUMINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY," +
                        "`timeIdx` MEDIUMINT UNSIGNED NOT NULL," +
                        "`value` double NOT NULL," +
                        locationColumns +
                        "KEY `timeIdx` (`timeIdx`)" +
                        ") ENGINE=InnoDB DEFAULT CHARSET=latin1;";
                sqlExecuteSimpleQuery(sql);
                log.info("Created new database table: " + tableName);

                // r is ordered by ASCENDING time
                Long startTime = TimestampTruncateUtil.truncate(r.firstElement().timestamp, windowLengthSec*1000);
                Long firstPacketTimestamp = r.firstElement().timestamp;
                Long lastPacketTimestamp = r.lastElement().timestamp;

                Connection conn = ds.getConnection();
                PreparedStatement p = conn.prepareStatement("INSERT INTO " + cacheMetaDataTable + 
                        " (entry_id, signal_id, window_length, has_location_data, start_time, " +
                        "first_packet_timestamp) VALUES (?, ?, ?, ?, ?, ?)");
                p.setInt(1, nextEntryId);
                p.setInt(2, seenSignalsEntryIds.get(signal));
                p.setInt(3, windowLengthSec);
                if(hasLocationData) {
                    p.setInt(4, 1);
                } else {
                    p.setInt(4, 0);
                }
                p.setLong(5, startTime);
                p.setLong(6, firstPacketTimestamp);
                p.executeUpdate();
                p.close();
                p = null;
                conn.close();
                conn = null;

                HashMap<Integer, Integer> lookup = cacheIdLookup.get(signal);
                if(lookup == null) {
                    lookup = new HashMap<Integer, Integer>();
                    cacheIdLookup.put(signal, lookup);
                }
                lookup.put(windowLengthSec, nextEntryId);
                e = new SqlDbCacheMetaEntry();
                e.signal = signal;
                e.windowLengthSec = windowLengthSec;
                e.startTime = startTime;
                e.firstPacketTimestamp = firstPacketTimestamp;
                e.lastPacketTimestamp = lastPacketTimestamp;
                e.hasLocationData = hasLocationData;
                cacheMeta.put(nextEntryId, e);
            } catch(SQLException ex) {
                log.error(ex);
                return;
            }
        } else {
            Integer cacheEntryId = getCacheEntryId(signal, windowLengthSec);
            tableName = tablePrefix + cacheEntryId;
            e = cacheMeta.get(cacheEntryId);
        }

        if(!e.hasLocationData) {
            aggregateAndUpdate(signal, windowLengthSec, tableName, r);
        } else {
            aggregateAndUpdateWithLocation(signal, windowLengthSec, tableName, r);
        }
    }

    private void sqlExecuteSimpleQuery(String sql) throws SQLException {
        Connection conn = ds.getConnection();
        Statement stmt = conn.createStatement();
        stmt.executeUpdate(sql);
        stmt.close();
        stmt = null;
        conn.close();
        conn = null;
    }

    public Vector<CachedDataInfo> getCachedDataInfo() {
        Vector<CachedDataInfo> ret = new Vector<CachedDataInfo>();
        for(SqlDbCacheMetaEntry e : cacheMeta.values()) {
            Date lastPacketTimestamp = null;
            if(e.lastPacketTimestamp != null) {
                cal = Calendar.getInstance();
                cal.setTimeInMillis(e.lastPacketTimestamp);
                lastPacketTimestamp = cal.getTime();    
            }
            CachedDataInfo i = new CachedDataInfo(e.signal, e.windowLengthSec, e.numElements, 
                    e.hasLocationData, e.lastUpdate, lastPacketTimestamp, e.hits);
            ret.add(i);
        }
        return ret;
    }

    @SuppressWarnings("unchecked")
    public Vector<VizzlySignal> getSignals() {
        return (Vector<VizzlySignal>)(seenSignals.clone());
    }

    public Vector<TimedLocationValue> getSignalData(VizzlySignal signal,
            int windowLengthSec, Long timeFilterStart, Long timeFilterEnd,
            Boolean updateStats) {
        if(!isInitialized) {
            return null;
        }
        if(updateStats) {
            cacheRequests++;
        }
        if(isInCache(signal, windowLengthSec)) {
            if(updateStats) {
                cacheHits++;
            }
            Integer cacheEntryId = getCacheEntryId(signal, windowLengthSec);
            String tableName = tablePrefix + cacheEntryId;
            Boolean hasLocationData = cacheMeta.get(cacheEntryId).hasLocationData;

            String locationCols = (hasLocationData) ? ", location_lat, location_lng" : "";

            PreparedStatement p = null;
            try {
                Connection conn = ds.getConnection();
                if(timeFilterStart != null && timeFilterEnd == null) {
                    int timeIdxStart = getTimeIdx(timeFilterStart, cacheMeta.get(cacheEntryId).startTime, 
                            cacheMeta.get(cacheEntryId).windowLengthSec);
                    p = conn.prepareStatement("SELECT timeIdx, value" + locationCols +
                            " FROM " + tableName + " WHERE timeIdx >= ? ORDER BY timeIdx ASC");
                    p.setInt(1, timeIdxStart);
                } else if(timeFilterStart == null && timeFilterEnd != null) {
                    int timeIdxEnd = getTimeIdx(timeFilterEnd, cacheMeta.get(cacheEntryId).startTime, 
                            cacheMeta.get(cacheEntryId).windowLengthSec);
                    p = conn.prepareStatement("SELECT timeIdx, value" + locationCols +
                            " FROM " + tableName + " WHERE timeIdx <= ? ORDER BY timeIdx ASC");
                    p.setInt(1, timeIdxEnd);
                } else if(timeFilterStart != null && timeFilterEnd != null) {
                    int timeIdxStart = getTimeIdx(timeFilterStart, cacheMeta.get(cacheEntryId).startTime, 
                            cacheMeta.get(cacheEntryId).windowLengthSec);
                    int timeIdxEnd = getTimeIdx(timeFilterEnd, cacheMeta.get(cacheEntryId).startTime, 
                            cacheMeta.get(cacheEntryId).windowLengthSec);
                    p = conn.prepareStatement("SELECT timeIdx, value" + locationCols +
                            " FROM " + tableName + " WHERE timeIdx >= ? AND timeIdx <= ? ORDER BY timeIdx ASC");
                    p.setInt(1, timeIdxStart);
                    p.setInt(2, timeIdxEnd);
                } else {
                    p = conn.prepareStatement("SELECT timeIdx, value" + locationCols +
                            " FROM " + tableName + " ORDER BY timeIdx ASC");
                }

                Vector<TimedLocationValue> ret = new Vector<TimedLocationValue>();
                ResultSet rs = p.executeQuery();
                SqlDbCacheMetaEntry e = cacheMeta.get(cacheEntryId);
                while(rs.next()) {
                    int timeIdx = rs.getInt(1);
                    double value = rs.getDouble(2);
                    long timestamp = getTimestamp(timeIdx, e.startTime, e.windowLengthSec);
                    TimedLocationValue v = null;
                    if(!hasLocationData) {
                        v = new TimedLocationValue(timestamp, value);
                    } else {
                        double locationLat = rs.getDouble(3);
                        double locationLng = rs.getDouble(4);
                        v = new TimedLocationValue(timestamp, value, locationLat, locationLng);
                    }
                    ret.add(v);
                }
                p.close();
                p = null;
                conn.close();
                conn = null;

                if(updateStats) {
                    cacheMeta.get(cacheEntryId).hits++;
                }

                return ret;
            } catch(SQLException e) {
                log.error(e);
            }
        }
        if(updateStats) {
            cacheMisses++;
        }
        return null;
    }

    public Boolean isInCache(VizzlySignal signal, int windowLengthSec) {
        HashMap<Integer, Integer> lookupMap = cacheIdLookup.get(signal);
        if(lookupMap != null) {
            Integer entryId = lookupMap.get(windowLengthSec);
            if(entryId != null) {
                if(cacheMeta.get(entryId) != null && cacheMeta.get(entryId).lastUpdate != null) {
                    return true;
                }
            }
        }
        return false;
    }
    
    private Boolean isInCacheIgnoreData(VizzlySignal signal, int windowLengthSec) {
        HashMap<Integer, Integer> lookupMap = cacheIdLookup.get(signal);
        if(lookupMap != null) {
            Integer entryId = lookupMap.get(windowLengthSec);
            if(entryId != null) {
                return true;
            }
        }
        return false;
    }

    private int getCacheEntryId(VizzlySignal signal, int windowLengthSec) {
        HashMap<Integer, Integer> lookupMap = cacheIdLookup.get(signal);
        if(lookupMap != null) {
            Integer entryId = lookupMap.get(windowLengthSec);
            if(entryId != null) {
                return entryId;
            }
        }
        return -1;
    }

    public Long getStartTime(VizzlySignal signal, int windowLengthSec) {
        int entryId = getCacheEntryId(signal, windowLengthSec);
        if(entryId == -1) {
            return null;
        }
        return cacheMeta.get(entryId).startTime;
    }

    public Long getEndTime(VizzlySignal signal, int windowLengthSec) {
        int entryId = getCacheEntryId(signal, windowLengthSec);
        if(entryId == -1) {
            return null;
        }
        return cacheMeta.get(entryId).endTime;
    }

    public Long getFirstPacketTimestamp(VizzlySignal signal, int windowLengthSec) {
        int entryId = getCacheEntryId(signal, windowLengthSec);
        if(entryId == -1) {
            return null;
        }
        return cacheMeta.get(entryId).firstPacketTimestamp;
    }

    public Long getLastPacketTimestamp(VizzlySignal signal, int windowLengthSec) {
        int entryId = getCacheEntryId(signal, windowLengthSec);
        if(entryId == -1) {
            return null;
        }
        return cacheMeta.get(entryId).lastPacketTimestamp;
    }

    public Date getLastUpdate(VizzlySignal signal, int windowLengthSec) {
        int entryId = getCacheEntryId(signal, windowLengthSec);
        if(entryId == -1) {
            return null;
        }
        return cacheMeta.get(entryId).lastUpdate;
    }

    public void addSignal(VizzlySignal signal) {
        synchronized(seenSignals) {
            if(seenSignals.contains(signal)) {
                return;
            }
            try {
                Connection conn = ds.getConnection();
                conn.setAutoCommit(false);
                PreparedStatement p = conn.prepareStatement("INSERT INTO " + signalsDbTable + " (signal_id, ds_type, " +
                        "ds_name, ds_server, data_field, sel_type, sel_field, sel_value, time_field, location_lat_field, " +
                        "location_lng_field, agg_function) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");
                p.setInt(1, nextSignalId);
                p.setString(2, signal.dataSource.type);
                p.setString(3, signal.dataSource.name);
                p.setString(4, signal.dataSource.serverAddress);
                p.setString(5, signal.dataField);
                p.setString(6, signal.deviceSelect.type);
                p.setString(7, signal.deviceSelect.field);
                p.setString(8, signal.deviceSelect.value);
                p.setString(9, signal.timeField);
                p.setString(10, signal.locationLatField);
                p.setString(11, signal.locationLngField);
                p.setString(12, signal.aggFunction);
                p.executeUpdate();
                conn.commit();
                p.close();
                p = null;
                conn.close();
                conn = null;
                seenSignals.add(signal);
                seenSignalsEntryIds.put(signal, nextSignalId);
                nextSignalId++;
            } catch(SQLException e) {
                log.error(e);
            }
        }
    }

    public Boolean removeSignal(VizzlySignal signal) {
        if(!isInitialized) {
            return false;
        }
        try {
            // Delete meta data in memory first, data in database afterwards
            HashMap<Integer, Integer> lookupMap = null;
            int signalId = 0;
            synchronized(seenSignals) {
                seenSignals.remove(signal);
            }
            signalId = seenSignalsEntryIds.get(signal);
            seenSignalsEntryIds.remove(signal);

            lookupMap = cacheIdLookup.get(signal);
            if(lookupMap != null) {
                for(Integer i : lookupMap.values()) {
                    cacheMeta.remove(i);
                }
                cacheIdLookup.remove(signal);
            }

            Connection conn = ds.getConnection();
            PreparedStatement p = conn.prepareStatement("DELETE FROM " + cacheMetaDataTable + " WHERE signal_id = ?");
            p.setInt(1, signalId);
            p.executeUpdate();
            p.close();
            p = null;

            if(lookupMap != null) {
                Statement s = null;
                for(Integer i : lookupMap.values()) {
                    s = conn.createStatement();
                    s.executeUpdate("DROP TABLE " + tablePrefix + i);
                    s.close();
                    log.info("Removed database table: " + tablePrefix + i);
                }
                s = null;
            }

            conn.close();
            conn = null;
        } catch(SQLException e) {
            log.error(e);
        }
        return true;
    }

    public long getCacheSize() {
        long total = 0;
        for(SqlDbCacheMetaEntry e : cacheMeta.values()) {
            if(e.hasLocationData) {
                total += e.numElements * 24;
            } else {
                total += e.numElements * 8;
            }
        }
        return total;
    }

    public int getNumberOfSeenSignals() {
        return seenSignals.size();
    }

    public int getNumberOfCacheEntries() {
        return cacheMeta.size();
    }

    private void aggregateAndUpdate(VizzlySignal signal, int windowLengthSec, String tableName, Vector<TimedLocationValue> data) {
        if(data.size() == 0) {
            return;
        }
        try {
            Integer cacheEntryId = getCacheEntryId(signal, windowLengthSec);
            if(cacheEntryId == -1) {
                log.error("DB inconsistency, no cache entry found.");
                return;
            }
            SqlDbCacheMetaEntry e = cacheMeta.get(cacheEntryId); 

            // First step: Pre-aggregate new data
            Vector<TimedValue> aggregatedData = DataAggregationUtil.aggregateData(data, windowLengthSec);

            if(aggregatedData.size() == 0) {
                log.debug("Empty aggregated data.");
                return;
            }

            // Second step: Remove old, overlapping data
            Connection conn = ds.getConnection();
            conn.setAutoCommit(false);
            int timeIdx = getTimeIdx(aggregatedData.get(0).timestamp, e.startTime, e.windowLengthSec);
            PreparedStatement p = conn.prepareStatement("DELETE FROM " + tableName + " WHERE timeIdx >= ?");
            p.setInt(1, timeIdx);
            p.executeUpdate();
            conn.commit();
            p.close();

            p = conn.prepareStatement("INSERT INTO " + tableName + " (timeIdx, value) VALUES (?, ?)");

            // Third step: Add new data
            for(int i = 0; i < aggregatedData.size(); i++) {
                TimedValue v = aggregatedData.get(i);
                timeIdx = getTimeIdx(v.timestamp, e.startTime, e.windowLengthSec);
                p.setInt(1, timeIdx);
                p.setDouble(2, v.value);
                p.addBatch();
            }

            p.executeBatch();
            conn.commit();
            p.close();
            p = null;
            conn.close();
            conn = null;

            cacheMeta.get(cacheEntryId).lastPacketTimestamp = data.get(data.size()-1).timestamp;
            cacheMeta.get(cacheEntryId).endTime = TimestampTruncateUtil.truncate(data.get(data.size()-1).timestamp, windowLengthSec*1000);
            cal = Calendar.getInstance();
            cacheMeta.get(cacheEntryId).lastUpdate = cal.getTime();
            updateNumElements(signal, windowLengthSec);
            updateCacheEntryDb(signal, windowLengthSec);
        } catch(SQLException e) {
            log.error(e);
        }

    }

    private void aggregateAndUpdateWithLocation(VizzlySignal signal, int windowLengthSec, String tableName, Vector<TimedLocationValue> data) {
        if(data.size() == 0) {
            return;
        }
        try {
            Integer cacheEntryId = getCacheEntryId(signal, windowLengthSec);
            if(cacheEntryId == -1) {
                log.error("DB inconsistency, no cache entry found.");
                return;
            }
            SqlDbCacheMetaEntry e = cacheMeta.get(cacheEntryId); 

            // First step: Pre-aggregate new data
            Vector<TimedLocationValue> aggregatedData = DataAggregationUtil.aggregateDataWithLocation(data, windowLengthSec);

            if(aggregatedData.size() == 0) {
                log.debug("Empty aggregated data.");
                return;
            }

            // Second step: Clean-up previously filled, overlapping data
            Connection conn = ds.getConnection();
            conn.setAutoCommit(false);
            int timeIdx = getTimeIdx(aggregatedData.get(0).timestamp, e.startTime, e.windowLengthSec);
            PreparedStatement p = conn.prepareStatement("DELETE FROM " + tableName + " WHERE timeIdx >= ?");
            p.setInt(1, timeIdx);
            p.executeUpdate();
            conn.commit();
            p.close();

            p = conn.prepareStatement("INSERT INTO " + tableName + " (timeIdx, value, " +
                    "location_lat, location_lng) VALUES (?, ?, ?, ?)");

            // Third step: Add new data
            for(int i = 0; i < aggregatedData.size(); i++) {
                TimedLocationValue v = aggregatedData.get(i);
                timeIdx = getTimeIdx(v.timestamp, e.startTime, e.windowLengthSec);
                p.setInt(1, timeIdx);
                p.setDouble(2, v.value);
                p.setDouble(3, v.location.latitude);
                p.setDouble(4, v.location.longitude);
                p.addBatch();
            }

            p.executeBatch();
            conn.commit();
            p.close();
            p = null;
            conn.close();
            conn = null;

            cacheMeta.get(cacheEntryId).lastPacketTimestamp = data.get(data.size()-1).timestamp;
            cacheMeta.get(cacheEntryId).endTime = TimestampTruncateUtil.truncate(data.get(data.size()-1).timestamp, windowLengthSec*1000);
            cal = Calendar.getInstance();
            cacheMeta.get(cacheEntryId).lastUpdate = cal.getTime();
            updateNumElements(signal, windowLengthSec);
            updateCacheEntryDb(signal, windowLengthSec);
        } catch(SQLException e) {
            log.error(e);
        }
    }

    private void updateCacheEntryDb(VizzlySignal signal, int windowLengthSec) throws SQLException {
        Connection conn = ds.getConnection();
        PreparedStatement p = conn.prepareStatement("UPDATE " + cacheMetaDataTable + 
                " SET end_time = ?, last_packet_timestamp = ?, last_update = ?, num_elements = ?, hits = ? WHERE entry_id = ?");
        int cacheEntryId = getCacheEntryId(signal, windowLengthSec);
        SqlDbCacheMetaEntry e = cacheMeta.get(cacheEntryId);
        p.setLong(1, e.endTime);
        p.setLong(2, e.lastPacketTimestamp);
        p.setLong(3, e.lastUpdate.getTime());
        p.setInt(4, e.numElements);
        p.setInt(5, e.hits);
        p.setInt(6, cacheEntryId);
        p.executeUpdate();
        p.close();
        p = null;
        conn.close();
        conn = null;
    }

    private void updateNumElements(VizzlySignal signal, int windowLengthSec) throws SQLException {
        int cacheEntryId = getCacheEntryId(signal, windowLengthSec);
        Connection conn = ds.getConnection();
        Statement s = conn.createStatement();
        String tableName = tablePrefix + cacheEntryId;
        ResultSet rs = s.executeQuery("SELECT COUNT(*) FROM " + tableName + " WHERE id > 0");
        if(rs.next()) {
            cacheMeta.get(cacheEntryId).numElements = rs.getInt(1);
        }
        s.close();
        s = null;
        conn.close();
        conn = null;
    }

    private int getTimeIdx(long timestamp, long startTime, int windowLengthSec) {
        long windowLengthSecMilli = (long)windowLengthSec*1000;
        long diff = timestamp-startTime;
        double t = Long.valueOf(diff).doubleValue();
        double i = Long.valueOf(windowLengthSecMilli).doubleValue();
        Double idx = Math.floor(t/i);
        return idx.intValue();
    }

    private long getTimestamp(int timeIdx, long startTime, int windowLengthSec) {
        long windowLengthMilli = (long)windowLengthSec*1000;
        long timestamp = startTime+(long)timeIdx*windowLengthMilli;
        return timestamp;
    }

}
