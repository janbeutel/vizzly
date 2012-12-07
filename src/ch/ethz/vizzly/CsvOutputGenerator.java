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

import java.io.StringWriter;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Vector;

import org.apache.log4j.Logger;

import ch.ethz.vizzly.cache.CacheManager;
import ch.ethz.vizzly.datareader.AbstractDataReader;
import ch.ethz.vizzly.datareader.DataReaderRegistry;
import ch.ethz.vizzly.datatype.LocationValueAggregate;
import ch.ethz.vizzly.datatype.TimedLocationValue;
import ch.ethz.vizzly.datatype.VizzlyException;
import ch.ethz.vizzly.datatype.VizzlySignal;
import ch.ethz.vizzly.performance.AbstractPerformanceTracker;
import ch.ethz.vizzly.performance.DataFetchPerformanceMeasurement;
import ch.ethz.vizzly.performance.DataFetchPerformanceMeasurement.DataBackend;
import ch.ethz.vizzly.performance.UserRequestPerformanceMeasurement;
import ch.ethz.vizzly.util.LocationAggregationGrid;
import ch.ethz.vizzly.util.LocationFilter;

import com.ibm.icu.text.DecimalFormat;

/**
 * This class generates the CSV outputs that are then sent to a client.
 * @author Matthias Keller
 *
 */
public class CsvOutputGenerator {

    /**
     * Log.
     */
    private static Logger log = Logger.getLogger(CsvOutputGenerator.class);

    private static final int MAP_GRID_CELL_LENGTH_PIX = 35;

    public CsvOutputGenerator() {
    }

    /**
     * Method for generating a single CSV output by combining information of several cache contents. Called from
     * SensorVizDataSourceServlet when a user requests data.
     */
    public static String getTimedDataCSV(VizzlySignal[] signals, Long timeFilterStart, Long timeFilterEnd, Double latSW, 
            Double lngSW, Double latNE, Double lngNE, int signalIdx, boolean forceLoadUnaggregated, int canvasWidth, 
            UserRequestPerformanceMeasurement reqMeas, CacheManager cache, AbstractPerformanceTracker perfTracker, 
            DataReaderRegistry readerRegistry)
                    throws VizzlyException {
        if(!cache.isInitialized()) {
            throw new VizzlyException("Cache initialization is ongoing. Please wait.");
        }

        AggregationLevelLookup aggregationLookup = AggregationLevelLookup.getInstance();

        VizzlySignal[] displayedSignals = null;
        if(signalIdx == -1) {
            // Stand-alone time series display: Load all signals
            displayedSignals = signals;
        } else {
            // Time series display integrated into map display: Load only one signal
            displayedSignals = new VizzlySignal[1];
            displayedSignals[0] = signals[signalIdx];
        }

        Vector<Boolean> signalIsAvailable = new Vector<Boolean>();
        Vector<Boolean> valuesAreAggregated = new Vector<Boolean>();

        // First check if all data is already available in the cache
        for(VizzlySignal s : displayedSignals) {
            if(cache.isInCache(s)) {
                signalIsAvailable.add(true);
            } else {
                signalIsAvailable.add(false);
            }
        }
        if(!signalIsAvailable.contains(true)) {
            throw new VizzlyException("New signal requested. Please come back later.");
        }

        ArrayList<Vector<TimedLocationValue>> valuesList = new ArrayList<Vector<TimedLocationValue>>();

        SimpleDateFormat dateFormatter = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
        Calendar cal = Calendar.getInstance();

        // The user can enforce to get unaggregated data - check if the request is valid
        if(forceLoadUnaggregated) {
            // We support at most 12 hours of unaggregated data to be loaded without checking
            // the sampling rate
            // Designed for "now" button that displays the last 12 hours in a timeline plot
            if(timeFilterEnd-timeFilterStart > 12*3600*1000) {
                forceLoadUnaggregated = false;
            }
        }

        reqMeas.setDataFetchStart();
        // If cached, the aggregation window length is the same for all signals
        // The following code determined the largest, common window length
        int windowLengthSec = AggregationLevelLookup.MIN_WINDOW_LENGTH_SEC;
        if(!forceLoadUnaggregated) {
            for(int i = 0; i < displayedSignals.length; i++) {
                if(!signalIsAvailable.get(i)) {
                    continue;
                }
                VizzlySignal s = displayedSignals[i];
                if(!aggregationLookup.canLoadUnaggregatedData(s, timeFilterStart, timeFilterEnd, canvasWidth, cache)) {
                    int windowLengthThis = aggregationLookup.getWindowLength(s, timeFilterStart, timeFilterEnd, canvasWidth, cache);
                    if(windowLengthThis > windowLengthSec) {
                        windowLengthSec = windowLengthThis;
                    }
                }
            }
        }
        for(int i = 0; i < displayedSignals.length; i++) {
            if(!signalIsAvailable.get(i)) {
                valuesList.add(null);
                valuesAreAggregated.add(null);
                continue;
            }
            VizzlySignal s = displayedSignals[i];
            // Also do not try to load unaggregated data if the selection is out of bounds
            if(!forceLoadUnaggregated && (!aggregationLookup.canLoadUnaggregatedData(s, timeFilterStart, timeFilterEnd, canvasWidth, cache) 
                    || timeFilterStart == null || timeFilterEnd == null 
                    || timeFilterStart > cache.getLastPacketTimestamp(s) 
                    || timeFilterEnd < cache.getFirstPacketTimestamp(s))) {
                Boolean ignoreLocation = (latSW == null);
                Vector<TimedLocationValue> d = cache.getSignalData(s, windowLengthSec, timeFilterStart, timeFilterEnd, ignoreLocation);
                if(d != null) {
                    if(ignoreLocation) {
                        valuesList.add(d);
                        valuesAreAggregated.add(true);
                    } else {
                        Vector<TimedLocationValue> n = LocationFilter.filterAndAggregateByLocation(d, latSW, lngSW, latNE, lngNE);
                        valuesList.add(n);
                        valuesAreAggregated.add(true);
                    }
                } else {
                    log.error("Could not load data from cache - also it should be there");
                    signalIsAvailable.set(i, false);
                    valuesList.add(null);
                    valuesAreAggregated.add(null);
                }
            } else {
                // Get data directly from original datasource
                AbstractDataReader dr = readerRegistry.getDataReader(s.dataSource.type);
                // Set 100.000 as row limit for safety reasons, should be much less values
                long dataFetchStart = System.currentTimeMillis();
                reqMeas.setDataFetchStart();
                Vector<TimedLocationValue> vals = dr.getSignalData(s, timeFilterStart, timeFilterEnd, 100000);
                long dataFetchEnd = System.currentTimeMillis();
                if(vals != null && vals.size() > 0) {
                    if(latSW == null) {
                        valuesList.add(vals);
                    } else {
                        valuesList.add(LocationFilter.filterByLocation(vals, latSW, lngSW, latNE, lngNE));
                    }
                    valuesAreAggregated.add(false);
                    DataFetchPerformanceMeasurement ps = new DataFetchPerformanceMeasurement(dataFetchStart,  s.getUniqueIdentifier(), -1, DataBackend.DIRECTACCESS, 
                            1, vals.size(), dataFetchEnd-dataFetchStart);
                    perfTracker.addDataFetchMeasurement(ps);
                } else {
                    log.error("Could not load data from raw data source");
                    signalIsAvailable.set(i, false);
                    valuesList.add(null);
                    valuesAreAggregated.add(null);
                }

            }
        }
        reqMeas.setDataFetchEnd();

        if(valuesList.size() == 0) {
            throw new VizzlyException("No data found. Please change your selection, try again later, and contact us if the problem remains.");
        }

        // Write CSV header
        StringWriter csvOutput = new StringWriter();
        csvOutput.write("generation_time,");
        for(int i = 0; i < displayedSignals.length; i++) {
            csvOutput.write(displayedSignals[i].displayName);
            if(i < displayedSignals.length-1) {
                csvOutput.write(",");
            }
        }
        csvOutput.write("\n");

        // Add line with information on data time span
        long viewStartTime = -1, viewEndTime = -1;
        for(int i = 0; i < displayedSignals.length; i++) {
            if(!signalIsAvailable.get(i)) {
                continue;
            }
            VizzlySignal s = displayedSignals[i];
            Long firstTimestamp = cache.getFirstPacketTimestamp(s);
            Long lastTimestamp = cache.getLastPacketTimestamp(s);

            if(firstTimestamp != null) {
                viewStartTime = (viewStartTime != -1) ? Math.min(viewStartTime, firstTimestamp) : firstTimestamp;
            }

            if(lastTimestamp != null) {
                viewEndTime = Math.max(viewEndTime, lastTimestamp);
            }
        }
        //viewEndTime = (viewEndTime > cal.getTimeInMillis()) ? cal.getTimeInMillis() : viewEndTime;
        csvOutput.write("# " + Long.toString(viewStartTime) + ", " + Long.toString(viewEndTime) + "\n");

        DecimalFormat df = new DecimalFormat("#.###");

        int returnedLines = 0;
        // Output aggregated data first
        if(valuesAreAggregated.contains(true)) {
            long startTime = -1;
            long endTime = -1;

            // Find smallest start time, and largest end time - need common basis for output
            for(int i = 0; i < valuesList.size(); i++) {
                if(!signalIsAvailable.get(i)) {
                    continue;
                }
                if(!valuesAreAggregated.get(i)) {
                    continue;
                }
                if(valuesList.get(i).size() > 0) {
                    startTime = (startTime != -1) ? Math.min(startTime, valuesList.get(i).get(0).timestamp) :valuesList.get(i).get(0).timestamp;
                    endTime = Math.max(endTime, valuesList.get(i).get(valuesList.get(i).size()-1).timestamp);
                }
            }

            // Plot should not end in the future, but at the current date at most
            endTime = Math.min(endTime, cal.getTimeInMillis());

            // Remember the current position in each vector
            int vectorPos[] = new int[valuesList.size()];
            for(int i = 0; i < valuesList.size(); i++) {
                vectorPos[i] = 0;
            }

            for(long time = startTime; time <= endTime; time+=(windowLengthSec*1000)) {
                StringBuilder sb = new StringBuilder();
                Boolean hasData = false;
                cal.setTimeInMillis(time);
                sb.append(dateFormatter.format(cal.getTime())).append(",");
                for(int i = 0; i < valuesList.size(); i++) {
                    // Omit unaggregated data at this point
                    if(!signalIsAvailable.get(i) || !valuesAreAggregated.get(i)) {
                        if(i < valuesList.size()-1) {
                            sb.append(",");
                        }
                        continue;
                    }
                    Vector<TimedLocationValue> thisVec = valuesList.get(i);
                    // Omit empty values
                    if(vectorPos[i] == thisVec.size() || thisVec.get(vectorPos[i]).timestamp != time) {
                        if(i < valuesList.size()-1) {
                            sb.append(",");
                        }
                        continue;
                    }
                    hasData = true;
                    sb.append(df.format(thisVec.get(vectorPos[i]).value*displayedSignals[i].scaling));
                    vectorPos[i]++;
                    if(i < valuesList.size()-1) {
                        sb.append(",");
                    }
                }
                if(hasData) {
                    csvOutput.write(sb.append("\n").toString());
                    returnedLines++;
                }
            }
        }

        // Now output unaggregated data. Dygraph will re-order the overall CSV, if needed.
        if(valuesAreAggregated.contains(false)) {
            String commas = "";
            for(int i = 0; i < valuesList.size(); i++) {
                commas += ",";
            }
            for(int i = 0; i < valuesList.size(); i++) {
                if(!signalIsAvailable.get(i)) {
                    continue;
                }
                if(valuesAreAggregated.get(i)) {
                    continue;
                }
                Vector<TimedLocationValue> d = valuesList.get(i);
                for(int j = 0; j < d.size(); j++) {
                    TimedLocationValue v = d.get(j);
                    String commasFront = commas.substring(0, i+1); // substring ends at end-1
                    String commasBack = commas.substring(i+1, commas.length());
                    StringBuilder sb = new StringBuilder();
                    cal.setTimeInMillis(v.timestamp);
                    sb.append(dateFormatter.format(cal.getTime()));
                    sb.append(commasFront).append(df.format(v.value*displayedSignals[i].scaling)).append(commasBack);
                    csvOutput.write(sb.append("\n").toString());
                    returnedLines++;
                }
            }
        }
        reqMeas.setNumReturnedLines(returnedLines);
        reqMeas.setNumRequestedSignals(valuesList.size());

        return csvOutput.toString();
    }

    public static String getAggregationMapCSV(VizzlySignal[] signals, Long timeFilterStart, Long timeFilterEnd, Double latSW, Double lngSW, 
            Double latNE, Double lngNE, int signalIdx, int canvasWidth, int canvasHeight, UserRequestPerformanceMeasurement reqMeas,
            CacheManager cache, AbstractPerformanceTracker perfTracker, DataReaderRegistry readerRegistry)
                    throws VizzlyException {
        if(!cache.isInitialized()) {
            throw new VizzlyException("Cache initialization is ongoing. Please wait.");
        }

        StringWriter outWriter = new StringWriter();
        VizzlySignal s = signals[signalIdx];
        AggregationLevelLookup aggregationLookup = AggregationLevelLookup.getInstance();

        if(!cache.isInCache(s)) {
            throw new VizzlyException("New signal requested. Please come back later.");
        }

        // Setup map grid for aggregation
        int numRows = new Double(Math.floor((double)canvasHeight/(double)MAP_GRID_CELL_LENGTH_PIX)).intValue();
        int numCols = new Double(Math.floor((double)canvasWidth/(double)MAP_GRID_CELL_LENGTH_PIX)).intValue();
        LocationAggregationGrid grid = new LocationAggregationGrid(latSW, lngSW, latNE, lngNE, numRows, numCols);

        if(!aggregationLookup.canLoadUnaggregatedData(s, timeFilterStart, timeFilterEnd, canvasWidth, cache)) {
            // Get data from cache
            int windowLengthSec = aggregationLookup.getWindowLength(s, timeFilterStart, timeFilterEnd, canvasWidth, cache);
            reqMeas.setDataFetchStart();
            Vector<TimedLocationValue> d = cache.getSignalData(s, windowLengthSec, timeFilterStart, timeFilterEnd, false);
            reqMeas.setDataFetchEnd();
            if(d != null) {
                for(TimedLocationValue v : d) {
                    grid.addValue(v);
                }
            } else {
                throw new VizzlyException("Requested signal is not available. Please try again later and contact us if the problem remains.");
            }
        } else {
            // Get data directly from original datasource
            AbstractDataReader dr = readerRegistry.getDataReader(s.dataSource.type);
            // Set 100.000 as row limit for safety reasons, should be much less values
            long dataFetchStart = System.currentTimeMillis();
            reqMeas.setDataFetchStart();
            Vector<TimedLocationValue> vals = dr.getSignalData(s, timeFilterStart, timeFilterEnd, 100000);
            reqMeas.setDataFetchEnd();
            long dataFetchEnd = System.currentTimeMillis();
            for(int i=0; i<vals.size(); i++) {
                grid.addValue(vals.get(i));
            }
            if(vals.size() > 0) {
                DataFetchPerformanceMeasurement ps = new DataFetchPerformanceMeasurement(dataFetchStart, s.getUniqueIdentifier(), 
                        -1, DataBackend.DIRECTACCESS, 1, vals.size(), dataFetchEnd-dataFetchStart);
                perfTracker.addDataFetchMeasurement(ps);
            }
        }

        // Output time bounds
        Long firstTimestamp = cache.getFirstPacketTimestamp(s);
        Long lastTimestamp = cache.getLastPacketTimestamp(s);
        outWriter.write("# " + Long.toString(firstTimestamp) + ", " + Long.toString(lastTimestamp) + "\n");

        int returnedLines = 0;
        DecimalFormat df = new DecimalFormat("#.#");
        LocationValueAggregate[] aggMap = grid.getAggregatedData();
        for(int i = 0; i < aggMap.length; i++) {
            if(aggMap[i] == null) {
                continue;
            }
            outWriter.write(aggMap[i].getLocation().latitude + "," + aggMap[i].getLocation().longitude + "," 
                    + df.format(aggMap[i].getAggregatedValue()*s.scaling) + "\n");
            returnedLines++;
        }

        reqMeas.setNumReturnedLines(returnedLines);
        reqMeas.setNumRequestedSignals(1);

        return outWriter.toString();
    }

}
