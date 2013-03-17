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

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;

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

    private final String stateFile = "estimation.dat";

    private final String stateFileTemp = "estimation.dat.tmp";

    
    /**
     * Log.
     */
    private static Logger log = Logger.getLogger(AggregationLevelLookup.class);

    private Object estimationFileLock = new Object();

    /**
     * Singleton
     */
    private static AggregationLevelLookup instance = null;

    private ConcurrentHashMap<VizzlySignal,SamplingRateEstimation> rateEstimators = null;

    private AggregationLevelLookup() {
        rateEstimators = new ConcurrentHashMap<VizzlySignal,SamplingRateEstimation>();
        loadStateFromFile();
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
        writeStateToFile();
    }

    public void deleteSignalEstimation(VizzlySignal signal) {
        rateEstimators.remove(signal);
        writeStateToFile();
    }

    private void writeStateToFile() {
        FileOutputStream fos = null;
        ObjectOutputStream o = null;
        synchronized(estimationFileLock) {
            try {
                fos = new FileOutputStream(stateFileTemp); 
                o = new ObjectOutputStream(fos);
                o.writeObject(Integer.valueOf(rateEstimators.size()));
                for(VizzlySignal s : rateEstimators.keySet()) {
                    o.writeObject(s);
                    o.writeObject(rateEstimators.get(s));
                }
                o.close();
                o = null;
                fos.close();
                fos = null;
                File f = new File(stateFileTemp);
                f.renameTo(new File(stateFile));
            } catch(IOException e) {
                log.error(stateFile, e);
            } finally { 
                try { 
                    if(o != null) {
                        o.close();
                    }
                    if(fos != null) {
                        fos.close();
                    }
                } catch (Exception e) { 
                    log.error("Error in writeStateToFile: " + e);
                } 
            }
        }
    }

    private void loadStateFromFile() {
        FileInputStream fis = null;
        ObjectInputStream o = null;
        synchronized(estimationFileLock) {
            try {
                File f = new File(stateFile);
                if(!f.exists()) {
                    return;
                }
                fis = new FileInputStream(stateFile); 
                o = new ObjectInputStream(fis); 
                Integer numElements = (Integer)o.readObject();
                log.debug("Loading " + numElements + " sampling rate estimations from file");
                for(int i = 0; i < numElements; i++) {
                    VizzlySignal s = (VizzlySignal)o.readObject();
                    SamplingRateEstimation e = (SamplingRateEstimation)o.readObject();
                    rateEstimators.put(s, e);
                }
            } catch(IOException e) {
                log.error(e);
            } catch(ClassNotFoundException e) {
                log.error(e);
            } finally { 
                try {
                    if(o != null) {
                        o.close();
                    }
                    if(fis != null) {
                        fis.close();
                    }
                } catch (Exception e) { 
                    log.error("Error in loadStateFromFile: " + e);
                } 
            }
        }
    }

}
