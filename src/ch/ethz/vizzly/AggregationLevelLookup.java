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
import java.util.HashMap;
import java.util.Vector;
import java.util.concurrent.Semaphore;

import org.apache.log4j.Logger;

import ch.ethz.vizzly.cache.CacheManager;
import ch.ethz.vizzly.datatype.VizzlyException;
import ch.ethz.vizzly.datatype.TimedLocationValue;
import ch.ethz.vizzly.datatype.VizzlySignal;

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

    /**
     * Log.
     */
    private static Logger log = Logger.getLogger(AggregationLevelLookup.class);

    /**
     * Used to protect the map of rate estimators.
     */
    private final Semaphore writeAccess = new Semaphore(1);
    
    /**
     * Singleton
     */
    private static AggregationLevelLookup instance = null;

    private HashMap<VizzlySignal,SamplingRateEstimation> rateEstimators = null;
   
    private AggregationLevelLookup() {
        rateEstimators = new HashMap<VizzlySignal,SamplingRateEstimation>();
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
        try {
            writeAccess.acquire();
            SamplingRateEstimation e = rateEstimators.get(signal);
            if(e == null) {
                e = new SamplingRateEstimation(values.firstElement().timestamp);
                rateEstimators.put(signal, e);
            }
            e.updateEstimation(values);
            writeAccess.release();
            writeStateToFile();
        } catch(InterruptedException ex) {
            log.error(ex);
        }
    }
    
    public void deleteSignalEstimation(VizzlySignal signal) {
        try {
            writeAccess.acquire();
            rateEstimators.remove(signal);
            writeAccess.release();
            writeStateToFile();
        } catch(InterruptedException ex) {
            log.error(ex);
        }
    }

    private void writeStateToFile() {
        FileOutputStream fos = null;
        ObjectOutputStream o = null;
        try {
            writeAccess.acquire();
            fos = new FileOutputStream(stateFile); 
            o = new ObjectOutputStream(fos);
            o.writeObject(Integer.valueOf(rateEstimators.size()));
            for(VizzlySignal s : rateEstimators.keySet()) {
                o.writeObject(s);
                o.writeObject(rateEstimators.get(s));
            }
        } catch(IOException e) {
            log.error(stateFile, e);
        } catch(InterruptedException e) {
            log.error(e);
        } finally { 
            try { 
                if(o != null) {
                    o.close();
                }
                if(fos != null) {
                    fos.close();
                }
                writeAccess.release();
            } catch (Exception e) { 
                log.error("Error in _writeStateToFile: " + e);
            } 
        }
    }

    private synchronized void loadStateFromFile() {
        FileInputStream fis = null;
        ObjectInputStream o = null;
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
