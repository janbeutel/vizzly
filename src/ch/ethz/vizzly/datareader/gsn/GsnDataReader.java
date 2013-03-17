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

package ch.ethz.vizzly.datareader.gsn;

import java.util.Vector;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.log4j.Logger;

import ch.ethz.vizzly.datareader.AbstractDataReader;
import ch.ethz.vizzly.datatype.ServerSpec;
import ch.ethz.vizzly.datatype.VizzlyException;
import ch.ethz.vizzly.datatype.VizzlyInvalidSignalException;
import ch.ethz.vizzly.datatype.VizzlySignal;
import ch.ethz.vizzly.datatype.readings.TimedLocationValue;

/**
 * This class distributed GSN read requests to the respective GSN servers.
 * @author Matthias Keller
 *
 */
public class GsnDataReader extends AbstractDataReader {

    /**
     * Log.
     */
    private static Logger log = Logger.getLogger(GsnDataReader.class);
    
    private GsnFetcherInstances fetcherList = null;

    public GsnDataReader() {
        fetcherList = new GsnFetcherInstances();
    }

    public void validateSignal(VizzlySignal signal) throws VizzlyInvalidSignalException {
        if(signal.dataSource.serverAddress == null) throw new VizzlyInvalidSignalException("Could not parse signal: GSN server address is missing.");
        Pattern p = Pattern.compile("[a-zA-Z0-9\\.\\-:]+"); 
        Matcher m = p.matcher(signal.dataSource.serverAddress); 
        if(!m.matches()) {
            throw new VizzlyInvalidSignalException("Could not parse signal: GSN server address is invalid.");
        }
      
        try {
            GsnMultiDataFetcher f = fetcherList.getMultiDataFetcher(ServerSpec.fromAddress(signal.dataSource.serverAddress));
            if(!f.isVirtualSensorValid(signal.dataSource.name)) throw new VizzlyInvalidSignalException("Could not parse signal: Invalid GSN virtual sensor.");
            
            if(!f.isFieldValid(signal.dataSource.name, signal.dataField)) throw new VizzlyInvalidSignalException("Could not parse signal: dataField is invalid.");
            if(!f.isFieldValid(signal.dataSource.name, signal.timeField)) throw new VizzlyInvalidSignalException("Could not parse signal: timeField is invalid.");
            if(!signal.deviceSelect.type.equals("all")) {
                if(!f.isFieldValid(signal.dataSource.name, signal.deviceSelect.field)) throw new VizzlyInvalidSignalException("Could not parse signal: Device select field is invalid.");
            }
            
            if(signal.locationLatField != null) {
                if(!f.isFieldValid(signal.dataSource.name, signal.locationLatField)) throw new VizzlyInvalidSignalException("Could not parse signal: locationLatField is invalid.");
                if(!f.isFieldValid(signal.dataSource.name, signal.locationLngField)) throw new VizzlyInvalidSignalException("Could not parse signal: locationLngField is invalid.");
            }
            
        } catch(VizzlyException e) {
            log.error(e.getLocalizedMessage(), e);
        }
        
        return;
    }

    public Vector<TimedLocationValue> getSignalData(VizzlySignal signal, Long timeFilterStart, Long timeFilterEnd, int rowLimit)
            throws VizzlyException {
        // Find fetcher for server on IP:port
        GsnMultiDataFetcher fetcher = fetcherList.getMultiDataFetcher(ServerSpec.fromAddress(signal.dataSource.serverAddress));
        return fetcher.getDataFromSource(signal, timeFilterStart, timeFilterEnd, rowLimit);
    }

}
