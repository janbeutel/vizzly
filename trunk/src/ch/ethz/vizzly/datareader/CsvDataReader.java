/*
 * Copyright 2013 ETH Zurich, Computer Engineering and Networks Laboratory
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

package ch.ethz.vizzly.datareader;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
import java.util.Vector;

import org.apache.log4j.Logger;

import ch.ethz.vizzly.datatype.TimedLocationValue;
import ch.ethz.vizzly.datatype.VizzlyException;
import ch.ethz.vizzly.datatype.VizzlyInvalidSignalException;
import ch.ethz.vizzly.datatype.VizzlySignal;

/**
 * This class implements a data reader that uses CSV files as input. 
 * @author Matthias Keller
 *
 */
public class CsvDataReader extends AbstractDataReader {

    
    private static Logger log = Logger.getLogger(CsvDataReader.class);
    
    private final String csvDataDir = "./csvdata";
    
    /**
     * We want to skip parts of the file based on the specified time interval. For each signal,
     * the corresponding tree map translates between the timestamp (key) to a byte position (entry).
     */
    private HashMap<VizzlySignal, TreeMap<Long,Long>> timeToFilePositionMap = null;
    
    /**
     * For each signal we store the specified column names
     */
    private HashMap<VizzlySignal, String[]> columnNamesMap = null;
    
    public CsvDataReader() {
        File csvDir = new File(csvDataDir);
        if(!csvDir.exists()) {
            csvDir.mkdir();
        }
        timeToFilePositionMap = new HashMap<VizzlySignal, TreeMap<Long,Long>>();
        columnNamesMap = new HashMap<VizzlySignal, String[]>();
    }
    
    public void validateSignal(VizzlySignal signal) throws VizzlyInvalidSignalException {
        // TODO Auto-generated method stub
        return;
    }

    public Vector<TimedLocationValue> getSignalData(VizzlySignal signal,
            Long timeFilterStart, Long timeFilterEnd, int rowLimit)
            throws VizzlyException {
        
        try {
            FileInputStream is = new FileInputStream(csvDataDir + "/" + signal.dataSource.name);
        
            // We might skip a number of bytes to improve performance, this decision is based
            // on the specified time interval.
            long skipBytes = 0;
            TreeMap<Long,Long> timePosMap = null;
            if(timeFilterStart != null && (timePosMap = timeToFilePositionMap.get(signal)) != null) {
                if(timePosMap.size() != 0) {
                    // If we find no closer entry, the last entry is correct
                    skipBytes = timePosMap.lastEntry().getValue();
                }
                for (Map.Entry<Long,Long> entry : timePosMap.entrySet()) {
                    // Search for next higher entry, then use the one before
                    if(entry.getKey() >= timeFilterStart) {
                        Map.Entry<Long, Long> p = timePosMap.lowerEntry(entry.getKey());
                        if(p != null) {
                            skipBytes = p.getValue();
                        }
                        break;
                    }
                }
            }
            
            if(skipBytes > 0) {
                log.debug("Skipping " + skipBytes + " bytes in file.");
                is.skip(skipBytes);
            }
            
            long curFilePosBytes = skipBytes;
            BufferedReader bufRead = new BufferedReader(new InputStreamReader(is));
            String curLine;
            String[] columnNames = null;
            if(curFilePosBytes == 0) {
                curLine = bufRead.readLine();
                curFilePosBytes += curLine.length()+1;
                if(curLine == null || !curLine.startsWith("#")) {
                    bufRead.close();
                    is.close();
                    throw new VizzlyException("File " + signal.dataSource.name + " is missing field description.");
                }
                columnNames = curLine.replaceFirst("#", "").replace(", ", ",").trim().split(",");
                columnNamesMap.put(signal, columnNames);
            } else {
                columnNames = columnNamesMap.get(signal);
            }
            
            // Check if requested columns exists
            int timeColumnIdx = -1;
            int dataColumnIdx = -1;
            int deviceSelColumnIdx = -1;
            int locLatColumnIdx = -1;
            int locLngColumnIdx = -1;
            for(int i = 0; i < columnNames.length; i++) {
                if(columnNames[i].equals(signal.timeField)) {
                    timeColumnIdx = i;
                } else if(columnNames[i].equals(signal.dataField)) {
                    dataColumnIdx = i;
                } else if(signal.specifiesSingleDevice() &&
                        columnNames[i].equals(signal.deviceSelect.field)) {
                    deviceSelColumnIdx = i;
                } else if(signal.hasLocation() && columnNames[i].equals(signal.locationLatField)) {
                    locLatColumnIdx = i;
                } else if(signal.hasLocation() && columnNames[i].equals(signal.locationLngField)) {
                    locLngColumnIdx = i;
                }
            }
            
            if(timeColumnIdx == -1 || dataColumnIdx == -1) {
                bufRead.close();
                is.close();
                throw new VizzlyException("Invalid column names (time and data) in signal specification.");
            } else if(signal.specifiesSingleDevice() &&
                    deviceSelColumnIdx == -1) {
                bufRead.close();
                is.close();
                throw new VizzlyException("Invalid column names (device select) in signal specification.");
            } else if(signal.hasLocation() && (locLatColumnIdx == -1 || locLngColumnIdx == -1)) {
                bufRead.close();
                is.close();
                throw new VizzlyException("Invalid column names (location) in signal specification.");
            }
            
            // If no timestamp to file position map exists for this signal, create one.
            // If it exists, determine where it ends.
            long lastFilePosMark = 0;
            if(!timeToFilePositionMap.containsKey(signal)) {
                timeToFilePositionMap.put(signal, new TreeMap<Long,Long>());
            } else {
                lastFilePosMark = timeToFilePositionMap.get(signal).lastEntry().getValue();
            }
            
            
            Vector<TimedLocationValue> ret = new Vector<TimedLocationValue>();
            while((curLine = bufRead.readLine()) != null) {
            
                curFilePosBytes += curLine.length()+1;
                String[] parts = curLine.replace(" ", "").split(",");
                
                if(parts.length < columnNames.length) {
                    // Incomplete line
                    log.debug("Incomplete line, parts len = " + parts.length + ", num columns = " + columnNames.length);
                    continue;
                }
                
                Long timestamp = Long.valueOf(parts[timeColumnIdx]);
                
                // Add entry to time to file position mapping
                if(curFilePosBytes >= (lastFilePosMark + 5000)) {
                    timeToFilePositionMap.get(signal).put(timestamp, curFilePosBytes);
                    lastFilePosMark = curFilePosBytes;
                }
             
                // Discard line if outside of selected time range
                if(timeFilterStart != null && timestamp < timeFilterStart) {
                    continue;
                }
                // We apparently moved too far in this case, stop
                if(timeFilterEnd != null && timestamp > timeFilterEnd) {
                    break;
                }
                // Discard line if only data from a specific device is requested
                if(signal.specifiesSingleDevice() && !parts[deviceSelColumnIdx].equals(signal.deviceSelect.value)) {
                    continue;
                }
                
                if(!signal.hasLocation()) {
                    ret.add(new TimedLocationValue(timestamp, 
                            Double.valueOf(parts[dataColumnIdx])));
                } else {
                    ret.add(new TimedLocationValue(timestamp, 
                            Double.valueOf(parts[dataColumnIdx]),
                            Double.valueOf(parts[locLatColumnIdx]),
                            Double.valueOf(parts[locLngColumnIdx])));
                }
                
                if(rowLimit > 0 && ret.size() > rowLimit) {
                    break;
                }
                
            }
          
            bufRead.close();
            is.close();
        
            return ret;
        
        } catch(FileNotFoundException e) {
            log.error(e);
            throw new VizzlyException("Requested CSV file was not found.");
        } catch(IOException e) {
            log.error(e);
            throw new VizzlyException("IO error while reading CSV file.");
        }
    }


}
