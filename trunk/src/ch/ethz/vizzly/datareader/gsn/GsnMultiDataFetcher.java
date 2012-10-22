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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
import java.util.Vector;
import java.util.concurrent.Semaphore;

import org.apache.log4j.Logger;

import ch.ethz.vizzly.datatype.ServerSpec;
import ch.ethz.vizzly.datatype.VizzlyException;
import ch.ethz.vizzly.datatype.TimedLocationValue;
import ch.ethz.vizzly.datatype.VizzlySignal;
import ch.ethz.vizzly.util.GeoCoordConverter;

/**
 * This class handles the actual conversion of a request to a GSN URL including
 * the data fetching from this URL.
 * @author Matthias Keller
 *
 */
public class GsnMultiDataFetcher {
    
    private final int FETCH_MAX_ROW_LIMIT = 200000;

    private static Logger log = Logger.getLogger(GsnMultiDataFetcher.class);

    private GsnServerStructure structure = null;

    private final Semaphore semaphore = new Semaphore(8);

    private final String LATITUDE_FIELD = "latitude";

    private final String LONGITUDE_FIELD = "longitude";

    private ServerSpec serverSpec = null;

    /* Used to check if it makes sense to query the GSN server again. */
    private Date lastStructureUpdate = null;

    public GsnMultiDataFetcher(ServerSpec serverSpec) {
        this.serverSpec = serverSpec;
        structure = GsnServerStructure.fromGsnServer(serverSpec);
        lastStructureUpdate = Calendar.getInstance().getTime();
    }

    /**
     * This method generated a URL that retrieves the requested data from the /multidata interface of
     * a GSN server.
     */
    private String buildGsnDataSourceUrl(VizzlySignal signal, Long timeFilterStart, Long timeFilterEnd, int windowLengthSec, int rowLimit, Boolean includeLocation) {

        windowLengthSec = 1;
        StringBuilder urlStringBuilder = new StringBuilder();

        urlStringBuilder.append("http://").append(signal.dataSource.serverAddress).append("/multidata?");
        urlStringBuilder.append("vs[0]=").append(signal.dataSource.name);
        urlStringBuilder.append("&field[0]=").append(signal.dataField);

        // Location information
        if(includeLocation) {
            urlStringBuilder.append("&vs[1]=").append(signal.dataSource.name);
            urlStringBuilder.append("&field[1]=").append(LATITUDE_FIELD);
            urlStringBuilder.append("&vs[2]=").append(signal.dataSource.name);
            urlStringBuilder.append("&field[2]=").append(LONGITUDE_FIELD);
        }

        SimpleDateFormat dateFormatter = new SimpleDateFormat("dd/MM/yyyy+HH:mm:ss");
        Calendar cal = Calendar.getInstance();
        if(timeFilterStart != null) {
            cal.setTimeInMillis(timeFilterStart.longValue());
            urlStringBuilder.append("&from=").append(dateFormatter.format(cal.getTime()));
        }
        if(timeFilterEnd != null) {
            cal.setTimeInMillis(timeFilterEnd.longValue());
            urlStringBuilder.append("&to=").append(dateFormatter.format(cal.getTime()));
        }

        // There are also virtual sensors that require neither a device ID, nor a position
        if(!signal.deviceSelect.type.toLowerCase().equals("all")) {
            urlStringBuilder.append("&c_join[0]=and").append("&c_vs[0]=").append(signal.dataSource.name);
            urlStringBuilder.append("&c_field[0]="+signal.deviceSelect.field);
            urlStringBuilder.append("&c_min[0]=").append(Integer.parseInt(signal.deviceSelect.value)-1);
            urlStringBuilder.append("&c_max[0]=").append(Integer.parseInt(signal.deviceSelect.value));
        }

        if(windowLengthSec > 1) {
            urlStringBuilder.append("&agg_function=avg&agg_period=1&agg_unit=").append(windowLengthSec*1000);
        }

        if(rowLimit != -1) {
            urlStringBuilder.append("&nb=SPECIFIED&nb_value=").append(rowLimit);
        }

        urlStringBuilder.append("&timeline=").append(signal.timeField);
        
        return urlStringBuilder.toString();
    }

    public Vector<TimedLocationValue> getDataFromSource(VizzlySignal signal, Long timeFilterStart, Long timeFilterEnd, int rowLimit)
            throws VizzlyException {
        if(!isVirtualSensorValid(signal.dataSource.name)) {
            throw new VizzlyException("Virtual sensor " + signal.dataSource.name + " not found on " + signal.dataSource.serverAddress);
        }

        if(!isFieldValid(signal.dataSource.name, signal.dataField)) {
            throw new VizzlyException("Field " + signal.dataField + " not found on " + signal.dataSource.serverAddress);
        }

        Boolean includeLocation = false;
        if(isFieldValid(signal.dataSource.name, LATITUDE_FIELD) && isFieldValid(signal.dataSource.name, LONGITUDE_FIELD)) {
            includeLocation = true;
        }

        // GSN has a limit on the number of rows that is returned with each request
        int singleFetchRowLimit = (rowLimit > 0 && rowLimit < FETCH_MAX_ROW_LIMIT) ? rowLimit : FETCH_MAX_ROW_LIMIT;
        int round = 1;
        Long timeFilterEndToGsn = timeFilterEnd;
        Vector<TimedLocationValue> rAll = null;
        while (true) {
            if(round > 1) {
                log.debug("Fetching round " + round);
            }
            String url = buildGsnDataSourceUrl(signal, timeFilterStart, timeFilterEndToGsn, 0, singleFetchRowLimit, includeLocation);
            Vector<TimedLocationValue> r = fetchDataFromGsn(url, includeLocation);
            if(round == 1) {
                rAll = r;
            } else {
                rAll.addAll(r);
            }
            if(r.size() < singleFetchRowLimit) {
                // Stop if the maximum row limit was not fully used, thus there is no more data
                break;
            }
            if(rowLimit > 0 && rAll.size() >= rowLimit) {
                // Stop if the maximum number of returned rows for the request is exceeded
                break;
            }
            round++;
            // Get entry with the lowest timestamp, this is the last entry coming from GSN
            timeFilterEndToGsn = r.get(r.size()-1).timestamp;
            // Adapt maximum number of lines that the next run can return
            if(rowLimit > 0 && singleFetchRowLimit > (rowLimit-rAll.size())) {
                singleFetchRowLimit = rowLimit-rAll.size();
            }
        }
        if(rAll != null && rAll.size() > 0) {
            // Data from GSN is in reverse order with the most recent timestamp first
            Collections.reverse(rAll);
        }
        return rAll;
    }

    /**
     * Method for requesting data from a GSN server over HTTP.
     * @return Parsed result data
     */
    private Vector<TimedLocationValue> fetchDataFromGsn(String dataSourceUrlStr, Boolean includeLocation) 
            throws VizzlyException {

        Vector<TimedLocationValue> data = new Vector<TimedLocationValue>();
        long fetchStart = System.currentTimeMillis();

        try {
            semaphore.acquire();

            URL dataSourceUrl = new URL(dataSourceUrlStr);
            BufferedReader in = new BufferedReader(new InputStreamReader(dataSourceUrl.openStream()));
            Calendar cal = Calendar.getInstance();
            SimpleDateFormat dateFormatter = new SimpleDateFormat("dd/MM/yyyy HH:mm:ss.SSS zzz");
            
            String inputLine;
            long timestamp;
            while ((inputLine = in.readLine()) != null) {
                if(inputLine.startsWith("#")) {
                    continue;
                }
                if(inputLine.contains("null")) {
                    continue;
                }
                String [] parts = inputLine.split(",");

                if(includeLocation) {
                    cal.setTime(dateFormatter.parse(parts[3]));
                    timestamp = cal.getTimeInMillis();
                    data.add(new TimedLocationValue(timestamp, Double.valueOf(parts[0]),
                            GeoCoordConverter.convertLatitude(Double.valueOf(parts[1])),
                            GeoCoordConverter.convertLongitude(Double.valueOf(parts[2]))));
                } else {
                    cal.setTime(dateFormatter.parse(parts[1]));
                    timestamp = cal.getTimeInMillis();
                    data.add(new TimedLocationValue(timestamp, Double.valueOf(parts[0])));
                }

            }
            in.close();
            log.debug("Num filled rows: " + data.size());

        } catch (IOException e) {
            log.warn("Fetching data from GSN failed. Network error: " + serverSpec.serverIp.toString() + ":" 
                    + Integer.valueOf(serverSpec.serverPort).toString(), e);
            throw new VizzlyException("Fetching data from GSN failed.");
        } catch (ParseException e) {
            log.warn("Parsing received data failed.", e);
            throw new VizzlyException("Fetching data from GSN failed.");
        } catch (InterruptedException e) {
            log.warn("Parsing was interrupted.", e);
            throw new VizzlyException("Fetching data from GSN failed.");
        } finally {
            semaphore.release();
        }

        long fetchEnd = System.currentTimeMillis();
        log.debug("Query = " + dataSourceUrlStr + ", time = " + (fetchEnd-fetchStart));

        return data;
    }

    private synchronized Boolean tryStructureUpdate() {
        // Re-fetch server structure only if last update was a while ago
        if(Calendar.getInstance().getTime().getTime()-lastStructureUpdate.getTime() < 300000L) {
            return false;
        }
        GsnServerStructure s = GsnServerStructure.fromGsnServer(serverSpec);
        lastStructureUpdate = Calendar.getInstance().getTime();
        // Might be empty when the GSN server is not willing to talk - better keep the old version then
        if(s.getVirtualSensors().size() > 0) {
            structure = s;
        }
        return true;
    }

    private Boolean isVirtualSensorValid(String virtualSensor) {
        if(!structure.getVirtualSensors().contains(virtualSensor)) {
            // Maybe we just need to update our cached information
            if(!tryStructureUpdate()) {
                return false;
            }
            if(!structure.getVirtualSensors().contains(virtualSensor)) {
                return false;
            }
        }
        return true;
    }

    private Boolean isFieldValid(String virtualSensor, String field) {
        if(structure.getFields(virtualSensor) == null) {
            return false;
        }
        if(!structure.getFields(virtualSensor).contains(field)) {
            // Maybe we just need to update our cached information
            if(!tryStructureUpdate()) {
                return false;
            }
            if(structure.getFields(virtualSensor) == null) {
                return false;
            }
            if(!structure.getFields(virtualSensor).contains(field)) {
                return false;
            }
        }
        return true;
    }
}
